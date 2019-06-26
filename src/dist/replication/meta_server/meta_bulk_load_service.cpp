/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <dsn/dist/fmt_logging.h>

#include "meta_bulk_load_service.h"

namespace dsn {
namespace replication {

bulk_load_service::bulk_load_service(meta_service *meta_svc) : _meta_svc(meta_svc)
{
    _state = _meta_svc->get_server_state();
}

void bulk_load_service::on_start_bulk_load(start_bulk_load_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    {
        zauto_read_lock l(app_lock());

        app = _state->get_app(request.app_name);
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            derror_f("app {} is not existed or not available", request.app_name);
            response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
            return;
        }

        // TODO(heyuchen): consider finish,failed,paused,canceled
        if (app->app_bulk_load_status != bulk_load_status::BLS_INVALID) {
            derror_f("app {} is already executing bulk load, bulk load status is {}",
                     app->app_name.c_str(),
                     enum_to_string(app->app_bulk_load_status));
            response.err = ERR_BUSY;
            return;
        }

        if (_meta_svc->get_block_service_manager().get_block_filesystem(
                request.file_provider_type) == nullptr) {
            derror_f("invalid remote file provider type {}", request.file_provider_type);
            response.err = ERR_INVALID_PARAMETERS;
            return;
        }
    }

    // TODO(heyuchen):
    // Validate:
    // 1. check file existed
    // 2. check partition count
    ddebug_f("start app {} bulk load", request.app_name);

    // set meta level to steady
    meta_function_level::type level = _meta_svc->get_function_level();
    if (level != meta_function_level::fl_steady) {
        _meta_svc->set_function_level(meta_function_level::fl_steady);
        ddebug_f("change meta server function level from {} to {} to avoid possible balance",
                 _meta_function_level_VALUES_TO_NAMES.find(level)->second,
                 _meta_function_level_VALUES_TO_NAMES.find(meta_function_level::fl_steady)->second);
    }

    // set bulk_load_status to BLS_DOWMLOADING on zk
    update_blstatus_downloading_on_remote_storage(std::move(app), std::move(rpc));
}

void bulk_load_service::update_blstatus_downloading_on_remote_storage(
    std::shared_ptr<app_state> app, start_bulk_load_rpc rpc)
{
    auto on_write_storage = [app, rpc, this](error_code err) {
        if (err == ERR_OK) {
            ddebug_f("app {} update bulk load status to {} on remote storage",
                     app->app_name,
                     enum_to_string(bulk_load_status::BLS_DOWNLOADING));

            zauto_write_lock l(app_lock());
            app->app_bulk_load_status = bulk_load_status::BLS_DOWNLOADING;
            app->helpers->bl_states.app_status = app->app_bulk_load_status;
            app->helpers->bl_states.partitions_in_progress = app->partition_count;

            // create bulk load info
            create_bulk_load_folder_on_remote_storage(std::move(app), std::move(rpc));

        } else if (err == ERR_TIMEOUT) {
            dwarn_f("failed to update app {} bulk load status, remote storage is not available, "
                    "please try later",
                    app->app_name);
            tasking::enqueue(
                LPC_META_STATE_HIGH,
                _meta_svc->tracker(),
                std::bind(&bulk_load_service::update_blstatus_downloading_on_remote_storage,
                          this,
                          std::move(app),
                          std::move(rpc)),
                0,
                std::chrono::seconds(1));
        } else {
            derror_f("failed to update app {} bulk load status, error is {}",
                     app->app_name,
                     err.to_string());
            auto response = rpc.response();
            response.err = ERR_ZOOKEEPER_OPERATION;
        }
    };

    app_info info = *app;
    info.app_bulk_load_status = bulk_load_status::BLS_DOWNLOADING;
    _meta_svc->get_remote_storage()->set_data(_state->get_app_path(*app),
                                              dsn::json::json_forwarder<app_info>::encode(info),
                                              LPC_META_STATE_HIGH,
                                              on_write_storage,
                                              _meta_svc->tracker());
}

void bulk_load_service::create_bulk_load_folder_on_remote_storage(std::shared_ptr<app_state> app,
                                                                  start_bulk_load_rpc rpc)
{
    std::string bulk_load_path = get_app_bulk_load_path(app);

    // TODO(heyuchen): handle dir exist
    auto on_write_stroage = [app, rpc, bulk_load_path, this](error_code err) {
        if (err == ERR_OK) {
            ddebug_f("create app {} bulk load dir", app->app_name);
            for (int i = 0; i < app->partition_count; ++i) {
                update_partition_blstatus_downloading(app, i, bulk_load_path, rpc);
            }
        } else if (err == ERR_TIMEOUT) {
            dwarn_f("failed to create app {} bulk load dir, remote storage is not available, "
                    "please try later",
                    app->app_name);
            tasking::enqueue(
                LPC_META_STATE_HIGH,
                _meta_svc->tracker(),
                std::bind(&bulk_load_service::create_bulk_load_folder_on_remote_storage,
                          this,
                          std::move(app),
                          std::move(rpc)),
                0,
                std::chrono::seconds(1));
        } else {
            derror_f("failed to create app {} bulk load dir, error is {}",
                     app->app_name,
                     err.to_string());
            auto response = rpc.response();
            response.err = ERR_ZOOKEEPER_OPERATION;
        }
    };

    _meta_svc->get_remote_storage()->create_node(
        bulk_load_path, LPC_META_STATE_HIGH, on_write_stroage);
}

void bulk_load_service::update_partition_blstatus_downloading(std::shared_ptr<app_state> app,
                                                              uint32_t pidx,
                                                              const std::string &bulk_load_path,
                                                              start_bulk_load_rpc rpc)
{
    partition_bulk_load_info pinfo;
    pinfo.status = bulk_load_status::BLS_DOWNLOADING;

    // TODO(heyuchen): handle partition dir exist
    auto on_write_stroage = [app, pidx, bulk_load_path, rpc, pinfo, this](error_code err) {
        if (err == ERR_OK) {
            ddebug_f("app {} create partition[{}] bulk_load_info", app->app_name, pidx);

            zauto_write_lock(app_lock());
            --app->helpers->bl_states.partitions_in_progress;
            app->helpers->bl_states.partitions_info.insert(std::make_pair(pidx, pinfo));
            if (app->helpers->bl_states.partitions_in_progress == 0) {
                ddebug_f("app {} start bulk load succeed", app->app_name);
                auto response = rpc.response();
                response.err = ERR_OK;
            }
        } else if (err == ERR_TIMEOUT) {
            dwarn_f("failed to create app {} partition[{}] bulk_load_info, remote storage is not "
                    "available, please try later",
                    app->app_name,
                    pidx);
            tasking::enqueue(LPC_META_STATE_HIGH,
                             _meta_svc->tracker(),
                             std::bind(&bulk_load_service::update_partition_blstatus_downloading,
                                       this,
                                       std::move(app),
                                       pidx,
                                       bulk_load_path,
                                       rpc),
                             0,
                             std::chrono::seconds(1));

        } else {
            derror_f("failed to create app {} partition[{}] bulk_load_info, error is {}",
                     app->app_name,
                     pidx,
                     err.to_string());
            auto response = rpc.response();
            response.err = ERR_ZOOKEEPER_OPERATION;
        }
    };

    _meta_svc->get_remote_storage()->create_node(
        get_partition_bulk_load_path(bulk_load_path, pidx),
        LPC_META_STATE_HIGH,
        on_write_stroage,
        dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo),
        _meta_svc->tracker());
}

} // namespace replication
} // namespace dsn
