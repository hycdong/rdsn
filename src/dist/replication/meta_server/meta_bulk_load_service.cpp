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

bulk_load_service::bulk_load_service(meta_service *meta_svc, const std::string &bulk_load_dir)
    : _meta_svc(meta_svc), _bulk_load_root(bulk_load_dir)
{
    _state = _meta_svc->get_server_state();
}

void bulk_load_service::create_bulk_load_dir_on_remote_stroage()
{
    std::string path = _bulk_load_root;
    blob value = blob();
    _meta_svc->get_meta_storage()->create_node(std::move(path), std::move(value), [this]() {
        ddebug_f("create bulk load root({}) succeed", _bulk_load_root);
    });
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
        if (app->is_bulk_loading) {
            derror_f("app {} is already executing bulk load, please wait", app->app_name.c_str());
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

    // TODO(heyuchen): validate in bulk load info file
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

    // set bulk_load_status to true on zk
    start_bulk_load_on_remote_storage(std::move(app), std::move(rpc));
}

void bulk_load_service::start_bulk_load_on_remote_storage(std::shared_ptr<app_state> app,
                                                          start_bulk_load_rpc rpc)
{
    app_info info = *app;
    info.is_bulk_loading = true;

    blob value = dsn::json::json_forwarder<app_info>::encode(info);
    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), [app, rpc, this]() {
            {
                zauto_write_lock l(app_lock());
                app->is_bulk_loading = true;
            }
            {
                zauto_write_lock l(_lock);
                _bulk_load_states.apps_in_progress_count[app->app_id] = app->partition_count;
            }
            create_app_bulk_load_info_with_rpc(std::move(app), std::move(rpc));
        });
}

void bulk_load_service::create_app_bulk_load_info_with_rpc(std::shared_ptr<app_state> app,
                                                           start_bulk_load_rpc rpc)
{
    std::string bulk_load_path = get_app_bulk_load_path(app->app_id);
    const auto req = rpc.request();
    app_bulk_load_info ainfo;
    ainfo.app_id = app->app_id;
    ainfo.app_name = app->app_name;
    ainfo.status = bulk_load_status::BLS_DOWNLOADING;
    ainfo.cluster_name = req.cluster_name;
    ainfo.file_provider_type = req.file_provider_type;
    blob value = dsn::json::json_forwarder<app_bulk_load_info>::encode(ainfo);

    {
        zauto_write_lock l(_lock);
        _bulk_load_info[ainfo.app_id] = ainfo;
    }

    ddebug_f("prepare create {}", bulk_load_path);

    _meta_svc->get_meta_storage()->create_node(
        std::move(bulk_load_path), std::move(value), [app, rpc, bulk_load_path, this]() {
            ddebug_f("create app {} bulk load dir", app->app_name);
            for (int i = 0; i < app->partition_count; ++i) {
                create_partition_bulk_load_info_with_rpc(
                    app->app_name, gpid(app->app_id, i), app->partition_count, bulk_load_path, rpc);
            }
        });
}

void bulk_load_service::create_partition_bulk_load_info_with_rpc(const std::string &app_name,
                                                                 gpid pid,
                                                                 uint32_t partition_count,
                                                                 const std::string &bulk_load_path,
                                                                 start_bulk_load_rpc rpc)
{
    partition_bulk_load_info pinfo;
    pinfo.status = bulk_load_status::BLS_DOWNLOADING;
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->create_node(
        get_partition_bulk_load_path(bulk_load_path, pid.get_partition_index()),
        std::move(value),
        [app_name, pid, partition_count, bulk_load_path, rpc, pinfo, this]() {
            ddebug_f(
                "app {} create partition[{}] bulk_load_info", app_name, pid.get_partition_index());
            {
                zauto_write_lock l(_lock);
                --_bulk_load_states.apps_in_progress_count[pid.get_app_id()];
                _bulk_load_states.partitions_info.insert(std::make_pair(pid, pinfo));

                if (_bulk_load_states.apps_in_progress_count[pid.get_app_id()] == 0) {
                    ddebug_f("app {} start bulk load succeed", app_name);
                    _bulk_load_states.apps_in_progress_count[pid.get_app_id()] = partition_count;
                    auto response = rpc.response();
                    response.err = ERR_OK;
                }
            }
            // start send bulk load to replica servers
            partition_bulk_load(pid);
        });
}

void bulk_load_service::partition_bulk_load(gpid pid)
{
    dsn::rpc_address primary_addr;
    std::string app_name;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            dwarn_f("app {} not exist, set bulk load finish", app->app_name);
            // TODO(heyuchen): handler it
            return;
        }
        app_name = app->app_name;
        primary_addr = app->partitions[pid.get_partition_index()].primary;
    }

    partition_bulk_load_info pbl_info;
    app_bulk_load_info ainfo;
    {
        zauto_read_lock l(_lock);
        pbl_info = _bulk_load_states.partitions_info[pid];
        ainfo = _bulk_load_info[pid.get_app_id()];
    }

    if (primary_addr.is_invalid()) {
        dwarn_f("app {} gpid({}.{}) primary is invalid, try it later");
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_bulk_load, this, pid),
                         0,
                         std::chrono::seconds(1));
        return;
    }

    bulk_load_request req;
    req.pid = pid;
    req.app_name = app_name;
    req.primary_addr = primary_addr;
    req.app_bl_status = ainfo.status;
    req.remote_provider_name = ainfo.file_provider_type;
    req.cluster_name = ainfo.cluster_name;
    req.partition_bl_info = pbl_info;

    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_BULK_LOAD, 0, pid.thread_hash());
    dsn::marshall(msg, req);
    dsn::rpc_response_task_ptr rpc_callback = rpc::create_rpc_response_task(
        msg,
        _meta_svc->tracker(),
        [this, pid, primary_addr](error_code err, bulk_load_response &&resp) {
            on_partition_bulk_load_reply(err, std::move(resp), pid, primary_addr);
        });

    zauto_write_lock l(_lock);
    _bulk_load_states.partitions_request[pid] = rpc_callback;

    ddebug_f(
        "send bulk load request to replica server, app({}.{}), target_addr = {}, app bulk load "
        "={}, partition bulk load status = {}, remote provider = {}, cluster_name = {}",
        pid.get_app_id(),
        pid.get_partition_index(),
        primary_addr.to_string(),
        enum_to_string(req.app_bl_status),
        enum_to_string(pbl_info.status),
        req.remote_provider_name,
        req.cluster_name);
    _meta_svc->send_request(msg, primary_addr, rpc_callback);
}

void bulk_load_service::on_partition_bulk_load_reply(dsn::error_code err,
                                                     bulk_load_response &&response,
                                                     gpid pid,
                                                     const rpc_address &primary_addr)
{
    if (err != ERR_OK) {
        dwarn_f("gpid({}.{}) failed to recevie bulk load response, err={}",
                pid.get_app_id(),
                pid.get_partition_index(),
                err.to_string());
    } else if (response.err == ERR_OBJECT_NOT_FOUND || response.err == ERR_INVALID_STATE) {
        dwarn_f("gpid({}.{}) doesn't exist or has invalid state on node({}), err={}, plesae retry "
                "later",
                pid.get_app_id(),
                pid.get_partition_index(),
                primary_addr.to_string(),
                response.err.to_string());
    } else if (response.err != ERR_OK) {
        // TODO(heyuchen): add bulk load status check, not only downloading error handler below
        if (response.err == ERR_CORRUPTION) {
            derror_f("gpid({}.{}) failed to download files from remote provider, coz files "
                     "are damaged, err={}",
                     pid.get_app_id(),
                     pid.get_partition_index(),
                     response.err.to_string());
        } else {
            dwarn_f("gpid({}.{}) failed to download files from remote provider, coz file "
                    "system error, err={}",
                    pid.get_app_id(),
                    pid.get_partition_index(),
                    response.err.to_string());
        }
        // TODO(heyuchen): set partition bulk load and app bulk load failed, not return
        // immdiately
        std::string partition_bulk_load_path = get_partition_bulk_load_path(
            get_app_bulk_load_path(pid.get_app_id()), pid.get_partition_index());
        update_partition_bulk_load_status(
            response.app_name, pid, partition_bulk_load_path, bulk_load_status::BLS_FAILED);
        return;
    } else {
        // download sst files
        // TODO(heyuchen): add bulk load status check, not only downloading error handler below
        // handle file damaged error during downloading files from remote stroage

        dsn::error_code ec = check_download_status(response);
        if (ec != ERR_OK) {
            dwarn_f("recevie bulk load response from {} app({}), gpid({}.{}), bulk load status={}, "
                    "download meet error={}",
                    primary_addr.to_string(),
                    response.app_name,
                    pid.get_app_id(),
                    pid.get_partition_index(),
                    enum_to_string(response.partition_bl_status),
                    ec.to_string());
            std::string partition_bulk_load_path = get_partition_bulk_load_path(
                get_app_bulk_load_path(pid.get_app_id()), pid.get_partition_index());
            update_partition_bulk_load_status(
                response.app_name, pid, partition_bulk_load_path, bulk_load_status::BLS_FAILED);
        } else {
            // download sst files succeed
            int32_t cur_progress =
                response.__isset.total_download_progress ? response.total_download_progress : 0;
            ddebug_f("recevie bulk load response from {} app({}) gpid({}.{}), bulk load status={}, "
                     "total_download_progress={}",
                     primary_addr.to_string(),
                     response.app_name,
                     pid.get_app_id(),
                     pid.get_partition_index(),
                     enum_to_string(response.partition_bl_status),
                     cur_progress);
            {
                zauto_write_lock l(_lock);
                _bulk_load_states.partitions_total_download_progress[pid] = cur_progress;
                if (response.__isset.download_progresses) {
                    _bulk_load_states.partitions_download_progress[pid] =
                        response.download_progresses;
                }
            }

            // TODO(heyuchen): change it to common value
            int32_t max_progress = 100;
            if (cur_progress >= max_progress) {
                ddebug_f("gpid({}.{}) finish download remote files from remote provider",
                         pid.get_app_id(),
                         pid.get_partition_index());

                {
                    zauto_read_lock l(app_lock());
                    std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
                    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
                        dwarn_f("app {} not exist, set bulk load finish", app->app_name);
                        // TODO(heyuchen): handler it
                        return;
                    }

                    std::string partition_bulk_load_path = get_partition_bulk_load_path(
                        get_app_bulk_load_path(pid.get_app_id()), pid.get_partition_index());
                    // TODO(heyuchen): check if status switch is valid
                    update_partition_bulk_load_status(app->app_name,
                                                      pid,
                                                      partition_bulk_load_path,
                                                      bulk_load_status::BLS_DOWNLOADED);
                }
                // TODO(heyuchen): not return here
                return;
            }
        }
    }

    // TODO(heyuchen): delay time to config
    tasking::enqueue(LPC_META_STATE_NORMAL,
                     _meta_svc->tracker(),
                     std::bind(&bulk_load_service::partition_bulk_load, this, pid),
                     0,
                     std::chrono::seconds(10));
}

dsn::error_code bulk_load_service::check_download_status(bulk_load_response &response)
{
    // download_progresses filed is not set
    if (!response.__isset.download_progresses) {
        return ERR_INVALID_STATE;
    }

    for (auto iter = response.download_progresses.begin();
         iter != response.download_progresses.end();
         ++iter) {
        partition_download_progress progress = iter->second;
        if (progress.status != ERR_OK) {
            dwarn_f("app({}) gpid({}.{}) meet error during downloading sst files, node_address={}, "
                    "error={}",
                    response.app_name,
                    progress.pid.get_app_id(),
                    progress.pid.get_partition_index(),
                    iter->first.to_string(),
                    progress.status.to_string());
            return progress.status;
        }
    }
    return ERR_OK;
}

void bulk_load_service::update_partition_bulk_load_status(const std::string &app_name,
                                                          dsn::gpid pid,
                                                          std::string &path,
                                                          bulk_load_status::type status)
{
    zauto_read_lock l(_lock);
    partition_bulk_load_info pinfo = _bulk_load_states.partitions_info[pid];
    pinfo.status = status;
    dsn::blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->set_data(
        std::move(path), std::move(value), [this, app_name, pid, path, status]() {
            zauto_write_lock l(_lock);
            bulk_load_status::type old_status = _bulk_load_states.partitions_info[pid].status;
            ddebug_f("app {} update partition[{}] bulk_load status from {} to {}",
                     app_name,
                     pid.get_partition_index(),
                     enum_to_string(old_status),
                     enum_to_string(status));

            _bulk_load_states.partitions_info[pid].status = status;

            if (status == bulk_load_status::BLS_DOWNLOADED) {
                _bulk_load_states.partitions_request[pid] = nullptr;
                if (--_bulk_load_states.apps_in_progress_count[pid.get_app_id()] == 0) {
                    update_app_bulk_load_status(pid.get_app_id(), status);
                }
            } else if (status == bulk_load_status::BLS_FAILED) {
                // TODO(heyuchen): consider other setting
                update_app_bulk_load_status(pid.get_app_id(), status);
            }
        });
}

void bulk_load_service::update_app_bulk_load_status(uint32_t app_id,
                                                    bulk_load_status::type new_status)
{
    app_bulk_load_info ainfo = _bulk_load_info[app_id];
    auto old_status = ainfo.status;
    ainfo.status = new_status;
    blob value = dsn::json::json_forwarder<app_bulk_load_info>::encode(ainfo);

    _meta_svc->get_meta_storage()->set_data(get_app_bulk_load_path(app_id),
                                            std::move(value),
                                            [this, old_status, ainfo, app_id, new_status]() {
                                                ddebug_f(
                                                    "update app({}) bulk load status from {} to {}",
                                                    ainfo.app_name,
                                                    enum_to_string(old_status),
                                                    enum_to_string(new_status));
                                                _bulk_load_info[app_id] = ainfo;
                                            });
}

void bulk_load_service::on_query_bulk_load_status(query_bulk_load_rpc rpc)
{
    const auto &request = rpc.request();
    const std::string &app_name = request.app_name;

    configuration_query_bulk_load_response &response = rpc.response();
    response.err = ERR_OK;
    response.app_name = app_name;

    uint32_t app_id, partition_count;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(app_name);
        app_id = app->app_id;
        partition_count = app->partition_count;

        if (app == nullptr) {
            derror_f("app {} is not existed", app_name);
            response.err = ERR_OBJECT_NOT_FOUND;
            return;
        }

        if (app->status != app_status::AS_AVAILABLE) {
            derror_f("app {} is not available", app_name);
            response.err = ERR_APP_DROPPED;
            return;
        }

        if (!app->is_bulk_loading) {
            dwarn_f("app {} is not during bulk load", app_name);
            response.err = ERR_INVALID_STATE;
            return;
        }
    }

    {
        zauto_read_lock l(_lock);
        response.app_status = get_app_bulk_load_status(app_id);
        response.partition_status.resize(partition_count);
        if (response.app_status == bulk_load_status::BLS_DOWNLOADING ||
            response.app_status == bulk_load_status::BLS_DOWNLOADED) {
            response.__isset.download_progresses = true;
            response.download_progresses.resize(partition_count);
        }

        auto partition_bulk_load_info_map = _bulk_load_states.partitions_info;
        for (auto iter = partition_bulk_load_info_map.begin();
             iter != partition_bulk_load_info_map.end();
             iter++) {
            int idx = iter->first.get_partition_index();
            response.partition_status[idx] = iter->second.status;
            if (response.__isset.download_progresses) {
                response.download_progresses[idx] =
                    _bulk_load_states.partitions_download_progress[iter->first];
            }
        }
    }
}

} // namespace replication
} // namespace dsn
