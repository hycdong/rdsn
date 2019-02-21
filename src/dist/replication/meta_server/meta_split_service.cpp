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

#include "dist/replication/meta_server/meta_split_service.h"
#include "dist/replication/meta_server/meta_state_service_utils.h"

namespace dsn {
namespace replication {

void meta_split_service::app_partition_split(app_partition_split_rpc rpc)
{
    const auto &request = rpc.request();
    ddebug_f("split partition for app({}), new_partition_count={}",
             request.app_name,
             request.new_partition_count);

    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    { // validate rpc parameters

        zauto_write_lock l(app_lock());

        // if app is not available
        app = _state->get_app(request.app_name);
        if (!app || app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_APP_NOT_EXIST;
            dwarn_f("client({}) sent split request with invalid app({}), app is not existed or unavailable",
                    ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                    request.app_name);
            return;
        }

        response.app_id = app->app_id;
        response.partition_count = app->partition_count;

        // if new_partition_count != old_partition_count*2
        if (request.new_partition_count != app->partition_count * 2) {
            response.err = ERR_INVALID_PARAMETERS;
            dwarn_f("client({}) sent split request with wrong partition count: app({}), partition count({}),"
                    "new_partition_count({})",
                    ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                    request.app_name,
                    app->partition_count,
                    request.new_partition_count);
            return;
        }

        // if there's ongoing split already.
        for (const auto &partition_config : app->partitions) {
            if (partition_config.ballot < 0) {
                response.err = ERR_BUSY;
                dwarn_f("app is already during partition split, client({}) sent repeated split request: app({}), new_partition_count({})",
                        ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                        request.app_name,
                        request.new_partition_count);
                return;
            }
        }
    }

    // validation passed
    do_app_partition_split(std::move(app), std::move(rpc));
}

void meta_split_service::do_app_partition_split(std::shared_ptr<app_state> app,
                                                app_partition_split_rpc rpc)
{

    auto on_write_storage_complete = [app, rpc, this](error_code ec) {
        if (ec == ERR_OK) {
            ddebug_f(
                "app {} write new partition count on remote storage, new partition count is {}",
                app->app_name.c_str(),
                app->partition_count * 2);

            zauto_write_lock l(app_lock());
            app->partition_count *= 2;
            app->helpers->contexts.resize(app->partition_count);
            app->partitions.resize(app->partition_count);

            for (int i = 0; i < app->partition_count; ++i) {
                app->helpers->contexts[i].config_owner = &app->partitions[i];
                if (i >= app->partition_count / 2) {
                    app->partitions[i].ballot = invalid_ballot;
                    app->partitions[i].pid = gpid(app->app_id, i);
                }
            }

            auto &response = rpc.response();
            response.err = ERR_OK;
            response.partition_count = app->partition_count;

        } else if (ec == ERR_TIMEOUT) {
            dwarn_f("remote storage is not available now, please try it later");
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             std::bind(&meta_split_service::do_app_partition_split,
                                       this,
                                       std::move(app),
                                       std::move(rpc)),
                             0,
                             std::chrono::seconds(1));
            // TODO(hyc):consider tracker
        } else {
            dassert(false, "failed to write to remote stroage, error is %s", ec.to_string());
        }
    };

    if (app->init_partition_count <= 0) {
        app->init_partition_count = app->partition_count;
    }
    auto copy = *app;
    copy.partition_count *= 2;
    blob value = dsn::json::json_forwarder<app_info>::encode(copy);

    _meta_svc->get_remote_storage()->set_data(_state->get_app_path(*app),
                                              std::move(value),
                                              LPC_META_STATE_HIGH,
                                              on_write_storage_complete,
                                              _meta_svc->tracker());
}

void meta_split_service::register_child_on_meta(register_child_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_IO_PENDING;

    zauto_write_lock(app_lock());

    std::shared_ptr<app_state> app = _state->get_app(request.app.app_id);
    dassert(app != nullptr, "get get app for app id(%d)", request.app.app_id);
    dassert(app->is_stateful, "don't support stateless apps currently, id(%d)", request.app.app_id);

    dsn::gpid parent_gpid = request.parent_config.pid;
    dsn::gpid child_gpid = request.child_config.pid;
    partition_configuration parent_config = app->partitions[parent_gpid.get_partition_index()];
    partition_configuration child_config = app->partitions[child_gpid.get_partition_index()];
    config_context &parent_context = app->helpers->contexts[parent_gpid.get_partition_index()];

    if (request.parent_config.ballot < parent_config.ballot) {
        dwarn_f("gpid({}.{}) register child failed, request is out-dated, request ballot is {}, "
                "local ballot is {}",
                parent_gpid.get_app_id(),
                parent_gpid.get_partition_index(),
                request.parent_config.ballot,
                parent_config.ballot);
        response.err = ERR_INVALID_VERSION;
        return;
    }

    if (child_config.ballot != invalid_ballot) {
        dwarn_f("duplicated register child reques, gpid({}.{}) has already been registered, ballot "
                "is {}",
                child_gpid.get_app_id(),
                child_gpid.get_partition_index(),
                child_config.ballot);
        response.err = ERR_CHILD_REGISTERED;
        return;
    }

    if (parent_context.stage == config_status::pending_proposal ||
        parent_context.stage == config_status::pending_remote_sync) {
        dwarn_f("another request is syncing with remote storage, ignore this request - gpid({}.{}) "
                "register child",
                parent_gpid.get_app_id(),
                parent_gpid.get_partition_index());
        return;
    }

    ddebug_f("gpid({}.{}) will resgiter child gpid({}.{})",
             parent_gpid.get_app_id(),
             parent_gpid.get_partition_index(),
             child_gpid.get_app_id(),
             child_gpid.get_partition_index());

    parent_context.stage = config_status::pending_remote_sync;
    parent_context.msg = rpc.dsn_request();
    parent_context.pending_sync_task = add_child_on_remote_storage(rpc);
}

dsn::task_ptr meta_split_service::add_child_on_remote_storage(register_child_rpc rpc)
{
    auto &request = rpc.request();

    std::string partition_path = _state->get_partition_path(request.child_config.pid);
    blob value = dsn::json::json_forwarder<partition_configuration>::encode(request.child_config);

    return _meta_svc->get_remote_storage()->create_node(
        partition_path,
        LPC_META_STATE_HIGH,
        std::bind(&meta_split_service::on_add_child_on_remote_storage_reply,
                  this,
                  std::placeholders::_1,
                  rpc),
        value);
}

void meta_split_service::on_add_child_on_remote_storage_reply(error_code ec, register_child_rpc rpc)
{
    zauto_write_lock(app_lock());

    const auto &request = rpc.request();
    auto &response = rpc.response();

    std::shared_ptr<app_state> app = _state->get_app(request.app.app_id);
    dassert(app->status == app_status::AS_AVAILABLE || app->status == app_status::AS_DROPPING,
            "if app removed, this task should be cancelled");

    dsn::gpid parent_gpid = request.parent_config.pid;
    dsn::gpid child_gpid = request.child_config.pid;

    config_context &parent_context = app->helpers->contexts[parent_gpid.get_partition_index()];

    if (ec == ERR_TIMEOUT) { // retry register child on remote storage
        parent_context.pending_sync_task =
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             [this, parent_context, rpc]() mutable {
                                 parent_context.pending_sync_task =
                                     add_child_on_remote_storage(rpc);
                             },
                             0,
                             std::chrono::seconds(1));
    } else if (ec == ERR_OK) {
        ddebug_f("gpid({}.{}) resgiter child gpid({}.{}) on remote storage succeed",
                 parent_gpid.get_app_id(),
                 parent_gpid.get_partition_index(),
                 child_gpid.get_app_id(),
                 child_gpid.get_partition_index());

        std::shared_ptr<configuration_update_request> update_child_request(
            new configuration_update_request);
        update_child_request->config = request.child_config;
        update_child_request->info = *app;
        update_child_request->type = config_type::CT_REGISTER_CHILD;
        update_child_request->node = request.primary_address;

        partition_configuration child_config = app->partitions[child_gpid.get_partition_index()];
        child_config.secondaries = request.child_config.secondaries;

        // update local child partition configuration
        _state->update_configuration_locally(*app, update_child_request);

        parent_context.pending_sync_task = nullptr;
        parent_context.stage = config_status::not_pending;

        if (parent_context.msg) {
            response.err = ERR_OK;
            response.app = *app;
            response.parent_config = app->partitions[parent_gpid.get_partition_index()];
            response.child_config = app->partitions[child_gpid.get_partition_index()];
            parent_context.msg = nullptr;
        }
    } else {
        dassert(false, "we can't handle this right now, err = %s", ec.to_string());
    }
}

void meta_split_service::on_query_child_state(query_child_state_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    dsn::gpid parent_gpid = request.parent_gpid;

    zauto_write_lock l(app_lock());
    std::shared_ptr<app_state> app = _state->get_app(parent_gpid.get_app_id());
    dassert(app != nullptr, "get get app for app id(%d)", parent_gpid.get_app_id());

    if (app->helpers->contexts[parent_gpid.get_partition_index()].pending_sync_task != nullptr) {
        dwarn_f("gpid({}.{}) execute pending sync task, please wait and try later",
                parent_gpid.get_app_id(),
                parent_gpid.get_partition_index());
        response.err = ERR_TRY_AGAIN;
        return;
    }

    ddebug_f("gpid({}.{}) query child partition state",
             parent_gpid.get_app_id(),
             parent_gpid.get_partition_index());

    // TODO(hyc): consider and comments
    response.err = ERR_OK;
    response.partition_count = app->partition_count;
    response.ballot = invalid_ballot;

    if (parent_gpid.get_partition_index() <= app->partition_count / 2) {
        response.ballot =
            app->partitions[parent_gpid.get_partition_index() + app->partition_count / 2].ballot;
    }
}

meta_split_service::meta_split_service(meta_service *meta_srv)
{
    this->_meta_svc = meta_srv;
    this->_state = meta_srv->get_server_state();
}

} // namespace replication
} // namespace dsn
