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

meta_split_service::meta_split_service(meta_service *meta_srv)
{
    _meta_svc = meta_srv;
    _state = meta_srv->get_server_state();
}

void meta_split_service::app_partition_split(app_partition_split_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    {
        zauto_write_lock l(app_lock());

        app = _state->get_app(request.app_name);
        if (!app || app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_APP_NOT_EXIST;
            dwarn_f("client({}) sent split request with invalid app({}), app is not existed or "
                    "unavailable",
                    rpc.remote_address().to_string(),
                    request.app_name);
            return;
        }

        response.app_id = app->app_id;
        response.partition_count = app->partition_count;

        // new_partition_count != old_partition_count*2
        if (request.new_partition_count != app->partition_count * 2) {
            response.err = ERR_INVALID_PARAMETERS;
            dwarn_f("client({}) sent split request with wrong partition count: app({}), partition "
                    "count({}),"
                    "new_partition_count({})",
                    rpc.remote_address().to_string(),
                    request.app_name,
                    app->partition_count,
                    request.new_partition_count);
            return;
        }

        for (const auto &partition_config : app->partitions) {
            // partition already during split
            if (partition_config.ballot < 0) {
                response.err = ERR_BUSY;
                dwarn_f("app is already during partition split, client({}) sent repeated split "
                        "request: app({}), new_partition_count({})",
                        rpc.remote_address().to_string(),
                        request.app_name,
                        request.new_partition_count);
                return;
            }
        }
    }

    ddebug_f("app({}) start to partition split, new_partition_count={}",
             request.app_name,
             request.new_partition_count);

    do_app_partition_split(std::move(app), std::move(rpc));
}

void meta_split_service::do_app_partition_split(std::shared_ptr<app_state> app,
                                                app_partition_split_rpc rpc)
{
    auto on_write_storage_complete = [app, rpc, this]() {
        ddebug_f("app({}) update partition count on remote storage, new partition count is {}",
                 app->app_name.c_str(),
                 app->partition_count * 2);

        zauto_write_lock l(app_lock());
        app->helpers->split_states.splitting_count = app->partition_count;
        app->partition_count *= 2;
        app->helpers->contexts.resize(app->partition_count);
        app->partitions.resize(app->partition_count);

        for (int i = 0; i < app->partition_count; ++i) {
            app->helpers->contexts[i].config_owner = &app->partitions[i];
            if (i >= app->partition_count / 2) {
                app->partitions[i].ballot = invalid_ballot;
                app->partitions[i].pid = gpid(app->app_id, i);
            } else {
                // TODO(heyuchen): pause add
                app->helpers->split_states.status[i] = split_status::splitting;
                ddebug_f("hyc: app({}) partition[{}] start split", app->app_name, i);
            }
        }

        auto &response = rpc.response();
        response.err = ERR_OK;
        response.partition_count = app->partition_count;
    };

    if (app->init_partition_count <= 0) {
        app->init_partition_count = app->partition_count;
    }
    auto copy = *app;
    copy.partition_count *= 2;
    blob value = dsn::json::json_forwarder<app_info>::encode(copy);

    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), on_write_storage_complete);
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
    // check child gpid, split might cancel
    if (child_gpid.get_partition_index() > app->partition_count) {
        dwarn_f("gpid({}.{}) register child failed, coz split is canceled",
                parent_gpid.get_app_id(),
                parent_gpid.get_partition_index());
        response.err = ERR_REJECT;
        return;
    }

    partition_configuration parent_config = app->partitions[parent_gpid.get_partition_index()];
    partition_configuration child_config = app->partitions[child_gpid.get_partition_index()];
    config_context &parent_context = app->helpers->contexts[parent_gpid.get_partition_index()];

    // TODO(heyuchen): pause remove
    //    if ((parent_config.partition_flags & pc_flags::child_dropped) == pc_flags::child_dropped)
    //    {
    //        dwarn_f("gpid({}.{}) register child failed, coz split is paused",
    //                parent_gpid.get_app_id(),
    //                parent_gpid.get_partition_index());
    //        response.err = ERR_CHILD_DROPPED;
    //        return;
    //    }

    // TODO(heyuchen): pause add
    auto iter = app->helpers->split_states.status.find(parent_gpid.get_partition_index());
    if (iter == app->helpers->split_states.status.end() ||
        iter->second != split_status::splitting) {
        dwarn_f("gpid({}.{}) register child failed, because partition is not splitting",
                parent_gpid.get_app_id(),
                parent_gpid.get_partition_index());
        // TODO(heyuchen): change error code
        response.err = ERR_CHILD_DROPPED;
        return;
    }

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
        dwarn_f(
            "duplicated register child request, gpid({}.{}) has already been registered, ballot "
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

    app->helpers->split_states.status.erase(parent_gpid.get_partition_index());
    app->helpers->split_states.splitting_count--;
    ddebug_f("gpid({}.{}) will resgiter child gpid({}.{})",
             parent_gpid.get_app_id(),
             parent_gpid.get_partition_index(),
             child_gpid.get_app_id(),
             child_gpid.get_partition_index());

    parent_context.stage = config_status::pending_remote_sync;
    parent_context.msg = rpc.dsn_request();
    parent_context.pending_sync_task = add_child_on_remote_storage(rpc, true);
}

dsn::task_ptr meta_split_service::add_child_on_remote_storage(register_child_rpc rpc,
                                                              bool create_new)
{
    auto &request = rpc.request();

    std::string partition_path = _state->get_partition_path(request.child_config.pid);
    blob value = dsn::json::json_forwarder<partition_configuration>::encode(request.child_config);

    if (create_new) {
        return _meta_svc->get_remote_storage()->create_node(
            partition_path,
            LPC_META_STATE_HIGH,
            std::bind(&meta_split_service::on_add_child_on_remote_storage_reply,
                      this,
                      std::placeholders::_1,
                      rpc,
                      create_new),
            value);
    } else {
        return _meta_svc->get_remote_storage()->set_data(
            partition_path,
            value,
            LPC_META_STATE_HIGH,
            std::bind(&meta_split_service::on_add_child_on_remote_storage_reply,
                      this,
                      std::placeholders::_1,
                      rpc,
                      create_new),
            _meta_svc->tracker());
    }
}

void meta_split_service::on_add_child_on_remote_storage_reply(error_code ec,
                                                              register_child_rpc rpc,
                                                              bool create_new)
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

    if (ec == ERR_TIMEOUT ||
        (ec == ERR_NODE_ALREADY_EXIST && create_new)) { // retry register child on remote storage
        bool retry_create_new = (ec == ERR_TIMEOUT) ? create_new : false;
        int delay = (ec == ERR_TIMEOUT) ? 1 : 0;
        parent_context.pending_sync_task =
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             [this, parent_context, rpc, retry_create_new]() mutable {
                                 parent_context.pending_sync_task =
                                     add_child_on_remote_storage(rpc, retry_create_new);
                             },
                             0,
                             std::chrono::seconds(delay));
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
            // TODO(heyuchen): pause add
            // app->helpers->split_states.status.erase(parent_gpid.get_partition_index());
            // app->helpers->split_states.splitting_count--;
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

// TBD(heyuchen): refactor this function
void meta_split_service::control_partition_split(control_split_rpc rpc)
{
    const auto &req = rpc.request();
    const std::string &app_name = req.app_name;
    const int32_t &old_partition_count = req.partition_count_before_split;
    const int32_t &parent_pidx = req.parent_pidx;
    const auto &control_type = req.control_type;

    auto &response = rpc.response();
    response.err = ERR_OK;

    zauto_write_lock l(app_lock());

    std::shared_ptr<app_state> app = _state->get_app(app_name);
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        derror_f("app({}) is not existed or not available", app_name);
        response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
        response.hint_msg =
            fmt::format("app {}", response.err == ERR_APP_NOT_EXIST ? "not existed" : "dropped");
        return;
    }

    if (old_partition_count * 2 != app->partition_count) {
        derror_f("pause split for app({}) failed, wrong partition count, request({}) VS app({})",
                 app_name,
                 old_partition_count,
                 app->partition_count);
        response.err = ERR_INVALID_PARAMETERS;
        response.hint_msg = "wrong partition count";
        return;
    }

    if (app->helpers->split_states.splitting_count <= 0) {
        derror_f("pause split for app({}) failed, current app is not splitting", app_name);
        response.err = ERR_INVALID_STATE;
        response.hint_msg = "app is not splitting";
        return;
    }

    if (parent_pidx >= old_partition_count) {
        derror_f("pause split for app({}) partition[{}] failed, partition is not parent partition",
                 app_name,
                 parent_pidx);
        response.err = ERR_INVALID_PARAMETERS;
        response.hint_msg = "invalid parent partition index";
        return;
    }

    if (control_type == split_control_type::PSC_PAUSE) {
        if (parent_pidx >= 0) {
            // pause single specific partition
            auto iter = app->helpers->split_states.status.find(parent_pidx);
            if (iter == app->helpers->split_states.status.end()) {
                derror_f(
                    "pause split for app({}) partition[{}] failed, this parent has registered its "
                    "child",
                    app_name,
                    parent_pidx);
                response.err = ERR_CHILD_REGISTERED;
                response.hint_msg = "parent partition has finished split";
                return;
            }

            if (iter->second == split_status::paused) {
                derror_f("duplicated pause split for app({}) partition[{}], split has been paused",
                         app_name,
                         parent_pidx);
                response.err == ERR_NO_NEED_OPERATE;
                response.hint_msg = "parent partition has paused split";
                return;
            }

            iter->second = split_status::paused;
            ddebug_f("app({}) partition({}) pause split", app_name, parent_pidx);
            send_stop_split_request(app->partitions[parent_pidx].primary,
                                    app_name,
                                    gpid(app->app_id, parent_pidx),
                                    app->partition_count,
                                    split_control_type::PSC_PAUSE);
        } else { // pause all splitting partitions
            for (auto &kv : app->helpers->split_states.status) {
                if (kv.second == split_status::splitting) {
                    const int32_t pidx = kv.first;
                    kv.second = split_status::paused;
                    ddebug_f("app({}) partition({}) pause split", app_name, pidx);
                    send_stop_split_request(app->partitions[pidx].primary,
                                            app_name,
                                            gpid(app->app_id, pidx),
                                            app->partition_count,
                                            split_control_type::PSC_PAUSE);
                }
            }
        }
    }

    if (control_type == split_control_type::PSC_RESTART) {
        if (parent_pidx >= 0) {
            // restart single specific partition
            auto iter = app->helpers->split_states.status.find(parent_pidx);
            if (iter == app->helpers->split_states.status.end()) {
                derror_f(
                    "restart split for app({}) partition[{}] failed, partition is not splitting",
                    app_name,
                    parent_pidx);
                response.err = ERR_INVALID_STATE;
                response.hint_msg = "parent partition is not splitting";
                return;
            }

            if (iter->second == split_status::splitting) {
                derror_f(
                    "duplicated restart split for app({}) partition[{}], split has been started",
                    app_name,
                    parent_pidx);
                response.err == ERR_NO_NEED_OPERATE;
                response.hint_msg = "parent partition has started split";
                return;
                return;
            }
            iter->second = split_status::splitting;
            ddebug_f("app({}) partition({}) restart split", app_name, parent_pidx);
        } else { // restart all paused partitions
            for (auto &kv : app->helpers->split_states.status) {
                if (kv.second == split_status::paused) {
                    kv.second = split_status::splitting;
                    ddebug_f("app({}) partition({}) restart split", app_name, kv.first);
                }
            }
        }
    }

    if (control_type == split_control_type::PSC_CANCEL) {
        if (app->helpers->split_states.splitting_count != old_partition_count) {
            derror_f("cancel split for app({}) failed, some partitions have finished split",
                     app_name);
            response.err = ERR_CHILD_REGISTERED;
            response.hint_msg = "some partitions finish split";
            return;
        }

        for (auto &kv : app->helpers->split_states.status) {
            ddebug_f("app({}) partition({}) cancel split, old status = {}",
                     app_name,
                     kv.first,
                     dsn::enum_to_string(kv.second));
            kv.second = split_status::canceling;
            send_stop_split_request(app->partitions[kv.first].primary,
                                    app_name,
                                    gpid(app->app_id, kv.first),
                                    app->partition_count,
                                    split_control_type::PSC_CANCEL);
        }

        do_cancel_partition_split(std::move(app), std::move(rpc));
    }
}

void meta_split_service::send_stop_split_request(const rpc_address &primary_addr,
                                                 const std::string &app_name,
                                                 const gpid &pid,
                                                 const int32_t partition_count,
                                                 split_control_type::type type)
{
    auto req = make_unique<stop_split_request>();
    req->pid = pid;
    req->partition_count = partition_count;
    req->type = type;

    ddebug_f("send control split request to node({}) app({}), pid({}), split_control_type({})",
             primary_addr.to_string(),
             app_name,
             pid,
             dsn::enum_to_string(type));
    stop_split_rpc rpc(std::move(req), RPC_STOP_SPLIT, 0_ms, 0, pid.thread_hash());
    rpc.call(primary_addr, _meta_svc->tracker(), [this, type, rpc](error_code err) mutable {
        if (err == ERR_OBJECT_NOT_FOUND || err == ERR_INVALID_STATE) {
            // TODO(heyuchen): retry pause split
            dwarn_f(" split failed, err = {}", err);
        }
        ddebug_f("control split succeed, control type = {}", type);
    });
}

void meta_split_service::query_partition_split(query_split_rpc rpc)
{
    const std::string &app_name = rpc.request().app_name;
    auto &response = rpc.response();
    response.err = ERR_OK;

    zauto_write_lock l(app_lock());

    std::shared_ptr<app_state> app = _state->get_app(app_name);
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        derror_f("app({}) is not existed or not available", app_name);
        response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
        response.hint_msg =
            fmt::format("app {}", response.err == ERR_APP_NOT_EXIST ? "not existed" : "dropped");
        return;
    }

    if (app->helpers->split_states.splitting_count <= 0) {
        derror_f("query split for app({}) failed, current app is not splitting", app_name);
        response.err = ERR_INVALID_STATE;
        response.hint_msg = "app is not splitting";
        return;
    }

    response.new_partition_count = app->partition_count;
    response.status = app->helpers->split_states.status;
    ddebug_f("query partition split succeed, app({}), partition_count({}), splitting_count({})",
             app->app_name,
             response.new_partition_count,
             response.status.size());
}

void meta_split_service::do_cancel_partition_split(std::shared_ptr<app_state> app,
                                                   control_split_rpc rpc)
{
    auto on_write_storage_complete = [app, rpc, this]() {
        ddebug_f("app({}) update partition count on remote storage, new partition count is {}",
                 app->app_name,
                 app->partition_count / 2);

        zauto_write_lock l(app_lock());

        app->partition_count /= 2;

        for (int i = app->partition_count; i < app->partition_count * 2; ++i) {
            app->partitions.erase(app->partitions.cbegin() + i);
            app->helpers->contexts.erase(app->helpers->contexts.cbegin() + i);
        }

        app->helpers->split_states.status.clear();
        app->helpers->split_states.splitting_count = 0;

        auto &response = rpc.response();
        response.err = ERR_OK;
    };

    auto copy = *app;
    copy.partition_count /= 2;
    blob value = dsn::json::json_forwarder<app_info>::encode(copy);
    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), on_write_storage_complete);
}

} // namespace replication
} // namespace dsn
