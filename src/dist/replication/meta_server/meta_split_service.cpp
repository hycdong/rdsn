// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/fail_point.h>

#include "dist/replication/meta_server/meta_split_service.h"
#include "dist/replication/meta_server/meta_state_service_utils.h"

namespace dsn {
namespace replication {

meta_split_service::meta_split_service(meta_service *meta_srv)
{
    _meta_svc = meta_srv;
    _state = meta_srv->get_server_state();
}

// ThreadPool: THREAD_POOL_META_SERVER
void meta_split_service::start_partition_split(start_split_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    {
        zauto_write_lock l(app_lock());

        app = _state->get_app(request.app_name);
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            derror_f("app({}) is not existed or not available", request.app_name);
            response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
            response.hint_msg = fmt::format(
                "app {}", response.err == ERR_APP_NOT_EXIST ? "not existed" : "dropped");
            return;
        }

        if (request.new_partition_count != app->partition_count * 2) {
            response.err = ERR_INVALID_PARAMETERS;
            derror_f("wrong partition count: app({}), partition count({}), new_partition_count({})",
                     request.app_name,
                     app->partition_count,
                     request.new_partition_count);
            response.hint_msg =
                fmt::format("wrong partition_count, should be {}", app->partition_count * 2);
            return;
        }

        if (app->helpers->split_states.splitting_count > 0) {
            response.err = ERR_BUSY;
            derror_f("app({}) has already executing partition split",
                     request.app_name,
                     request.new_partition_count);
            response.hint_msg = "app is already executing partition split";
            return;
        }
    }

    ddebug_f("app({}) start to partition split, new_partition_count = {}",
             request.app_name,
             request.new_partition_count);

    do_start_partition_split(std::move(app), std::move(rpc));
}

// ThreadPool: THREAD_POOL_META_SERVER
void meta_split_service::do_start_partition_split(std::shared_ptr<app_state> app,
                                                  start_split_rpc rpc)
{
    auto on_write_storage_complete = [app, rpc, this]() {
        ddebug_f("app({}) update partition_count on remote storage, new_partition_count = {}",
                 app->app_name,
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
                app->helpers->split_states.status[i] = split_status::splitting;
            }
        }

        auto &response = rpc.response();
        response.err = ERR_OK;
    };

    // TODO(heuychen): consider init_partition_count
    if (app->init_partition_count <= 0) {
        app->init_partition_count = app->partition_count;
    }
    auto copy = *app;
    copy.partition_count *= 2;
    blob value = dsn::json::json_forwarder<app_info>::encode(copy);
    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), on_write_storage_complete);
}

// ThreadPool: THREAD_POOL_META_SERVER
void meta_split_service::register_child_on_meta(register_child_rpc rpc)
{
    const auto &request = rpc.request();
    const std::string &app_name = request.app.app_name;
    auto &response = rpc.response();
    response.err = ERR_IO_PENDING;

    zauto_write_lock(app_lock());
    std::shared_ptr<app_state> app = _state->get_app(app_name);
    dassert_f(app != nullptr, "app({}) is not existed", app_name);
    dassert_f(app->is_stateful, "app({}) is stateless currently", app_name);

    const gpid &parent_gpid = request.parent_config.pid;
    const auto &parent_config = app->partitions[parent_gpid.get_partition_index()];

    auto iter = app->helpers->split_states.status.find(parent_gpid.get_partition_index());
    if (iter == app->helpers->split_states.status.end()) {
        dwarn_f("app({}) partition({}) register child failed, this partition has already splitted "
                "or split has been canceled",
                app_name,
                parent_gpid);
        response.err = ERR_CHILD_REGISTERED;
        response.parent_config = parent_config;
        return;
    }

    if (iter->second != split_status::splitting) {
        derror_f("app({}) partition({}) register child failed, current partition split_status = {}",
                 app_name,
                 parent_gpid,
                 dsn::enum_to_string(iter->second));
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.parent_config.ballot != parent_config.ballot) {
        derror_f("app({}) partition({}) register child failed, request is out-dated, request "
                 "ballot = {}, "
                 "local ballot = {}",
                 app_name,
                 parent_gpid,
                 request.parent_config.ballot,
                 parent_config.ballot);
        response.err = ERR_INVALID_VERSION;
        response.parent_config = parent_config;
        return;
    }

    config_context &parent_context = app->helpers->contexts[parent_gpid.get_partition_index()];
    if (parent_context.stage == config_status::pending_proposal ||
        parent_context.stage == config_status::pending_remote_sync) {
        dwarn_f("app({}) partition({}): another request is syncing with remote storage, ignore "
                "this request",
                app_name,
                parent_gpid);
        return;
    }

    app->helpers->split_states.status.erase(parent_gpid.get_partition_index());
    app->helpers->split_states.splitting_count--;
    ddebug_f("app({}) parent({}) will register child({})",
             app_name,
             parent_gpid,
             request.child_config.pid);

    parent_context.stage = config_status::pending_remote_sync;
    parent_context.msg = rpc.dsn_request();
    parent_context.pending_sync_task = add_child_on_remote_storage(rpc, true);
}

// ThreadPool: THREAD_POOL_META_SERVER
dsn::task_ptr meta_split_service::add_child_on_remote_storage(register_child_rpc rpc,
                                                              bool create_new)
{
    const auto &request = rpc.request();
    const std::string &partition_path = _state->get_partition_path(request.child_config.pid);
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

// ThreadPool: THREAD_POOL_META_SERVER
void meta_split_service::on_add_child_on_remote_storage_reply(error_code ec,
                                                              register_child_rpc rpc,
                                                              bool create_new)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    zauto_write_lock(app_lock());
    std::shared_ptr<app_state> app = _state->get_app(request.app.app_id);
    dassert_f(app != nullptr, "app is not existed, id({})", request.app.app_id);
    dassert_f(app->status == app_status::AS_AVAILABLE || app->status == app_status::AS_DROPPING,
              "app is not available now, id({})",
              request.app.app_id);

    const gpid &parent_gpid = request.parent_config.pid;
    const gpid &child_gpid = request.child_config.pid;
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
        return;
    }
    dassert_f(ec == ERR_OK, "we can't handle this right now, err = {}", ec.to_string());
    ddebug_f("partition({}) resgiter child({}) on remote storage succeed", parent_gpid, child_gpid);

    // update local child partition configuration
    std::shared_ptr<configuration_update_request> update_child_request =
        std::make_shared<configuration_update_request>();
    update_child_request->config = request.child_config;
    update_child_request->info = *app;
    update_child_request->type = config_type::CT_REGISTER_CHILD;
    update_child_request->node = request.primary_address;

    partition_configuration child_config = app->partitions[child_gpid.get_partition_index()];
    child_config.secondaries = request.child_config.secondaries;
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
}

// ThreadPool: THREAD_POOL_META_SERVER
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

// ThreadPool: THREAD_POOL_META_SERVER
void meta_split_service::control_partition_split(control_split_rpc rpc)
{
    const auto &req = rpc.request();
    const std::string &app_name = req.app_name;
    const int32_t &parent_pidx = req.parent_pidx;
    const auto &control_type = req.control_type;
    auto &response = rpc.response();

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
        derror_f(
            "app({}) is not splitting, {} split failed", app_name, control_type_str(control_type));
        response.err = ERR_INVALID_STATE;
        response.hint_msg = "app is not splitting";
        return;
    }

    if (control_type == split_control_type::PSC_CANCEL) {
        cancel_partition_split(std::move(app), std::move(rpc));
        return;
    }

    // pause or restart partition split
    if (parent_pidx >= app->partition_count / 2) {
        derror_f("{} split for app({}) partition[{}] failed, partition is not parent partition",
                 control_type_str(control_type),
                 app_name,
                 parent_pidx);
        response.err = ERR_INVALID_PARAMETERS;
        response.hint_msg = "invalid parent partition index";
        return;
    }

    if (control_type == split_control_type::PSC_PAUSE) {
        pause_partition_split(std::move(app), rpc);
    } else {
        restart_partition_split(std::move(app), rpc);
    }
}

// ThreadPool: THREAD_POOL_META_SERVER
void meta_split_service::pause_partition_split(std::shared_ptr<app_state> app,
                                               control_split_rpc rpc)
{
    const auto &req = rpc.request();
    const std::string &app_name = req.app_name;
    const int32_t &parent_pidx = req.parent_pidx;
    auto &response = rpc.response();

    // pause single specific partition
    if (parent_pidx >= 0) {
        auto iter = app->helpers->split_states.status.find(parent_pidx);
        if (iter == app->helpers->split_states.status.end()) {
            derror_f(
                "pause split for app({}) partition[{}] failed, child partition has been registered",
                app_name,
                parent_pidx);
            response.err = ERR_CHILD_REGISTERED;
            response.hint_msg = "partition has finished split";
            return;
        }

        if (iter->second == split_status::paused) {
            dwarn_f("duplicated pause request for app({}) partition[{}], split has been paused",
                    app_name,
                    parent_pidx);
            response.err == ERR_OK;
            return;
        }

        iter->second = split_status::paused;
        response.err = ERR_OK;
        ddebug_f("app({}) partition({}) pause split", app_name, parent_pidx);
        send_stop_split_request(app, gpid(app->app_id, parent_pidx), split_control_type::PSC_PAUSE);
        return;
    }

    // pause all splitting partitions
    for (auto &kv : app->helpers->split_states.status) {
        if (kv.second == split_status::splitting) {
            const int32_t pidx = kv.first;
            kv.second = split_status::paused;
            ddebug_f("app({}) partition({}) pause split", app_name, pidx);
            send_stop_split_request(
                app, gpid(app->app_id, parent_pidx), split_control_type::PSC_PAUSE);
        }
    }
    response.err = ERR_OK;
}

// ThreadPool: THREAD_POOL_META_SERVER
void meta_split_service::restart_partition_split(std::shared_ptr<app_state> app,
                                                 control_split_rpc rpc)
{
    const auto &req = rpc.request();
    const std::string &app_name = req.app_name;
    const int32_t &parent_pidx = req.parent_pidx;
    auto &response = rpc.response();

    // restart single specific partition
    if (parent_pidx >= 0) {
        auto iter = app->helpers->split_states.status.find(parent_pidx);
        if (iter == app->helpers->split_states.status.end()) {
            derror_f("restart split for app({}) partition[{}] failed, partition is not splitting",
                     app_name,
                     parent_pidx);
            response.err = ERR_INVALID_STATE;
            response.hint_msg = "partition is not splitting";
            return;
        }

        if (iter->second == split_status::splitting) {
            dwarn_f("duplicated restart request for app({}) partition[{}], partition is already "
                    "splitting",
                    app_name,
                    parent_pidx);
            response.err == ERR_OK;
            return;
        }
        iter->second = split_status::splitting;
        response.err = ERR_OK;
        ddebug_f("app({}) partition({}) restart split", app_name, parent_pidx);
        return;
    }

    // restart all paused partitions
    for (auto &kv : app->helpers->split_states.status) {
        if (kv.second == split_status::paused) {
            kv.second = split_status::splitting;
            ddebug_f("app({}) partition({}) restart split", app_name, kv.first);
        }
    }
    response.err = ERR_OK;
}

// ThreadPool: THREAD_POOL_META_SERVER
void meta_split_service::send_stop_split_request(std::shared_ptr<app_state> app,
                                                 const gpid &pid,
                                                 split_control_type::type type)
{
    FAIL_POINT_INJECT_F("meta_split_send_stop_split_request", [](dsn::string_view) {});

    auto req = make_unique<stop_split_request>();
    req->pid = pid;
    req->partition_count = app->partition_count;
    req->type = type;

    const auto &primary_addr = app->partitions[pid.get_partition_index()].primary;
    ddebug_f("send {} split request to node({}) app({}), partition({})",
             control_type_str(type),
             primary_addr.to_string(),
             app->app_name,
             pid);
    stop_split_rpc rpc(std::move(req), RPC_STOP_SPLIT, 0_ms, 0, pid.thread_hash());
    rpc.call(
        primary_addr,
        _meta_svc->tracker(),
        [this, app, pid, primary_addr, type, rpc](error_code err) mutable {
            if (err == ERR_OBJECT_NOT_FOUND || err == ERR_INVALID_STATE) {
                derror_f("send {} split request to node({}) app({}), partition({}) failed, err = "
                         "{}, retry",
                         control_type_str(type),
                         primary_addr.to_string(),
                         app->app_name,
                         pid,
                         err);
                tasking::enqueue(
                    LPC_META_CALLBACK,
                    _meta_svc->tracker(),
                    std::bind(&meta_split_service::send_stop_split_request, this, app, pid, type),
                    0,
                    std::chrono::seconds(1));
            }
            ddebug_f("send {} split request to node({}) app({}), partition({}) succeed",
                     control_type_str(type),
                     primary_addr.to_string(),
                     app->app_name,
                     pid);
        });
}

// ThreadPool: THREAD_POOL_META_SERVER
void meta_split_service::cancel_partition_split(std::shared_ptr<app_state> app,
                                                control_split_rpc rpc)
{
    const auto &req = rpc.request();
    auto &response = rpc.response();

    if (req.old_partition_count != app->partition_count / 2) {
        derror_f("wrong partition count: app({}), partition count({}), old_partition_count({})",
                 app->app_name,
                 app->partition_count,
                 req.old_partition_count);
        response.err = ERR_INVALID_PARAMETERS;
        response.hint_msg =
            fmt::format("wrong partition_count, should be {}", app->partition_count / 2);
        return;
    }

    if (app->helpers->split_states.splitting_count != req.old_partition_count) {
        derror_f("cancel split for app({}) failed, some partitions have finished split",
                 app->app_name);
        response.err = ERR_CHILD_REGISTERED;
        response.hint_msg = "some partitions finish split";
        return;
    }

    // send cancel split request to replica server
    for (auto &kv : app->helpers->split_states.status) {
        ddebug_f("app({}) partition({}) cancel split, old status = {}",
                 app->app_name,
                 kv.first,
                 dsn::enum_to_string(kv.second));
        kv.second = split_status::canceling;
        send_stop_split_request(app, gpid(app->app_id, kv.first), split_control_type::PSC_CANCEL);
    }

    do_cancel_partition_split(std::move(app), rpc);
}

// ThreadPool: THREAD_POOL_META_SERVER
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
