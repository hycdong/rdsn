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

        for (const auto &partition_config : app->partitions) {
            // if there's ongoing split already.
            if (partition_config.ballot < 0) {
                response.err = ERR_BUSY;
                dwarn_f("app is already during partition split, client({}) sent repeated split request: app({}), new_partition_count({})",
                        ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                        request.app_name,
                        request.new_partition_count);
                return;
            }

            // if split is paused or canceled
            if ((partition_config.partition_flags & pc_flags::child_dropped) == pc_flags::child_dropped) {
                response.err = ERR_CHILD_DROPPED;
                dwarn_f("client({}) sent split request with partition{}.{} flag is child_dropped, "
                        "app{} partition split might be paused or canceled, please check state",
                        ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                        partition_config.pid.get_app_id(),
                        partition_config.pid.get_partition_index(),
                        request.app_name);
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
    // check child gpid, split might cancel
    if(child_gpid.get_partition_index() > app->partition_count){
        dwarn_f("gpid({}.{}) register child failed, coz split is canceled",
                parent_gpid.get_app_id(),
                parent_gpid.get_partition_index());
        response.err = ERR_REJECT;
        return;
    }

    partition_configuration parent_config = app->partitions[parent_gpid.get_partition_index()];
    partition_configuration child_config = app->partitions[child_gpid.get_partition_index()];
    config_context &parent_context = app->helpers->contexts[parent_gpid.get_partition_index()];

    if((parent_config.partition_flags & pc_flags::child_dropped) == pc_flags::child_dropped) {
        dwarn_f("gpid({}.{}) register child failed, coz split is paused",
                parent_gpid.get_app_id(),
                parent_gpid.get_partition_index());
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
        dwarn_f("duplicated register child request, gpid({}.{}) has already been registered, ballot "
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
    parent_context.pending_sync_task = add_child_on_remote_storage(rpc, true);
}

dsn::task_ptr meta_split_service::add_child_on_remote_storage(register_child_rpc rpc, bool create_new)
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

void meta_split_service::on_add_child_on_remote_storage_reply(error_code ec, register_child_rpc rpc, bool create_new)
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

void meta_split_service::control_single_partition_split(control_single_partition_split_rpc rpc)
{
    const auto &request = rpc.request();
    int pidx = request.parent_partition_index;
    bool is_pause = request.is_pause;
    std::string op_name = is_pause ? "pause" : "restart";
    ddebug_f("{} table {} partition[{}] split", op_name, request.app_name, pidx);

    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    { // validate rpc parameters

        zauto_write_lock l(app_lock());

        // if app is not available
        app = _state->get_app(request.app_name);
        if (!app || app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_APP_NOT_EXIST;
            dwarn_f("client({}) sent {} split request with invalid app({}), app is not existed or "
                    "unavailable",
                    ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                    op_name,
                    request.app_name);
            return;
        }

        response.app_id = app->app_id;
        response.partition_count = app->partition_count;

        if (pidx < 0 || pidx >= app->partition_count / 2) {
            response.err = ERR_INVALID_PARAMETERS;
            dwarn_f("client({}) sent {} split request with wrong partition index({}), app({}) "
                    "partition_count({})",
                    ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                    op_name,
                    pidx,
                    request.app_name,
                    app->partition_count);
            return;
        }

        if (is_pause && app->partitions[pidx + app->partition_count / 2].ballot != -1) {
            response.err = ERR_CHILD_REGISTERED;
            dwarn_f("client({}) sent pause split request with partition index({}), but its child "
                    "partition has been registered",
                    ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                    pidx);
            return;
        }

        if ((is_pause && ((app->partitions[pidx].partition_flags &
                         pc_flags::child_dropped) == pc_flags::child_dropped)) ||
            (!is_pause && ((app->partitions[pidx].partition_flags & pc_flags::child_dropped) == 0))) {
            ddebug_f("client({}) sent {} split request with partition index({}), this partition "
                     "has already marked as {}, partition flag is {}",
                     ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                     op_name,
                     pidx,
                     op_name,
                     app->partitions[pidx].partition_flags);
            response.err = ERR_NO_NEED_OPERATE;
            return;
        }
    }
    // update partition flag to remote storage
    update_single_partition_split_flag(std::move(app), pidx, is_pause, std::move(rpc));
}

void meta_split_service::update_single_partition_split_flag(std::shared_ptr<app_state> app,
                                                            int pidx,
                                                            bool is_pause,
                                                            control_single_partition_split_rpc rpc)
{
    auto on_update_partition_flag = [this, app, pidx, is_pause, rpc](dsn::error_code err) mutable {
        if (err == dsn::ERR_OK) {
            zauto_write_lock l(app_lock());

            if (is_pause) {
                app->partitions[pidx].partition_flags |= pc_flags::child_dropped;
            } else {
                app->partitions[pidx].partition_flags &= (~pc_flags::child_dropped);
            }

            auto &response = rpc.response();
            response.err = ERR_OK;
        } else if (err == dsn::ERR_TIMEOUT) {
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             std::bind(&meta_split_service::update_single_partition_split_flag,
                                       this,
                                       app,
                                       pidx,
                                       is_pause,
                                       rpc),
                             0,
                             std::chrono::seconds(1));
        } else {
            dassert(false, "we can't handle this right now, err = %s", err.to_string());
        }
    };

    partition_configuration &pc = app->partitions[pidx];
    if (is_pause) {
        pc.partition_flags |= pc_flags::child_dropped;
    } else {
        pc.partition_flags &= (~pc_flags::child_dropped);
    }

    blob json_partition = dsn::json::json_forwarder<partition_configuration>::encode(pc);
    std::string partition_path = _state->get_partition_path(pc.pid);
    _meta_svc->get_remote_storage()->set_data(
        partition_path, json_partition, LPC_META_STATE_HIGH, on_update_partition_flag);
}

void meta_split_service::cancel_app_partition_split(cancel_app_partition_split_rpc rpc)
{
    const auto &req = rpc.request();
    const std::string app_name = req.app_name;
    int original_partition_count = req.original_partition_count;
    bool is_force = req.is_force;
    ddebug_f("cancel table {} partition split, original partition count={}, force cancel={}",
             app_name,
             original_partition_count,
             is_force ? "true" : "false");

    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    {
        zauto_write_lock l(app_lock());

        // if app is not available
        app = _state->get_app(app_name);
        if (!app || app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_APP_NOT_EXIST;
            dwarn_f(
                "client({}) sent cancel split request with invalid app({}), app is not existed or "
                "unavailable",
                ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                app_name);
            return;
        }

        response.app_id = app->app_id;
        response.partition_count = app->partition_count;

        // if origin_partition_count * 2 != partition_count
        if (original_partition_count * 2 != app->partition_count) {
            response.err = ERR_INVALID_PARAMETERS;
            dwarn_f("client({}) sent cancel split request with wrong partition count({}), app({}) "
                    "partition_count({})",
                    ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                    original_partition_count,
                    app_name,
                    app->partition_count);
            return;
        }

        std::vector<dsn::gpid> registered_gpid;
        for (int i = original_partition_count; i < app->partition_count; ++i) {
            if (app->partitions[i].ballot > 0) {
                registered_gpid.push_back(app->partitions[i].pid);
            }
        }

        int registered_count = registered_gpid.size();
        // only force cancel supported when some child partitions registered
        if (registered_count > 0 && !is_force) {
            dwarn_f("client({}) sent not force cancel app {} split request, but {} child partition "
                    "has been registed, can only be force canceled",
                    ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                    app_name,
                    registered_count);
            response.err = ERR_REJECT;
            return;
        }

        // if all child partition registered
        if (registered_count == original_partition_count) {
            dwarn_f("client({}) sent cancel app {} split request, but all child partitions have "
                    "been registered",
                    ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                    app_name);
            response.err = ERR_CHILD_REGISTERED;
            return;
        }

        // update partition count
        do_cancel_app_partition_split(app, rpc);

        // remove useless child partitions
        for (int i = 0; i < registered_count; ++i) {
            remove_child_partition(registered_gpid[i]);
        }
    }
}

void meta_split_service::do_cancel_app_partition_split(std::shared_ptr<app_state> app, cancel_app_partition_split_rpc rpc)
{
    auto on_write_storage_complete = [app, rpc, this](error_code ec){
        if(ec == ERR_OK){
            ddebug_f(
                "app {} write new partition count on remote storage, new partition count is {}",
                app->app_name.c_str(),
                app->partition_count / 2);

            zauto_write_lock l(app_lock());
            app->partition_count /= 2;

            for(int i = app->partition_count; i < app->partition_count*2; ++i){
                app->partitions.erase(app->partitions.cbegin()+i);
                app->helpers->contexts.erase(app->helpers->contexts.cbegin()+i);
            }

            ddebug_f("partitions size: {}, contexts size: {}", app->partitions.size(), app->helpers->contexts.size());

            auto &response = rpc.response();
            response.err = ERR_OK;
            response.partition_count = app->partition_count;

        } else if(ec == ERR_TIMEOUT){
            dwarn_f("remote storage is not available now, please try it later");
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             std::bind(&meta_split_service::do_cancel_app_partition_split,
                                       this,
                                       std::move(app),
                                       std::move(rpc)),
                             0,
                             std::chrono::seconds(1));
        }else{
            dassert(false, "failed to write to remote stroage, error is %s", ec.to_string());
        }
    };

    auto copy = *app;
    copy.partition_count /= 2;
    blob value = dsn::json::json_forwarder<app_info>::encode(copy);

    _meta_svc->get_remote_storage()->set_data(_state->get_app_path(*app),
                                              std::move(value),
                                              LPC_META_STATE_HIGH,
                                              on_write_storage_complete,
                                              _meta_svc->tracker());
}

void meta_split_service::remove_child_partition(gpid pid)
{
    auto callback = [pid, this](error_code ec){
        if(ec == ERR_OK){
            ddebug_f("remove partition({}.{}) succeed", pid.get_app_id(), pid.get_partition_index());
        }else if(ec == ERR_TIMEOUT){
            dwarn_f("remote storage is not available now, please try it later");
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             std::bind(&meta_split_service::remove_child_partition,
                                       this,
                                       std::move(pid)),
                             0,
                             std::chrono::seconds(1));
        } else if(ec == dsn::ERR_OBJECT_NOT_FOUND){
            ddebug_f("try to remove partition({}.{}), not existed", pid.get_app_id(), pid.get_partition_index());
        }
    };

    std::string partition_path = _state->get_partition_path(pid);
    _meta_svc->get_remote_storage()->delete_node(
            partition_path, true, LPC_DEFAULT_CALLBACK, callback, nullptr);
}

void meta_split_service::clear_partition_split_flag(clear_partition_split_flag_rpc rpc)
{
    const auto &req = rpc.request();
    const std::string app_name = req.app_name;
    ddebug_f("clear table {} partition split flags", app_name);

    auto &response = rpc.response();
    response.err = ERR_OK;
    std::vector<int> indexs;

    std::shared_ptr<app_state> app;
    {
        zauto_write_lock l(app_lock());

        // if app is not available
        app = _state->get_app(app_name);
        if (!app || app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_APP_NOT_EXIST;
            dwarn_f("client({}) sent clear split flag request with invalid app({}), app is not "
                    "existed or "
                    "unavailable",
                    ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                    app_name);
            return;
        }

        response.app_id = app->app_id;
        response.partition_count = app->partition_count;

        for (int i = 0; i < app->partition_count; ++i) {
            if ((app->partitions[i].partition_flags & pc_flags::child_dropped) ==
                pc_flags::child_dropped) {
                indexs.push_back(i);
            }
        }
        _need_clear_flag_count.store(indexs.size());
    }

    for(int i = 0; i < indexs.size(); ++i){
        do_clear_flag(app, indexs[i], rpc);
    }
}

void meta_split_service::do_clear_flag(std::shared_ptr<app_state> app, int pidx, clear_partition_split_flag_rpc rpc)
{
    auto clear_partition_flag = [this, app, pidx, rpc](dsn::error_code err) mutable {
        if (err == dsn::ERR_OK) {
            zauto_write_lock l(app_lock());

            app->partitions[pidx].partition_flags &= (~pc_flags::child_dropped);

            int count = this->_need_clear_flag_count.load() - 1;
            this->_need_clear_flag_count.store(count);

            if(count == 0){
                auto &response = rpc.response();
                response.err = ERR_OK;
            }
        } else if (err == dsn::ERR_TIMEOUT) {
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             std::bind(&meta_split_service::do_clear_flag,
                                       this,
                                       app,
                                       pidx,
                                       rpc),
                             0,
                             std::chrono::seconds(1));
        } else {
            dassert(false, "we can't handle this right now, err = %s", err.to_string());
        }
    };

    partition_configuration &pc = app->partitions[pidx];
    pc.partition_flags &= (~pc_flags::child_dropped);

    blob json_partition = dsn::json::json_forwarder<partition_configuration>::encode(pc);
    std::string partition_path = _state->get_partition_path(pc.pid);
    _meta_svc->get_remote_storage()->set_data(
        partition_path, json_partition, LPC_META_STATE_HIGH, clear_partition_flag);
}

meta_split_service::meta_split_service(meta_service *meta_srv)
{
    this->_meta_svc = meta_srv;
    this->_state = meta_srv->get_server_state();
    this->_need_clear_flag_count.store(0);
}

} // namespace replication
} // namespace dsn
