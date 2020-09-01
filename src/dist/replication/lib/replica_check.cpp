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

/*
 * Description:
 *     replica membership state periodical checking
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"

#include "dist/replication/lib/duplication/replica_duplicator_manager.h"

#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/utility/fail_point.h>

namespace dsn {
namespace replication {

void replica::init_group_check()
{
    _checker.only_one_thread_access();

    FAIL_POINT_INJECT_F("replica_init_group_check", [](dsn::string_view) {});

    ddebug("%s: init group check", name());

    if (partition_status::PS_PRIMARY != status() || _options->group_check_disabled)
        return;

    dassert(nullptr == _primary_states.group_check_task, "");
    _primary_states.group_check_task =
        tasking::enqueue_timer(LPC_GROUP_CHECK,
                               &_tracker,
                               [this] { broadcast_group_check(); },
                               std::chrono::milliseconds(_options->group_check_interval_ms),
                               get_gpid().thread_hash());
}

void replica::broadcast_group_check(split_status::type meta_split_status)
{
    FAIL_POINT_INJECT_F("replica_broadcast_group_check", [](dsn::string_view) {});

    dassert(nullptr != _primary_states.group_check_task, "");

    ddebug("%s: start to broadcast group check", name());

    if (_primary_states.group_check_pending_replies.size() > 0) {
        dwarn("%s: %u group check replies are still pending when doing next round check, cancel "
              "first",
              name(),
              static_cast<int>(_primary_states.group_check_pending_replies.size()));

        for (auto it = _primary_states.group_check_pending_replies.begin();
             it != _primary_states.group_check_pending_replies.end();
             ++it) {
            it->second->cancel(true);
        }
        _primary_states.group_check_pending_replies.clear();
    }

    for (auto it = _primary_states.statuses.begin(); it != _primary_states.statuses.end(); ++it) {
        if (it->first == _stub->_primary_address)
            continue;

        ::dsn::rpc_address addr = it->first;
        std::shared_ptr<group_check_request> request(new group_check_request);

        request->app = _app_info;
        request->node = addr;
        _primary_states.get_replica_config(it->second, request->config);
        request->last_committed_decree = last_committed_decree();
        request->__set_confirmed_decree(_duplication_mgr->min_confirmed_decree());

        // TODO(heyuchen):
        // refactor, remove primary split_status
        if (_split_status != split_status::NOT_SPLIT) {
            request->__set_primary_split_status(_split_status);
            if (_split_status == split_status::SPLITTING) {
                if (_child_gpid.get_app_id() > 0) {
                    request->__set_child_gpid(_child_gpid);
                } else {
                    // TODO(heyuchen): consider it
                    derror_replica("partition is splitting but child_gpid({}) is invalid, cleanup "
                                   "split context",
                                   _child_gpid);
                    parent_cleanup_split_context();
                }
            }
        }

        // TODO(heyuchen): new add
        if(request->config.status == partition_status::PS_SECONDARY){
            if(meta_split_status == split_status::PAUSED || meta_split_status == split_status::CANCELING){
                request->__set_meta_split_status(meta_split_status);
            }
        }

        if (request->config.status == partition_status::PS_POTENTIAL_SECONDARY) {
            auto it = _primary_states.learners.find(addr);
            dassert(
                it != _primary_states.learners.end(), "learner %s is missing", addr.to_string());
            request->config.learner_signature = it->second.signature;
        }

        ddebug("%s: send group check to %s with state %s",
               name(),
               addr.to_string(),
               enum_to_string(it->second));

        // ddebug_replica("hyc stopping_split count = {}", _primary_states.stopping_split.size());

        dsn::task_ptr callback_task =
            rpc::call(addr,
                      RPC_GROUP_CHECK,
                      *request,
                      &_tracker,
                      [=](error_code err, group_check_response &&resp) {
                          auto alloc = std::make_shared<group_check_response>(std::move(resp));
                          on_group_check_reply(err, request, alloc);
                      },
                      std::chrono::milliseconds(0),
                      get_gpid().thread_hash());

        _primary_states.group_check_pending_replies[addr] = callback_task;
    }

    // send empty prepare when necessary
    if (!_options->empty_write_disabled &&
        dsn_now_ms() >= _primary_states.last_prepare_ts_ms + _options->group_check_interval_ms) {
        mutation_ptr mu = new_mutation(invalid_decree);
        mu->add_client_request(RPC_REPLICATION_WRITE_EMPTY, nullptr);
        init_prepare(mu, false);
    }
}

void replica::on_group_check(const group_check_request &request,
                             /*out*/ group_check_response &response)
{
    _checker.only_one_thread_access();

    ddebug_replica("process group check, primary = {}, ballot = {}, status = {}, "
                   "last_committed_decree = {}, confirmed_decree = {}",
                   request.config.primary.to_string(),
                   request.config.ballot,
                   enum_to_string(request.config.status),
                   request.last_committed_decree,
                   request.__isset.confirmed_decree ? request.confirmed_decree : invalid_decree);

    if (request.config.ballot < get_ballot()) {
        response.err = ERR_VERSION_OUTDATED;
        dwarn("%s: on_group_check reply %s", name(), response.err.to_string());
        return;
    } else if (request.config.ballot > get_ballot()) {
        if (!update_local_configuration(request.config)) {
            response.err = ERR_INVALID_STATE;
            dwarn("%s: on_group_check reply %s", name(), response.err.to_string());
            return;
        }
    } else if (is_same_ballot_status_change_allowed(status(), request.config.status)) {
        update_local_configuration(request.config, true);
    }
    if (request.__isset.confirmed_decree) {
        _duplication_mgr->update_confirmed_decree_if_secondary(request.confirmed_decree);
    }

    switch (status()) {
    case partition_status::PS_INACTIVE:
        break;
    case partition_status::PS_SECONDARY:
        if (request.last_committed_decree > last_committed_decree()) {
            _prepare_list->commit(request.last_committed_decree, COMMIT_TO_DECREE_HARD);
        }
        // TODO(heyuchen): consider
        if (request.__isset.child_gpid) { // secondary create child replica
            parent_start_split(request);
        }
        if (request.app.partition_count ==
            _app_info.partition_count * 2) { // secondary update partition count
            update_local_partition_count(request.app.partition_count);
            parent_cleanup_split_context();
        }

        // TODO(heyuchen): new add
        if(request.__isset.meta_split_status && request.meta_split_status == split_status::PAUSED){
            secondary_parent_handle_paused();
            ddebug_replica("hyc secondary pause split succeed");
            response.__set_secondary_split_status(_split_status);
        }

        // TODO(heyuchen): new add
        if(request.__isset.meta_split_status && request.meta_split_status == split_status::CANCELING){
            secondary_parent_handle_cancel();
            ddebug_replica("hyc secondary cancel split succeed");
            response.__set_secondary_split_status(_split_status);
        }

        break;
    case partition_status::PS_POTENTIAL_SECONDARY:
        init_learn(request.config.learner_signature);
        break;
    case partition_status::PS_ERROR:
        break;
    default:
        dassert(false, "invalid partition_status, status = %s", enum_to_string(status()));
    }

    response.pid = get_gpid();
    response.node = _stub->_primary_address;
    response.err = ERR_OK;
    if (status() == partition_status::PS_ERROR) {
        response.err = ERR_INVALID_STATE;
        dwarn("%s: on_group_check reply %s", name(), response.err.to_string());
    }

    response.last_committed_decree_in_app = _app->last_committed_decree();
    response.last_committed_decree_in_prepare_list = last_committed_decree();
    response.learner_status_ = _potential_secondary_states.learning_status;
    response.learner_signature = _potential_secondary_states.learning_version;
}

void replica::on_group_check_reply(error_code err,
                                   const std::shared_ptr<group_check_request> &req,
                                   const std::shared_ptr<group_check_response> &resp)
{
    _checker.only_one_thread_access();

    if (partition_status::PS_PRIMARY != status() || req->config.ballot < get_ballot()) {
        return;
    }

    auto r = _primary_states.group_check_pending_replies.erase(req->node);
    dassert(r == 1, "invalid node address, address = %s", req->node.to_string());

    if (err != ERR_OK) {
        handle_remote_failure(req->config.status, req->node, err, "group check");
    } else {
        if (resp->err == ERR_OK) {
            if (resp->learner_status_ == learner_status::LearningSucceeded &&
                req->config.status == partition_status::PS_POTENTIAL_SECONDARY) {
                handle_learning_succeeded_on_primary(req->node, resp->learner_signature);
            }

            // TODO(heyuchen): new add
            if(resp->__isset.secondary_split_status){
                _primary_states.secondary_split_status[req->node] = resp->secondary_split_status;
            }

            if(req->__isset.meta_split_status && req->meta_split_status == split_status::PAUSED){
                if(_primary_states.secondary_split_status.size() + 1 == _primary_states.membership.max_replica_count){
                    bool finish_pause = true;
                    for(const auto &kv : _primary_states.secondary_split_status){
                        finish_pause &= (kv.second == split_status::NOT_SPLIT);
                    }
                    if(finish_pause){
                        ddebug_replica("hyc: group has paused split succeed");
                        parent_cleanup_split_context();
                    }
                }
            }

            if(req->__isset.meta_split_status && req->meta_split_status == split_status::CANCELING){
                if(_primary_states.secondary_split_status.size() + 1 == _primary_states.membership.max_replica_count){
                    bool finish_cancel = true;
                    for(const auto &kv : _primary_states.secondary_split_status){
                        finish_cancel &= (kv.second == split_status::NOT_SPLIT);
                    }
                    if(finish_cancel){
                        ddebug_replica("hyc all cancel split succeed");
                        parent_cleanup_split_context();
                        parent_send_notify_cancel_request();
                    }
                }
            }

        } else {
            handle_remote_failure(req->config.status, req->node, resp->err, "group check");
        }
    }
}

void replica::inject_error(error_code err)
{
    tasking::enqueue(LPC_REPLICATION_ERROR,
                     &_tracker,
                     [this, err]() { handle_local_failure(err); },
                     get_gpid().thread_hash());
}
}
} // end namepspace
