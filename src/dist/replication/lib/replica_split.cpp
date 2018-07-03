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

#include "replica.h"
#include "replica_stub.h"
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/utility/filesystem.h>

namespace dsn {
namespace replication {

///
/// create and init child replica
///
void replica::on_add_child(const group_check_request &request) // on parent
{
    if (request.config.ballot != get_ballot()) {
        dwarn_f("receive add child replica with different ballot, local ballot is {}, "
                "request ballot is {}, ignore",
                get_ballot(),
                request.config.ballot);
        return;
    }

    gpid child_gpid = request.child_gpid;
    if (_child_gpid == child_gpid) {
        dwarn_f("child replica already exist, child gpid is ({},{}), "
                "this replica {} may be spliting, ignore",
                child_gpid.get_app_id(),
                child_gpid.get_partition_index(),
                name());
        return;
    }

    if (child_gpid.get_partition_index() < _app_info.partition_count) {
        dwarn_f("{}, receive old add child replica request, child gpid is ({},{}), "
                "local partition count is {}, ignore",
                name(),
                child_gpid.get_app_id(),
                child_gpid.get_partition_index(),
                _app_info.partition_count);
        return;
    }

    _child_gpid = child_gpid;
    _child_ballot = get_ballot();

    ddebug_f("{} process add child replica({}, {}), primary is {}, ballot is {}, "
             "status is {}, last_committed_decree is {}",
             name(),
             child_gpid.get_app_id(),
             child_gpid.get_partition_index(),
             request.config.primary.to_string(),
             request.config.ballot,
             enum_to_string(request.config.status),
             request.last_committed_decree);

    tasking::enqueue(LPC_SPLIT_PARTITION,
                     tracker(),
                     std::bind(&replica_stub::add_split_replica,
                               _stub,
                               _config.primary,
                               _app_info,
                               _child_ballot,
                               _child_gpid,
                               get_gpid(),
                               _dir),
                     get_gpid().thread_hash());
}

void replica::init_child_replica(gpid parent_gpid,
                                 rpc_address primary_address,
                                 ballot init_ballot) // on child
{
    if (status() != partition_status::PS_INACTIVE) {
        dwarn_f("{} status is not PS_INACTIVE, is {}, skip split request",
                name(),
                enum_to_string(status()));

        _stub->on_exec(
            LPC_SPLIT_PARTITION, parent_gpid, [](replica_ptr r) { r->_child_gpid.set_app_id(0); });
        return;
    }

    // update replica config
    _config.ballot = init_ballot;
    _config.primary = primary_address;
    _config.status = partition_status::PS_PARTITION_SPLIT;

    // init split states
    _split_states.parent_gpid = parent_gpid;
    _split_states.is_caught_up = false;
    _split_states.is_prepare_list_copied = false;

    // heartbeat
    _split_states.check_state_task = tasking::enqueue(LPC_SPLIT_PARTITION,
                                                      tracker(),
                                                      std::bind(&replica::check_child_state, this),
                                                      get_gpid().thread_hash(),
                                                      std::chrono::seconds(5));

    ddebug_f("{}: init child replica, start ballot is {}, parent gpid is ({}.{})",
             name(),
             init_ballot,
             parent_gpid.get_app_id(),
             parent_gpid.get_partition_index());

    std::string learn_dir = _app->learn_dir();
    _stub->on_exec(LPC_SPLIT_PARTITION,
                   _split_states.parent_gpid,
                   std::bind(&replica::prepare_copy_parent_state,
                             std::placeholders::_1,
                             learn_dir,
                             get_gpid(),
                             get_ballot()),
                   std::bind(&replica::update_local_configuration_with_no_ballot_change,
                             std::placeholders::_1,
                             partition_status::PS_ERROR),
                   get_gpid());
}

void replica::check_child_state() // on child
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_f("{} status is not PS_PARTITION_SPLIT, is {}", name(), enum_to_string(status()));
        _split_states.check_state_task = nullptr;
        return;
    }

    // parent check its state
    _stub->on_exec(
        LPC_SPLIT_PARTITION,
        _split_states.parent_gpid,
        std::bind(&replica::check_parent_state, std::placeholders::_1, get_gpid(), get_ballot()),
        std::bind(&replica::update_local_configuration_with_no_ballot_change,
                  std::placeholders::_1,
                  partition_status::PS_ERROR),
        get_gpid());

    // restart check_state_task
    _split_states.check_state_task = tasking::enqueue(LPC_SPLIT_PARTITION,
                                                      tracker(),
                                                      std::bind(&replica::check_child_state, this),
                                                      get_gpid().thread_hash(),
                                                      std::chrono::seconds(5));
}

void replica::check_parent_state(gpid child_gpid, ballot child_ballot) // on parent
{
    if (child_ballot != get_ballot() || child_gpid != _child_gpid ||
        (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY &&
         (status() != partition_status::PS_INACTIVE || !_inactive_is_transient))) {

        dwarn_f("{}({}) receive out-dated split request, child config from request is ({}.{}), "
                "ballot is {}, "
                "local child config is ({}.{}), ballot is {}",
                name(),
                enum_to_string(status()),
                child_gpid.get_app_id(),
                child_gpid.get_partition_index(),
                child_ballot,
                _child_gpid.get_app_id(),
                _child_gpid.get_partition_index(),
                get_ballot());

        _stub->on_exec(LPC_SPLIT_PARTITION,
                       child_gpid,
                       std::bind(&replica::update_local_configuration_with_no_ballot_change,
                                 std::placeholders::_1,
                                 partition_status::PS_ERROR));

        _child_gpid.set_app_id(0);
    }
}

///
/// child async learn parent states
///
void replica::prepare_copy_parent_state(const std::string &dir,
                                        gpid child_gpid,
                                        ballot child_ballot) // on parent
{
    check_parent_state(child_gpid, child_ballot);
    if (_child_gpid.get_app_id() == 0) {
        return;
    }

    // get last_committed_decree as local_committed_decree
    decree local_committed_decree = _prepare_list->last_committed_decree();

    learn_state copy_parent_state;
    int64_t checkpoint_decree;

    // generate checkpoint
    dsn::error_code ec = _app->copy_checkpoint_to_dir(dir.c_str(), &checkpoint_decree);
    if (ec == ERR_OK) {
        ddebug_f("{} gerenate checkpoint succeed, checkpoint dir path is {}, decree is {}",
                 name(),
                 dir,
                 checkpoint_decree);
        copy_parent_state.to_decree_included = checkpoint_decree;
        // TODO(hyc): consider this files must has element???
        copy_parent_state.files.push_back(dsn::utils::filesystem::path_combine(dir, "dummy"));

    } else {
        derror_f("{} gerenate checkpoint failed, error is {}", name(), ec.to_string());
        ec = ERR_GET_LEARN_STATE_FAILED;
    }

    std::vector<mutation_ptr> mutation_list;
    std::vector<std::string> files;
    prepare_list *plist;

    if (ec == ERR_OK) {
        // get mutation and private log
        _private_log->get_mutation_log_file(
            get_gpid(), checkpoint_decree + 1, invalid_ballot, mutation_list, files);

        // get prepare list
        plist = new prepare_list(*_prepare_list);
        plist->truncate(local_committed_decree);
    }

    dassert(local_committed_decree == checkpoint_decree || !mutation_list.empty() || !files.empty(),
            "");

    dwarn_f("dir is {}", copy_parent_state.files[0]);

    // TODO(hyc): consider missing_handler
    _stub->on_exec(LPC_SPLIT_PARTITION,
                   _child_gpid,
                   std::bind(&replica::copy_parent_state,
                             std::placeholders::_1,
                             ec,
                             copy_parent_state,
                             mutation_list,
                             files,
                             plist));
}

void replica::copy_parent_state(error_code ec,
                                learn_state lstate,
                                std::vector<mutation_ptr> mutation_list,
                                std::vector<std::string> files,
                                prepare_list *plist) // on child
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_f("{} is copying parent state during partition split, but status is {}",
                name(),
                enum_to_string(status()));
        return;
    }

    if (ec != ERR_OK) {
        dwarn_f("{} failed to copy parent state, error code is {}, retry", name(), ec.to_string());
        _stub->on_exec(LPC_SPLIT_PARTITION,
                       _split_states.parent_gpid,
                       std::bind(&replica::prepare_copy_parent_state,
                                 std::placeholders::_1,
                                 _app->learn_dir(),
                                 get_gpid(),
                                 get_ballot()),
                       std::bind(&replica::update_local_configuration_with_no_ballot_change,
                                 std::placeholders::_1,
                                 partition_status::PS_ERROR),
                       get_gpid(),
                       std::chrono::seconds(1));
        return;
    }

    // copy prepare list
    decree last_committed_decree = plist->last_committed_decree();
    ddebug_f("{} is copying parent prepare list, last_committed_decree is {}, min decree is {}, "
             "max decree is {}",
             name(),
             last_committed_decree,
             plist->min_decree(),
             plist->max_decree());

    delete _prepare_list;
    plist->set_committer(std::bind(&replica::execute_mutation, this, std::placeholders::_1));
    _prepare_list = plist;
    _split_states.is_prepare_list_copied = true;

    for (decree d = last_committed_decree + 1; d <= _prepare_list->max_decree(); ++d) {
        auto mu = _prepare_list->get_mutation_by_decree(d);
        dassert(mu != nullptr, "");

        mu->data.header.pid = get_gpid();
        _stub->_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, tracker(), nullptr);
        _private_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, tracker(), nullptr);

        // set mutation has been logged in private log
        // TODO(hyc):consider it! shared log callback should set the mutation as logged?
        if (!mu->is_logged()) {
            mu->set_logged();
        }
    }

    // start async learn task
    _split_states.async_learn_task = tasking::enqueue(LPC_SPLIT_PARTITION_ASYNC_LEARN,
                                                      tracker(),
                                                      std::bind(&replica::apply_parent_state,
                                                                this,
                                                                ec,
                                                                lstate,
                                                                mutation_list,
                                                                files,
                                                                last_committed_decree),
                                                      get_gpid().thread_hash());
}

void replica::apply_parent_state(error_code ec,
                                 learn_state lstate,
                                 std::vector<mutation_ptr> mutation_list,
                                 std::vector<std::string> files,
                                 decree last_committed_decree) // on child
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_f("{} is applying parent state during partition split, but state is {}",
                name(),
                enum_to_string(status()));
        return;
    }

    ddebug_f("{} is learn data and mutations async, last_committed_decree is {}, private_log file "
             "count is {}, mutation in memory count is {}",
             name(),
             last_committed_decree,
             files.size(),
             mutation_list.size());

    bool is_error = false;

    // apply checkpoint
    dsn::error_code error =
        _app->apply_checkpoint(replication_app_base::chkpt_apply_mode::copy, lstate);
    if (error != ERR_OK) {
        is_error = true;
        derror_f("{} execute apply_parent_state, failed to apply checkpoint, error is {}",
                 name(),
                 error.to_string());
    }

    // apply private log and mutations in memory
    if (!is_error) {
        error = async_learn_mutation_private_log(mutation_list, files, last_committed_decree);
        if (error != ERR_OK) {
            is_error = true;
            derror_f("{} execute apply_parent_state, failed to apply private log and mutations in "
                     "memory, error is {}",
                     name(),
                     error.to_string());
        }
    }

    // TODO(hyc): consider why???
    // generate a checkpoint sync
    if (!is_error) {
        error = _app->sync_checkpoint();
        if (error != ERR_OK) {
            is_error = true;
            derror_f("{} execute apply_parent_state, failed to sync checkpoint, error is {}",
                     name(),
                     error.to_string());
        }
    }

    _split_states.async_learn_task = nullptr;
    if (error != ERR_OK) {
        update_local_configuration_with_no_ballot_change(partition_status::PS_ERROR);
        _stub->on_exec(LPC_SPLIT_PARTITION, _split_states.parent_gpid, [](replica *r) {
            r->_child_gpid.set_app_id(0);
        });
    } else {
        error = _app->update_init_info_ballot_and_decree(this);
        if (error == ERR_OK) {
            ddebug_f("{} update_init_info_ballot_and_decree, current ballot is {}",
                     name(),
                     get_ballot());
        } else {
            dwarn_f("{} update_init_info_ballot_and decree failed, error is {}",
                    name(),
                    error.to_string());
        }
        tasking::enqueue(LPC_SPLIT_PARTITION,
                         tracker(),
                         std::bind(&replica::child_catch_up, this),
                         get_gpid().thread_hash());
    }
}

error_code replica::async_learn_mutation_private_log(std::vector<mutation_ptr> mutation_list,
                                                     std::vector<std::string> files,
                                                     decree last_committed_decree) // on child
{
    error_code ec;
    int64_t offset;

    prepare_list plist(_app->last_committed_decree(),
                       _options->max_mutation_count_in_prepare_list,
                       [this, ec](mutation_ptr mu) {
                           if (mu->data.header.decree == _app->last_committed_decree() + 1) {
                               _app->apply_mutation(mu);
                           }
                       });

    // replay private log
    ec = mutation_log::replay(files,
                              [this, &plist](int log_length, mutation_ptr &mu) {
                                  decree d = mu->data.header.decree;
                                  if (d <= plist.last_committed_decree()) {
                                      return false;
                                  }

                                  mutation_ptr origin_mu = plist.get_mutation_by_decree(d);
                                  if (origin_mu != nullptr &&
                                      origin_mu->data.header.ballot >= mu->data.header.ballot) {
                                      return false;
                                  }

                                  plist.prepare(mu, partition_status::PS_SECONDARY);
                                  return true;

                              },
                              offset);

    ddebug_f("{} apply private_log files, file count is {}, app last_committed_decree is {}",
             name(),
             files.size(),
             _app->last_committed_decree());

    // apply mutations in memory
    if (ec == ERR_OK) {
        int count = 0;
        for (mutation_ptr &mu : mutation_list) {
            decree d = mu->data.header.decree;
            if (d <= plist.last_committed_decree()) {
                continue;
            }

            mutation_ptr origin_mu = plist.get_mutation_by_decree(d);
            if (origin_mu != nullptr && origin_mu->data.header.ballot >= mu->data.header.ballot) {
                continue;
            }

            if (!mu->is_logged()) {
                mu->set_logged();
            }

            plist.prepare(mu, partition_status::PS_SECONDARY);
            ++count;
        }

        ddebug("{} apply mutations in memory, count is {}, app last_committed_decree is {}",
               name(),
               count,
               _app->last_committed_decree());
    }

    plist.commit(last_committed_decree, COMMIT_TO_DECREE_HARD);

    return ec;
}

void replica::child_catch_up() // on child
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        derror_f("{} is catching up parent state during partition split, but status is {}",
                 name(),
                 status());
        return;
    }

    decree goal_decree = _prepare_list->last_committed_decree();
    decree local_decree = _app->last_committed_decree();
    // there are still some mutations child not learn
    if (local_decree < goal_decree) {
        if (local_decree >=
            _prepare_list->min_decree()) { // missing mutations are all in prepare_list
            dwarn_f("{} still has mutations in memory to learn, app last_committed_decree is {}, "
                    "prepare_list min_decree is {}",
                    name(),
                    local_decree,
                    _prepare_list->min_decree());
            for (decree d = local_decree + 1; d <= goal_decree; ++d) {
                auto mu = _prepare_list->get_mutation_by_decree(d);
                dassert(mu != nullptr, "");
                error_code ec = _app->apply_mutation(mu);
                if (ec != ERR_OK) {
                    update_local_configuration_with_no_ballot_change(partition_status::PS_ERROR);
                    return;
                }
            }
        } else { // some missing mutations are not in memory
            dwarn_f("{} still has mutations in private log to learn,  app last_committed_decree is "
                    "{}, prepare_list min_decree is {}, please wait",
                    name(),
                    local_decree,
                    _prepare_list->min_decree());
            _split_states.async_learn_task = tasking::enqueue(
                LPC_CATCHUP_WITH_PRIVATE_LOGS,
                tracker(),
                [this]() {
                    this->catch_up_with_private_logs(partition_status::PS_PARTITION_SPLIT);
                    _split_states.async_learn_task = nullptr;
                },
                get_gpid().thread_hash(),
                std::chrono::seconds(1));
            return;
        }
    }

    ddebug_f("{} catch up, will send notification to parent({}.{})",
             name(),
             _split_states.parent_gpid.get_app_id(),
             _split_states.parent_gpid.get_partition_index());

    _split_states.is_caught_up = true;

    notify_primary_split_catch_up();
}

///
/// prepare to register child replicas
///

void replica::notify_primary_split_catch_up() // on child
{
    notify_catch_up_request request;
    request.child_gpid = get_gpid();
    request.child_ballot = get_ballot();
    request.child_address = _stub->_primary_address;
    request.primary_parent_gpid = _config.pid;

    auto on_notify_primary_split_catch_up_reply = [this](error_code ec,
                                                         notify_cacth_up_response response) {
        if (ec == ERR_TIMEOUT) {
            dwarn_f("{} failed to notify catch up, because timeout, please wait and retry");
            tasking::enqueue(LPC_SPLIT_PARTITION,
                             tracker(),
                             std::bind(&replica::notify_primary_split_catch_up, this),
                             get_gpid().thread_hash(),
                             std::chrono::seconds(1));
        } else if (ec != ERR_OK || response.err != ERR_OK) {
            error_code err = (ec == ERR_OK) ? response.err : ec;
            derror_f("{} failed to notify primary catch up, error is {}", name(), err.to_string());

            update_local_configuration_with_no_ballot_change(partition_status::PS_ERROR);
            _stub->on_exec(LPC_SPLIT_PARTITION, _split_states.parent_gpid, [](replica_ptr r) {
                r->_child_gpid.set_app_id(0);
            });
        } else {
            ddebug_f("{} succeed to notify primary catch up", name());
        }
    };

    ddebug_f("{} send notification to primary: {}.{}@{}, ballot is {}",
             name(),
             _config.pid.get_app_id(),
             _config.pid.get_partition_index(),
             _config.primary.to_string(),
             get_ballot());

    rpc::call(_config.primary,
              RPC_SPLIT_NOTIFY_CATCH_UP,
              request,
              tracker(),
              on_notify_primary_split_catch_up_reply,
              std::chrono::seconds(0),
              get_gpid().thread_hash());
}

void replica::on_notify_primary_split_catch_up(
    notify_catch_up_request request, notify_cacth_up_response &response) // on primary parent
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_f("{} is not primary, status is {}", name(), enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.child_ballot < get_ballot()) {
        derror_f("{} receive out-date request, request ballot is {}, local ballot is {}",
                 name(),
                 request.child_ballot,
                 get_ballot());
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.child_gpid != _child_gpid) {
        derror_f("{} receive wrong child request, request child_gpid is {}.{}, local child_gpid is "
                 "{}.{}",
                 name(),
                 request.child_gpid.get_app_id(),
                 request.child_gpid.get_partition_index(),
                 _child_gpid.get_app_id(),
                 _child_gpid.get_partition_index());
        response.err = ERR_INVALID_STATE;
        return;
    }

    response.err = ERR_OK;

    // cache child_address
    _primary_states.child_address.insert(request.child_address);
    for (auto &iter : _primary_states.statuses) {
        if (_primary_states.child_address.find(iter.first) ==
            _primary_states.child_address.end()) { // not all child catch up
            dinfo_f("there are still child(address is {}) not catch up, wait",
                    iter.first.to_string());
            return;
        }
    }

    ddebug_f("{} all child catch up", name());
    _primary_states.child_address.clear();
    _primary_states.is_sync_to_child = true;
    decree sync_point = _prepare_list->max_decree() + 1;

    // TODO(hyc): ???
    //    if (!_options->empty_write_disabled) {
    //        mutation_ptr mu = new_mutation(invalid_decree);
    //        mu->add_client_request(RPC_REPLICATION_WRITE_EMPTY, nullptr);
    //        init_prepare(mu);
    //        dassert(sync_point == mu->data.header.decree,
    //                "%" PRId64 " vs %" PRId64,
    //                sync_point,
    //                mu->data.header.decree);
    //    };

    tasking::enqueue(LPC_SPLIT_PARTITION,
                     tracker(),
                     std::bind(&replica::check_sync_point, this, sync_point),
                     get_gpid().thread_hash());
}

void replica::check_sync_point(decree sync_point) // on primary parent
{
    ddebug_f("{} check sync point, sync_point is {}, local last_committed_decree is {}",
             name(),
             sync_point,
             _app->last_committed_decree());

    // if valid -> update_group_partition_count, otherwise retry
    if (_app->last_committed_decree() >= sync_point) {
        update_group_partition_count(_app_info.partition_count * 2);
    } else {
        dwarn_f(
            "{} local last_committed_decree is not caught up sync_point, please wait and retry");
        tasking::enqueue(LPC_SPLIT_PARTITION,
                         tracker(),
                         std::bind(&replica::check_sync_point, this, sync_point),
                         get_gpid().thread_hash(),
                         std::chrono::seconds(1));
    }
}

void replica::update_group_partition_count(int new_partition_count) // on primary parent
{
    if (_child_gpid.get_app_id() == 0 || _child_ballot < get_ballot()) {
        dwarn_f("{} receive out-date request, _child_gpid({}.{}), _child_ballot is {}, local "
                "ballot is {}",
                name(),
                _child_gpid.get_app_id(),
                _child_gpid.get_partition_index(),
                _child_ballot,
                get_ballot());
        _stub->on_exec(LPC_SPLIT_PARTITION,
                       _child_gpid,
                       std::bind(&replica::update_local_configuration_with_no_ballot_change,
                                 std::placeholders::_1,
                                 partition_status::PS_ERROR));
        _child_gpid.set_app_id(0);
        return;
    }

    // cache all node addresses
    std::shared_ptr<std::set<dsn::rpc_address>> group_address =
        std::make_shared<std::set<dsn::rpc_address>>();
    for (auto iter = _primary_states.statuses.begin(); iter != _primary_states.statuses.end();
         ++iter) {
        group_address->insert(iter->first);
    }

    ddebug_f("{} will send update_group_partition_count_request, new partition count is {}",
             name(),
             new_partition_count);

    for (auto &iter : _primary_states.statuses) {
        std::shared_ptr<update_group_partition_count_request> request(
            new update_group_partition_count_request);
        request->app = _app_info;
        request->app.partition_count = new_partition_count;
        request->target_address = iter.first;
        _primary_states.get_replica_config(iter.second, request->config);
        request->last_committed_decree = last_committed_decree();

        gpid pid = request->config.pid;
        ddebug_f("{} send update_group_partition_count_request to replica {}.{}@{}",
                 name(),
                 pid.get_app_id(),
                 pid.get_partition_index(),
                 iter.first.to_string());

        rpc::call(
            iter.first,
            RPC_SPLIT_UPDATE_PARTITION_COUNT,
            *request,
            tracker(), // TODO(hyc): consider, pid tracker???
            [=](error_code ec, update_group_partition_count_response &&response) {
                on_update_group_partition_count_reply(
                    ec,
                    request,
                    std::make_shared<update_group_partition_count_response>(std::move(response)),
                    group_address,
                    iter.first);
            },
            std::chrono::seconds(0),
            pid.thread_hash());
    }
}

void replica::on_update_group_partition_count(
    update_group_partition_count_request request,
    update_group_partition_count_response &response) // on all replicas
{

    if (request.config.ballot < get_ballot()) {
        dwarn_f(
            "{} receive out-dated group_check_request, request ballot is {}, local ballot is {}",
            name(),
            request.config.ballot,
            get_ballot());

        update_local_configuration_with_no_ballot_change(partition_status::PS_ERROR);
        _stub->on_exec(LPC_SPLIT_PARTITION, _split_states.parent_gpid, [](replica_ptr r) {
            r->_child_gpid.set_app_id(0);
        });

        response.err = ERR_VERSION_OUTDATED;
        return;
    }

    if (_split_states.parent_gpid.get_app_id() == 0 && _split_states.is_caught_up == false) {
        dwarn_f("{} receive out-dated group_check_request, child is not caught up", name());
        response.err = ERR_VERSION_OUTDATED;
        return;
    }

    ddebug_f("{} execute update group partition count, new partition count is {}",
             name(),
             request.app.partition_count);

    // save app_info on disk
    auto info = _app_info;
    info.partition_count = request.app.partition_count;

    replica_app_info new_info((app_info *)&info);
    std::string info_path = utils::filesystem::path_combine(_dir, ".app_info");
    error_code err = new_info.store(info_path.c_str());

    if (err != ERR_OK) {
        derror("{} failed to save app_info to {}, error is {}",
               name(),
               info_path.c_str(),
               err.to_string());
    } else {
        // save app_info in memory
        _app_info = info;
        _partition_version = _app_info.partition_count - 1;
        _app->set_partition_version(_partition_version);

        if (_child_gpid.get_partition_index() < _app_info.partition_count) {
            _child_gpid.set_app_id(0);
        }
    }

    response.err = err;
}

void replica::on_update_group_partition_count_reply(
    error_code ec,
    std::shared_ptr<update_group_partition_count_request> request,
    std::shared_ptr<update_group_partition_count_response> response,
    std::shared_ptr<std::set<dsn::rpc_address>> left_replicas,
    rpc_address finish_update_address) // on primary parent
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_f(
            "{} failed to exectue on_update_group_partition_count_reply, it is not primary but {}",
            name(),
            enum_to_string(status()));
        return;
    }

    if (request->config.ballot != get_ballot()) {
        dwarn_f("{} failed to exectue on_update_group_partition_count_reply, ballot not match, "
                "request ballot is {}, local ballot is {}",
                name(),
                request->config.ballot,
                get_ballot());
        return;
    }

    if (ec == ERR_OK && response->err == ERR_OK) {
        left_replicas->erase(finish_update_address);
        if (left_replicas->empty()) { // all child reply

            ddebug_f("{} finish update all replicas partition count", name());
            register_child_on_meta(get_ballot());

        } else { // not all child reply

            ddebug_f("{}, there are still {} child not update partition count",
                     name(),
                     left_replicas->size());
        }
    } else { // retry
        error_code error = (ec == ERR_OK) ? response->err : ec;
        dwarn_f("{} failed to execute on_update_group_partition_count_reply, error is {}, retry",
                name(),
                error.to_string());
        tasking::enqueue(
            LPC_SPLIT_PARTITION,
            tracker(),
            [this, request, finish_update_address, left_replicas]() {
                request->config.ballot = get_ballot();
                rpc::call(finish_update_address,
                          RPC_SPLIT_UPDATE_PARTITION_COUNT,
                          *request,
                          tracker(),
                          [=](error_code err, update_group_partition_count_response &&response) {
                              on_update_group_partition_count_reply(
                                  err,
                                  request,
                                  std::make_shared<update_group_partition_count_response>(
                                      std::move(response)),
                                  left_replicas,
                                  finish_update_address);
                          },
                          std::chrono::seconds(1),
                          get_gpid().thread_hash());
            },
            get_gpid().thread_hash(),
            std::chrono::seconds(1));
    }
}

void replica::register_child_on_meta(ballot b) {}

} // namespace replication
} // namespace dsn
