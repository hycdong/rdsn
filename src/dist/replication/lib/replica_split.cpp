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
    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY &&
        (status() != partition_status::PS_INACTIVE || !_inactive_is_transient)) {
        dwarn_f("receive add child replica with wrong status, current status is {}, ignore",
                enum_to_string(status()));
        return;
    }

    if (request.config.ballot != get_ballot()) {
        dwarn_f("receive add child replica with different ballot, local ballot is {}, "
                "request ballot is {}, ignore",
                get_ballot(),
                request.config.ballot);
        return;
    }

    gpid child_gpid = request.child_gpid;
    if (_child_gpid == child_gpid) {
        dwarn_f("child replica already exist, child gpid is ({}.{}), "
                "this replica {} may be splitting, ignore",
                child_gpid.get_app_id(),
                child_gpid.get_partition_index(),
                name());
        return;
    }

    if (child_gpid.get_partition_index() < _app_info.partition_count) {
        dwarn_f("{}, receive old add child replica request, child gpid is ({}.{}), "
                "local partition count is {}, ignore",
                name(),
                child_gpid.get_app_id(),
                child_gpid.get_partition_index(),
                _app_info.partition_count);
        return;
    }

    _child_gpid = child_gpid;
    _child_ballot = get_ballot();

    ddebug_f("{} process add child replica({}.{}), primary is {}, ballot is {}, "
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
        dwarn_f("{} status is not PS_INACTIVE, but {}, ignore this request",
                name(),
                enum_to_string(status()));

        _stub->on_exec(
            LPC_SPLIT_PARTITION_ERROR, parent_gpid, [](replica_ptr r) { r->_child_gpid.set_app_id(0); });
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
    _split_states.splitting_start_ts_ns = dsn_now_ns();
    //TODO(hyc): pref-counter - delete
    ddebug_f("{}: splitting_start_ts_ns is {}", name(), _split_states.splitting_start_ts_ns);

    _stub->_counter_replicas_splitting_recent_start_count->increment();
    //TODO(hyc): delete
    ddebug_f("stub recent start split count is {}", _stub->_counter_replicas_splitting_recent_start_count.get()->get_value());

    // heartbeat
    _split_states.check_state_task = tasking::enqueue(LPC_SPLIT_PARTITION,
                                                      tracker(),
                                                      std::bind(&replica::check_child_state, this),
                                                      get_gpid().thread_hash(),
                                                      std::chrono::seconds(5));

    ddebug_f("{}: start ballot is {}, parent gpid is ({}.{})",
             name(),
             init_ballot,
             parent_gpid.get_app_id(),
             parent_gpid.get_partition_index());

    _stub->on_exec(LPC_SPLIT_PARTITION,
                   _split_states.parent_gpid,
                   std::bind(&replica::prepare_copy_parent_state,
                             std::placeholders::_1,
                             _app->learn_dir(),
                             get_gpid(),
                             get_ballot()),
                   std::bind(&replica::handle_splitting_error,
                             std::placeholders::_1,
                             "init_child_replica coz invalid parent gpid"),
                   get_gpid());
}

void replica::check_child_state() // on child
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_f("{} status is not PS_PARTITION_SPLIT during check_child_state, but {}", name(), enum_to_string(status()));
        _split_states.check_state_task = nullptr;
        return;
    }

    // parent check its state
    _stub->on_exec(
        LPC_SPLIT_PARTITION,
        _split_states.parent_gpid,
        std::bind(&replica::check_parent_state, std::placeholders::_1, get_gpid(), get_ballot()),
        std::bind(&replica::handle_splitting_error,
                  std::placeholders::_1,
                  "check_child_state coz invalid parent gpid"),
        get_gpid());

    // restart check_state_task
    // TODO(hyc): consider heartbeat interval
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
                       std::bind(&replica::handle_splitting_error,
                                 std::placeholders::_1,
                                 "check_parent_state coz wrong parent state"));

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

    dsn::error_code ec = ERR_OK;
    learn_state copy_parent_state;
    int64_t checkpoint_decree;

    {
        // generate checkpoint
        ec = _app->copy_checkpoint_to_dir(dir.c_str(), &checkpoint_decree);
        if (ec == ERR_OK) {
            ddebug_f("{} prepare state succeed: copy checkpoint to dir {}, checkpoint decree is {}",
                     name(),
                     dir,
                     checkpoint_decree);
            copy_parent_state.to_decree_included = checkpoint_decree;
            copy_parent_state.files.push_back(dsn::utils::filesystem::path_combine(dir, "dummy"));

        } else {
            derror_f("{} prepare state failed: copy checkpoint failed, error is {}", name(), ec.to_string());
            ec = ERR_GET_LEARN_STATE_FAILED;
        }
    }

    std::vector<mutation_ptr> mutation_list;
    std::vector<std::string> files;
    prepare_list *plist = nullptr;
    uint64_t total_file_size = 0;

    if (ec == ERR_OK) {
        // get mutation and private log
        _private_log->get_mutation_log_file(
            get_gpid(), checkpoint_decree + 1, invalid_ballot, mutation_list, files, total_file_size);

        // get prepare list
        plist = new prepare_list(this, *_prepare_list);
        plist->truncate(last_committed_decree());

        ddebug_f("{} prepare state succeed: {} mutations, {} private log files, total file size is {}, last_committed_decree is {}",
                 name(),
                 mutation_list.size(),
                 files.size(),
                 total_file_size,
                 last_committed_decree());
        dassert(last_committed_decree() == checkpoint_decree || !mutation_list.empty() || !files.empty(),
                "");
    }

    // TODO(hyc): add missing_handler, parent replica delete plist object
    _stub->on_exec(LPC_SPLIT_PARTITION,
                   _child_gpid,
                   std::bind(&replica::copy_parent_state,
                             std::placeholders::_1,
                             ec,
                             copy_parent_state,
                             mutation_list,
                             files,
                             total_file_size,
                             plist),
                   [plist](replica* r){ delete plist;},
                   get_gpid());
}

void replica::copy_parent_state(error_code ec,
                                learn_state lstate,
                                std::vector<mutation_ptr> mutation_list,
                                std::vector<std::string> files,
                                uint64_t total_file_size,
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
                       std::bind(&replica::handle_splitting_error,
                                 std::placeholders::_1,
                                 "check_parent_state coz invalid parent gpid"),
                       get_gpid(),
                       std::chrono::seconds(1));
        return;
    }

    // copy prepare list
    decree last_committed_decree = plist->last_committed_decree();
    // start async learn task
    _split_states.splitting_start_async_learn_ts_ns = dsn_now_ns();
    _split_states.async_learn_task = tasking::enqueue(LPC_SPLIT_PARTITION_ASYNC_LEARN,
                                                      tracker(),
                                                      std::bind(&replica::apply_parent_state,
                                                                this,
                                                                ec,
                                                                lstate,
                                                                mutation_list,
                                                                files,
                                                                total_file_size,
                                                                last_committed_decree),
                                                      get_gpid().thread_hash());

    ddebug_f("{} copy parent prepare list, last_committed_decree is {}, min decree is {}, "
             "max decree is {}",
             name(),
             last_committed_decree,
             plist->min_decree(),
             plist->max_decree());

    delete _prepare_list;
    plist->set_committer(std::bind(&replica::execute_mutation, this, std::placeholders::_1));
    _prepare_list = new prepare_list(this, *plist);
//    _split_states.is_prepare_list_copied = true;

    for (decree d = last_committed_decree + 1; d <= _prepare_list->max_decree(); ++d) {
        auto mu = _prepare_list->get_mutation_by_decree(d);
        dassert(mu != nullptr, "");

        mu->data.header.pid = get_gpid();
        _stub->_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, tracker(), nullptr);
        _private_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, tracker(), nullptr);

        // set mutation has been logged in private log
        if (!mu->is_logged()) {
            mu->set_logged();
        }
    }

    _split_states.is_prepare_list_copied = true;

    //TODO(hyc): 0115 - test fix init
    // add cached mutations to prepare list
    for (mutation_ptr &mu : _split_states.child_temp_mutation_list) {
        ddebug_f("{} will copy mutation {} cached when prepare list is not copied", name(), mu->name());
        _stub->on_exec(LPC_SPLIT_PARTITION,
                       get_gpid(),
                       std::bind(&replica::on_copy_mutation,
                                 std::placeholders::_1,
                                 mu));
    }
}

void replica::apply_parent_state(error_code ec,
                                 learn_state lstate,
                                 std::vector<mutation_ptr> mutation_list,
                                 std::vector<std::string> files,
                                 uint64_t total_file_size,
                                 decree last_committed_decree) // on child
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_f("{} is applying parent state during partition split, but state is {}",
                name(),
                enum_to_string(status()));
        return;
    }

    ddebug_f("{} start to learn data and mutations asynchronously, last_committed_decree is {}, private_log file "
             "count is {}, mutation in memory count is {}",
             name(),
             last_committed_decree,
             files.size(),
             mutation_list.size());

    bool is_error = false;

    // apply checkpoint
    dsn::error_code error =
        _app->apply_checkpoint(replication_app_base::chkpt_apply_mode::learn, lstate);
    if (error != ERR_OK) {
        is_error = true;
        derror_f("{} failed to apply checkpoint, error is {}",
                 name(),
                 error.to_string());
    }

    // apply private log and mutations in memory
    if (!is_error) {
        error = async_learn_mutation_private_log(mutation_list, files, total_file_size, last_committed_decree);
        if (error != ERR_OK) {
            is_error = true;
            derror_f("{} failed to apply private log and mutations in memory, error is {}",
                     name(),
                     error.to_string());
        }
    }

    // generate a checkpoint sync
    if (!is_error) {
        error = _app->sync_checkpoint();
//        error = background_sync_checkpoint();
        if (error != ERR_OK) {
            is_error = true;
            derror_f("{} failed to sync checkpoint, error is {}",
                     name(),
                     error.to_string());
        }
    }

    _split_states.async_learn_task = nullptr;
    if (error != ERR_OK) {
        _stub->on_exec(LPC_SPLIT_PARTITION_ERROR, _split_states.parent_gpid, [](replica *r) {
            r->_child_gpid.set_app_id(0);
        });
        handle_splitting_error("apply_parent_state coz sync checkpoint failed");
    } else {
        error = _app->update_init_info_ballot_and_decree(this);
        if (error == ERR_OK) {
            ddebug_f("{} update_init_info_ballot_and_decree succeed, current ballot is {}",
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
                                                     uint64_t total_file_size,
                                                     decree last_committed_decree) // on child
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_f("{} status is not PS_PARTITION_SPLIT during while async learn mutations, but {}", name(), enum_to_string(status()));
        return ERR_INACTIVE_STATE;
    }

    error_code ec;
    int64_t offset;

    prepare_list plist(this,
                       _app->last_committed_decree(),
                       _options->max_mutation_count_in_prepare_list,
                       [this, &ec](mutation_ptr &mu) {
                           //TODO(hyc): for debug - delete
                           dinfo_f("{}, _app last_committed_decree is {}", this->name(), _app->last_committed_decree());
                           if (mu->data.header.decree == _app->last_committed_decree() + 1) {
                               //TODO(hyc): for debug - delete
                               dinfo_f("{} will apply mu {}, _app last_committed_decree is {}", this->name(), mu->name(), _app->last_committed_decree());
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
                                      dwarn_f("mu {} existed, ignore replay outdated mutation, origin ballot {} VS mu ballot {}",
                                              mu->name(), origin_mu->data.header.ballot, mu->data.header.ballot);
                                      return false;
                                  }

                                  //TODO(hyc): consider - learn
                                  if (origin_mu == nullptr && mu->data.header.ballot > this->get_ballot()) {
                                      dwarn_f("ballot changed while replay mu {}, replica ballot {} VS mu ballot {}",
                                              mu->name(), this->get_ballot(), mu->data.header.ballot);
                                      return false;
                                  }


                                  //TODO(hyc): for debug - delete
                                  dinfo_f("{} will replay mu {}, plist last_committed_decree is {}",
                                           this->name(), mu->name(), plist.last_committed_decree());
                                  plist.prepare(mu, partition_status::PS_SECONDARY);
                                  return true;

                              },
                              offset);

    _split_states.splitting_copy_file_count += files.size();
    _split_states.splitting_copy_file_size += total_file_size;
    _stub->_counter_replicas_splitting_recent_copy_file_count->add(files.size());
    _stub->_counter_replicas_splitting_recent_copy_file_size->add(total_file_size);
    ddebug_f("{} finish replay private_log files, file count is {}, app last_committed_decree is {}",
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

            dinfo_f("{} will prepare mutation {}, current count is {}", name(), mu->name(), count);

            plist.prepare(mu, partition_status::PS_SECONDARY);
            ++count;
        }

        _split_states.splitting_copy_mutation_count += count;
        _stub->_counter_replicas_splitting_recent_copy_mutation_count->add(count);
        ddebug_f("{} apply mutations in memory, count is {}, app last_committed_decree is {}",
                 name(),
                 count,
                 _app->last_committed_decree());

        //TODO(hyc): move if replay log succeed
        plist.commit(last_committed_decree, COMMIT_TO_DECREE_HARD);
        ddebug_f("{} commit to decree {}", name(), last_committed_decree);
    }  

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
                    "goal decree is {}, prepare_list min_decree is {}",
                    name(),
                    local_decree,
                    goal_decree,
                    _prepare_list->min_decree());
            for (decree d = local_decree + 1; d <= goal_decree; ++d) {
                auto mu = _prepare_list->get_mutation_by_decree(d);
                dassert(mu != nullptr, "");
                error_code ec = _app->apply_mutation(mu);
                if (ec != ERR_OK) {
                    handle_splitting_error("child_catchup coz app apply mutation failed");
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
                get_gpid().thread_hash());
            return;
        }
    }

    //TODO(hyc): delete max decree
    ddebug_f("{} catch up, will send notification to parent({}.{}), goal decree is {}, local decree is {}, max decree is {}",
             name(),
             _split_states.parent_gpid.get_app_id(),
             _split_states.parent_gpid.get_partition_index(),
             _prepare_list->last_committed_decree(),
             _app->last_committed_decree(),
             _prepare_list->max_decree());

    _split_states.is_caught_up = true;

    notify_primary_split_catch_up();
}

///
/// prepare to register child replicas
///

void replica::notify_primary_split_catch_up() // on child
{
    std::shared_ptr<notify_catch_up_request> request(new notify_catch_up_request);
    request->child_gpid = get_gpid();
    request->child_ballot = get_ballot();
    request->child_address = _stub->_primary_address;
    request->primary_parent_gpid = _split_states.parent_gpid;

    auto on_notify_primary_split_catch_up_reply = [this](error_code ec,
                                                         std::shared_ptr<notify_cacth_up_response> response) {
        if (ec == ERR_TIMEOUT) {
            dwarn_f("{} failed to notify primary catch up coz timeout, please wait and retry", this->name());
            tasking::enqueue(LPC_SPLIT_PARTITION,
                             tracker(),
                             std::bind(&replica::notify_primary_split_catch_up, this),
                             get_gpid().thread_hash(),
                             std::chrono::seconds(1));
        } else if (ec != ERR_OK || response->err != ERR_OK) {
            error_code err = (ec == ERR_OK) ? response->err : ec;
            derror_f("{} failed to notify primary catch up, error is {}", name(), err.to_string());
            _stub->on_exec(LPC_SPLIT_PARTITION_ERROR, _split_states.parent_gpid, [](replica_ptr r) {
                r->_child_gpid.set_app_id(0);
            });
            handle_splitting_error("notify_primary_split_catch_up");
        } else {
            ddebug_f("{} succeed to notify primary catch up", name());
        }
    };

    ddebug_f("{} send notification to primary: {}.{}@{}, ballot is {}",
             name(),
             _split_states.parent_gpid.get_app_id(),
             _split_states.parent_gpid.get_partition_index(),
             _config.primary.to_string(),
             get_ballot());

    rpc::call(_config.primary,
              RPC_SPLIT_NOTIFY_CATCH_UP,
              *request,
              tracker(),
              [=](error_code ec, notify_cacth_up_response &&response) {
                  on_notify_primary_split_catch_up_reply(
                      ec,
                      std::make_shared<notify_cacth_up_response>(std::move(response)));
              },
              std::chrono::seconds(0),
              _split_states.parent_gpid.thread_hash());
}

void replica::on_notify_primary_split_catch_up(
    notify_catch_up_request request, notify_cacth_up_response &response) // on primary parent
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_f("{} is not primary, status is {}", name(), enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.child_ballot != get_ballot()) {
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
    ddebug_f("{}: on_notify_primary_split_catch_up. local status:{}, local ballot:{}"
               ", request ballot:{} , from {}",
               name(),
               enum_to_string(status()),
               get_ballot(),
               request.child_ballot,
               request.child_address.to_string());

    // cache child_address
    _primary_states.child_address.insert(request.child_address);
    for (auto &iter : _primary_states.statuses) {
        if (_primary_states.child_address.find(iter.first) ==
            _primary_states.child_address.end()) { // not all child catch up
            ddebug_f("{} there are still child(address is {}) not catch up, wait",
                     name(),
                     iter.first.to_string());
            return;
        }
    }

    ddebug_f("{} all child catch up", name());

    _primary_states.child_address.clear();
    _primary_states.is_sync_to_child = true;

    decree sync_point = _prepare_list->max_decree() + 1;
    //TODO(hyc): for debug
    ddebug_f("{}: sync_point is {}", name(), sync_point);

    if (!_options->empty_write_disabled) {
        mutation_ptr mu = new_mutation(invalid_decree);
        mu->add_client_request(RPC_REPLICATION_WRITE_EMPTY, nullptr);
        init_prepare(mu, false);
        dassert(sync_point == mu->data.header.decree,
                "%" PRId64 " vs %" PRId64,
                sync_point,
                mu->data.header.decree);
    };

    tasking::enqueue(LPC_SPLIT_PARTITION,
                     tracker(),
                     std::bind(&replica::check_sync_point, this, sync_point),
                     get_gpid().thread_hash(),
                     std::chrono::seconds(1));
}

void replica::check_sync_point(decree sync_point) // on primary parent
{
    ddebug_f("{} check sync point, sync_point is {}, local last_committed_decree is {}",
             name(),
             sync_point,
             _app->last_committed_decree());

    // if valid -> update_group_partition_count, otherwise retry
    if (_app->last_committed_decree() >= sync_point) {
        update_group_partition_count(_app_info.partition_count * 2, true);
    } else {
        dwarn_f("{} local last_committed_decree is not caught up sync_point, please wait and retry",
                name());
        tasking::enqueue(LPC_SPLIT_PARTITION,
                         tracker(),
                         std::bind(&replica::check_sync_point, this, sync_point),
                         get_gpid().thread_hash(),
                         std::chrono::seconds(1));
    }
}

void replica::update_group_partition_count(int new_partition_count,
                                           bool is_update_child) // on primary parent
{
    if (is_update_child && (_child_gpid.get_app_id() == 0 || _child_ballot != get_ballot())) {
        dwarn_f("{} receive out-date request, _child_gpid({}.{}), _child_ballot is {}, local "
                "ballot is {}",
                name(),
                _child_gpid.get_app_id(),
                _child_gpid.get_partition_index(),
                _child_ballot,
                get_ballot());
        _stub->on_exec(LPC_SPLIT_PARTITION,
                       _child_gpid,
                       std::bind(&replica::handle_splitting_error,
                                 std::placeholders::_1,
                                 "update_group_partition_count coz out-dated request"));
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

    ddebug_f("{} will update group partition count, new partition count is {}, "
             "is_update_child is {}",
             name(),
             new_partition_count,
             is_update_child);

    gpid pid = is_update_child ? _child_gpid : get_gpid();
    dassert(pid.get_app_id() != 0, "");

    for (auto &iter : _primary_states.statuses) {
        std::shared_ptr<update_group_partition_count_request> request(
            new update_group_partition_count_request);
        request->app = _app_info;
        request->app.partition_count = new_partition_count;
        request->target_address = iter.first;
        request->config.pid = pid;
        request->config.ballot = get_ballot();
        request->last_committed_decree = last_committed_decree();

        dinfo_f("{} send update_group_partition_count_request to {} replica {}",
                 name(),
                 enum_to_string(iter.second),
                 iter.first.to_string());

        rpc::call(
            iter.first,
            RPC_SPLIT_UPDATE_PARTITION_COUNT,
            *request,
            tracker(),
            [=](error_code ec, update_group_partition_count_response &&response) {
                on_update_group_partition_count_reply(
                    ec,
                    request,
                    std::make_shared<update_group_partition_count_response>(std::move(response)),
                    group_address,
                    iter.first,
                    is_update_child);
            },
            std::chrono::seconds(1),
            get_gpid().thread_hash());
    }
}

void replica::on_update_group_partition_count(
    update_group_partition_count_request request,
    update_group_partition_count_response &response) // on all replicas
{

    if (request.config.ballot < get_ballot()) {
        dwarn_f(
            "{} receive out-dated update_group_partition_count_request, request ballot is {}, local ballot is {}",
            name(),
            request.config.ballot,
            get_ballot());

        _stub->on_exec(LPC_SPLIT_PARTITION_ERROR, _split_states.parent_gpid, [](replica_ptr r) {
            r->_child_gpid.set_app_id(0);
        });
        handle_splitting_error("on_update_group_partition_count coz out-dated ballot");

        response.err = ERR_VERSION_OUTDATED;
        return;
    }

    if (_split_states.parent_gpid.get_app_id() != 0 && _split_states.is_caught_up == false) {
        dwarn_f("{} receive out-dated update_group_partition_count_request, child is not caught up, request ballot is {},"
                "local ballot is {}",
                name(),
                request.config.ballot,
                get_ballot());
        response.err = ERR_VERSION_OUTDATED;
        return;
    }

//    ddebug_f("{} execute update group partition count, new partition count is {}",
//             name(),
//             request.app.partition_count);

    ddebug_f("{} process update partition count to {}, primary ={}, ballot = {}, status = {}, last_committed_decree = {}",
           name(),
           request.app.partition_count,
           request.config.primary.to_string(),
           request.config.ballot,
           enum_to_string(request.config.status),
           request.last_committed_decree);

    // save app_info on disk
    auto info = _app_info;
    info.partition_count = request.app.partition_count;

    replica_app_info new_info((app_info *)&info);
    std::string info_path = utils::filesystem::path_combine(_dir, ".app-info");
    error_code err = new_info.store(info_path.c_str());

    if (err != ERR_OK) {
        derror_f("{} failed to save app_info to {}, error is {}",
               name(),
               info_path.c_str(),
               err.to_string());
    } else {
        // save app_info in memory
//        _app_info = info;
//        _partition_version = _app_info.partition_count - 1;
//        _app->set_partition_version(_partition_version);
        // TODO(hyc): to check
        _app_info = info;
        _app->set_partition_version(_app_info.partition_count - 1);
        _partition_version = _app_info.partition_count - 1;
        _app->set_partition_version(_partition_version);
        ddebug_f("{}: succeed to update local partition version to {}",name(), _partition_version.load());

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
    rpc_address finish_update_address,
    bool is_update_child) // on primary parent
{
    _checker.only_one_thread_access();

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
        if (left_replicas->empty()) {
            if (is_update_child) { // all child reply
                ddebug_f("{} finish update child group partition count, is_update_child is {}", name(), is_update_child);
                register_child_on_meta(get_ballot());
            } else { // all parent group reply
                ddebug_f("{} finish update parent group partition count, is_update_child is {}", name(), is_update_child);
                _primary_states.is_sync_to_child = false;
            }
        } else { // not all reply
            ddebug_f("{}, there are still {} replica not update partition count in group",
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
            [this, request, finish_update_address, left_replicas, is_update_child]() {
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
                                  finish_update_address,
                                  is_update_child);
                          },
                          std::chrono::seconds(1),
                          get_gpid().thread_hash());
            },
            get_gpid().thread_hash(),
            std::chrono::seconds(1));
        return;
    }
}

///
/// register child replicas on meta
///

void replica::register_child_on_meta(ballot b) // on primary parent
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_f("{} is not primary, can not register child, current status is {}",
                name(),
                enum_to_string(status()));
        return;
    }

    if (_primary_states.reconfiguration_task != nullptr) {
        ddebug_f("{} is under reconfiguration, delay to register child", name());
        _primary_states.register_child_task =
            tasking::enqueue(LPC_SPLIT_PARTITION,
                             tracker(),
                             std::bind(&replica::register_child_on_meta, this, b),
                             get_gpid().thread_hash(),
                             std::chrono::seconds(1));
        return;
    }

    if (b != get_ballot()) {
        dwarn_f(
            "{} failed to register child, may out-dated, request ballot is {}, local ballot is {}",
            name(),
            b,
            get_ballot());
        return;
    }

    partition_configuration child_config = _primary_states.membership;
    child_config.ballot++;
    child_config.last_committed_decree = 0;
    child_config.last_drops.clear();
    child_config.pid.set_partition_index(_app_info.partition_count +
                                         get_gpid().get_partition_index());

    std::shared_ptr<register_child_request> request(new register_child_request);
    request->app = _app_info;
    request->child_config = child_config;
    request->parent_config = _primary_states.membership;
    request->primary_address = _stub->_primary_address;

    update_local_configuration_with_no_ballot_change(partition_status::PS_INACTIVE);
    set_inactive_state_transient(true);
    _partition_version = -1;

    ddebug_f(
        "{} set register child partition({}.{}) request to meta, current ballot is {}, child ballot is {}",
        name(),
        request->child_config.pid.get_app_id(),
        request->child_config.pid.get_partition_index(),
        request->parent_config.ballot,
        request->child_config.ballot);

    rpc_address meta_address(_stub->_failure_detector->get_servers());
    //TODO(hyc): new change
    _primary_states.register_child_task = rpc::call(meta_address,
              RPC_CM_REGISTER_CHILD_REPLICA,
              *request,
              tracker(),
              [=](error_code ec, register_child_response &&response) {
                  on_register_child_on_meta_reply(
                      ec, request, std::make_shared<register_child_response>(std::move(response)));
              },
              std::chrono::seconds(0),
              get_gpid().thread_hash());
}

void replica::on_register_child_on_meta_reply(
    dsn::error_code ec,
    std::shared_ptr<register_child_request> request,
    std::shared_ptr<register_child_response> response) // on primary parent
{
    // TODO(hyc): consider where should add check
    _checker.only_one_thread_access();

    if (partition_status::PS_INACTIVE != status() || _stub->is_connected() == false) {
        dwarn_f("{} status wrong or stub is not connected, status is {}",
                name(),
                enum_to_string(status()));
        _primary_states.register_child_task = nullptr;
        _primary_states.query_child_state_task = nullptr;

        return;
    }

    if (ec == ERR_OK) {
        ec = response->err;
    }

    if (ec != ERR_OK) {
        dwarn_f("{}: register child({}.{}) reply with error {}, request child ballot is {}, local "
                "ballot is {}",
                name(),
                request->child_config.pid.get_app_id(),
                request->child_config.pid.get_partition_index(),
                ec.to_string(),
                request->child_config.ballot,
                get_ballot());

        if (ec != ERR_INVALID_VERSION && ec != ERR_CHILD_REGISTERED) {
            _primary_states.register_child_task = tasking::enqueue(
                LPC_DELAY_UPDATE_CONFIG,
                tracker(),
                [this, request]() {
                    rpc_address target(_stub->_failure_detector->get_servers());
                    auto rpc_task_ptr = rpc::call(
                        target,
                        RPC_CM_REGISTER_CHILD_REPLICA,
                        *request,
                        tracker(),
                        [=](error_code err, register_child_response &&resp) {
                            on_register_child_on_meta_reply(
                                err,
                                request,
                                std::make_shared<register_child_response>(std::move(resp)));
                        },
                        std::chrono::seconds(0),
                        get_gpid().thread_hash());
                    _primary_states.register_child_task = rpc_task_ptr;
                },
                get_gpid().thread_hash(),
                std::chrono::seconds(1));

            return;
        }
    }

    ddebug_f("{}: register child({}.{}) succeed, parent ballot is {}, local ballot is {}, local "
             "status {}",
             name(),
             response->child_config.pid.get_app_id(),
             response->child_config.pid.get_partition_index(),
             response->parent_config.ballot,
             get_ballot(),
             enum_to_string(status()));

    if (ec == ERR_OK && response->err == ERR_OK) {
        dassert(_app_info.partition_count * 2 == response->app.partition_count,
                "local partition count is %d, remote partition count is %d",
                _app_info.partition_count,
                response->app.partition_count);
        // make child replica become available
        _stub->on_exec(LPC_SPLIT_PARTITION,
                       response->child_config.pid,
                       std::bind(&replica::child_partition_active,
                                 std::placeholders::_1,
                                 response->child_config));
        update_group_partition_count(response->app.partition_count, false);
    }

    _primary_states.register_child_task = nullptr;
    // TODO(hyc): when to change it into false, should there
    _primary_states.is_sync_to_child = true;
    _child_gpid.set_app_id(0);

    if (response->parent_config.ballot >= get_ballot()) {
        ddebug_f("{} ballot in response is {}, local ballot is {}, should update configuration",
                 name(),
                 response->parent_config.ballot,
                 get_ballot());
        update_configuration(response->parent_config);
    }
}

///
/// child replica copy mutations of parent
///

void replica::on_copy_mutation(mutation_ptr &mu) // on child
{
    // 1. check status - partition_split
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_f("{} not during partition split or ballot is not match, status is {}, current ballot is {}, local ballot of "
                 "mutation is {}, ignore copy mutation {}",
                 name(),
                 enum_to_string(status()),
                 get_ballot(),
                 mu->data.header.ballot,
                mu->name());

        _stub->on_exec(LPC_SPLIT_PARTITION_ERROR, _split_states.parent_gpid, [mu](replica_ptr r) {
            r->_child_gpid.set_app_id(0);
            r->on_copy_mutation_reply(ERR_OK, mu->data.header.ballot, mu->data.header.decree);
        });

        return;
    }

    // 2. check status - finish copy prepare list
    if (!_split_states.is_prepare_list_copied) {
        //TODO(hyc): 0115 - fix init bug
        //dwarn_f("{} not copy prepare list from parent, ignore mutation {}", name(), mu->name());
        dwarn_f("{} not copy prepare list from parent, cache mutation {}", name(), mu->name());
        _split_states.child_temp_mutation_list.emplace_back(mu);
        return;
    }

    // 3. ballot not match
    if (mu->data.header.ballot > get_ballot()) {
        dwarn_f("{} local ballot is smaller than request ballot, local ballot is {}, ballot of "
                "mutation is {}, ignore copy mutation {}",
                name(),
                get_ballot(),
                mu->data.header.ballot,
                mu->name());
        _stub->on_exec(LPC_SPLIT_PARTITION_ERROR, _split_states.parent_gpid, [mu](replica_ptr r) {
            r->_child_gpid.set_app_id(0);
            r->on_copy_mutation_reply(ERR_OK, mu->data.header.ballot, mu->data.header.decree);
        });
        handle_splitting_error("on_copy_mutation coz ballot changed");
        return;
    }

    //TODO(hyc): consider
    if(mu->data.header.decree <= _prepare_list->last_committed_decree()){
        dwarn_f("{}: mu decree {} VS plist last_committed_decree {}, ignore this mutation",
                name(), mu->data.header.decree, _prepare_list->last_committed_decree());
        return;
    }

    if(mu->data.header.sync_to_child){
        ddebug_f("{} start to sync copy mutation {}", name(), mu->name());
    }else{
        dinfo_f("{} start to copy mutation {} asynchronously", name(), mu->name());
    }


    // 4. prepare mu as secondary
    mu->data.header.pid = get_gpid();
    _prepare_list->prepare(mu, partition_status::PS_SECONDARY);

    if (!mu->data.header.sync_to_child) {
        // 5. child async copy mutation
        if (!mu->is_logged()) {
            mu->set_logged();
        }
        //        _private_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, tracker(), nullptr);
        mu->log_task() = _stub->_log->append(
            mu, LPC_WRITE_REPLICATION_LOG, &_tracker, nullptr, get_gpid().thread_hash());
    } else {
        // 6. child sync copy mutation
        mu->log_task() = _stub->_log->append(mu,
                                             LPC_WRITE_REPLICATION_LOG,
                                             &_tracker,
                                             std::bind(&replica::on_append_log_completed,
                                                       this,
                                                       mu,
                                                       std::placeholders::_1,
                                                       std::placeholders::_2),
                                             get_gpid().thread_hash());
    }
    _private_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, tracker(), nullptr);
}

void replica::handle_splitting_error(std::string err_msg)
{
    if(status() != partition_status::PS_ERROR){
        derror_f("{}: failed during {}, parent = {}.{}, split_duration = {}ms, async_learn_duration = {}ms",
               name(),
               err_msg.c_str(),
               _split_states.parent_gpid.get_app_id(),
               _split_states.parent_gpid.get_partition_index(),
               _split_states.total_ms(),
               _split_states.async_learn_ms());

        _stub->_counter_replicas_splitting_recent_split_fail_count->increment();
        update_local_configuration_with_no_ballot_change(partition_status::PS_ERROR);
    }
}

} // namespace replication
} // namespace dsn
