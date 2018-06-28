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
                                 decree last_committed_decree)
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
                                                     decree last_committed_decree)
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

    notify_primary_parent_finish_catch_up();
}

void replica::notify_primary_parent_finish_catch_up() {}

} // namespace replication
} // namespace dsn
