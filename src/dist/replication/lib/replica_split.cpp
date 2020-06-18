// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/utility/defer.h>
#include <dsn/utility/filesystem.h>
#include <dsn/utility/fail_point.h>

#include "replica.h"
#include "replica_stub.h"

namespace dsn {
namespace replication {

typedef rpc_holder<notify_catch_up_request, notify_cacth_up_response> notify_catch_up_rpc;

// ThreadPool: THREAD_POOL_REPLICATION
void replica::on_add_child(const group_check_request &request) // on parent partition
{
    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY &&
        (status() != partition_status::PS_INACTIVE || !_inactive_is_transient)) {
        dwarn_replica("receive add child request with wrong status {}, ignore this request",
                      enum_to_string(status()));
        return;
    }

    if (request.config.ballot != get_ballot()) {
        dwarn_replica(
            "receive add child request with different ballot, local ballot({}) VS request "
            "ballot({}), ignore this request",
            get_ballot(),
            request.config.ballot);
        return;
    }

    gpid child_gpid = request.child_gpid;
    if (_child_gpid == child_gpid) {
        dwarn_replica("child replica({}) is already existed, might be partition splitting, ignore "
                      "this request",
                      child_gpid);
        return;
    }

    if (child_gpid.get_partition_index() < _app_info.partition_count) {
        dwarn_replica("receive old add child request, child gpid is ({}), "
                      "local partition count is {}, ignore this request",
                      child_gpid,
                      _app_info.partition_count);
        return;
    }

    _child_gpid = child_gpid;
    _child_init_ballot = get_ballot();

    ddebug_replica("process add child({}), primary is {}, ballot is {}, "
                   "status is {}, last_committed_decree is {}",
                   child_gpid,
                   request.config.primary.to_string(),
                   request.config.ballot,
                   enum_to_string(request.config.status),
                   request.last_committed_decree);

    tasking::enqueue(LPC_CREATE_CHILD,
                     tracker(),
                     std::bind(&replica_stub::create_child_replica,
                               _stub,
                               _config.primary,
                               _app_info,
                               _child_init_ballot,
                               _child_gpid,
                               get_gpid(),
                               _dir),
                     get_gpid().thread_hash());
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::child_init_replica(gpid parent_gpid,
                                 rpc_address primary_address,
                                 ballot init_ballot) // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_init_replica", [](dsn::string_view) {});

    if (status() != partition_status::PS_INACTIVE) {
        dwarn_replica("wrong status {}", enum_to_string(status()));
        _stub->split_replica_error_handler(
            parent_gpid, std::bind(&replica::parent_cleanup_split_context, std::placeholders::_1));
        return;
    }

    // update replica config
    _config.ballot = init_ballot;
    _config.primary = primary_address;
    _config.status = partition_status::PS_PARTITION_SPLIT;

    // init split states
    _split_states.parent_gpid = parent_gpid;

    _split_states.is_prepare_list_copied = false;
    _split_states.is_caught_up = false;

    _split_states.splitting_start_ts_ns = dsn_now_ns();
    // TODO(hyc): pref-counter - delete
    ddebug_f("{}: splitting_start_ts_ns is {}", name(), _split_states.splitting_start_ts_ns);
    _stub->_counter_replicas_splitting_recent_start_count->increment();
    // TODO(hyc): delete
    ddebug_f("stub recent start split count is {}",
             _stub->_counter_replicas_splitting_recent_start_count.get()->get_value());

    // heartbeat
    _split_states.check_state_task = tasking::enqueue(LPC_PARTITION_SPLIT,
                                                      tracker(),
                                                      std::bind(&replica::check_child_state, this),
                                                      get_gpid().thread_hash(),
                                                      std::chrono::seconds(5));

    ddebug_replica("init ballot is {}, parent gpid is ({})", init_ballot, parent_gpid);

    dsn::error_code ec = _stub->split_replica_exec(
        LPC_PARTITION_SPLIT,
        _split_states.parent_gpid,
        std::bind(&replica::parent_prepare_states, std::placeholders::_1, _app->learn_dir()));
    if (ec != ERR_OK) {
        child_handle_split_error("parent not exist when execute parent_prepare_states");
    }
}

void replica::check_child_state() // on child
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_f("{} status is not PS_PARTITION_SPLIT during check_child_state, but {}",
                name(),
                enum_to_string(status()));
        _split_states.check_state_task = nullptr;
        return;
    }

    ddebug_f("{} child partition state checked", name());

    // parent check its state
    dsn::error_code ec =
        _stub->split_replica_exec(LPC_PARTITION_SPLIT,
                                  _split_states.parent_gpid,
                                  std::bind(&replica::parent_check_states, std::placeholders::_1));
    if (ec != ERR_OK) {
        child_handle_split_error("check_child_state failed because parent gpid is invalid");
    }

    // restart check_state_task
    // TODO(hyc): consider heartbeat interval
    _split_states.check_state_task = tasking::enqueue(LPC_PARTITION_SPLIT,
                                                      tracker(),
                                                      std::bind(&replica::check_child_state, this),
                                                      get_gpid().thread_hash(),
                                                      std::chrono::seconds(5));
}

// ThreadPool: THREAD_POOL_REPLICATION
bool replica::parent_check_states() // on parent partition
{
    FAIL_POINT_INJECT_F("replica_parent_check_states", [](dsn::string_view) { return true; });

    if (_child_init_ballot != get_ballot() || _child_gpid.get_app_id() == 0 ||
        (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY &&
         (status() != partition_status::PS_INACTIVE || !_inactive_is_transient))) {
        dwarn_replica("parent wrong states: status({}), init_ballot({}) VS current_ballot({}), "
                      "child_gpid({})",
                      enum_to_string(status()),
                      _child_init_ballot,
                      get_ballot(),
                      _child_gpid);
        _stub->split_replica_error_handler(
            _child_gpid,
            std::bind(&replica::child_handle_split_error,
                      std::placeholders::_1,
                      "wrong parent states when execute parent_check_states"));
        parent_cleanup_split_context();
        return false;
    }
    return true;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::parent_prepare_states(const std::string &dir) // on parent partition
{
    FAIL_POINT_INJECT_F("replica_parent_prepare_states", [](dsn::string_view) {});

    if (!parent_check_states()) {
        return;
    }

    learn_state parent_states;
    int64_t checkpoint_decree;
    // generate checkpoint
    dsn::error_code ec = _app->copy_checkpoint_to_dir(dir.c_str(), &checkpoint_decree);
    if (ec == ERR_OK) {
        ddebug_replica("prepare checkpoint succeed: checkpoint dir = {}, checkpoint decree = {}",
                       dir,
                       checkpoint_decree);
        parent_states.to_decree_included = checkpoint_decree;
        // learn_state.files[0] will be used to get learn dir in function 'storage_apply_checkpoint'
        // so we add a fake file name here, this file won't appear on disk
        parent_states.files.push_back(dsn::utils::filesystem::path_combine(dir, "file_name"));
    } else {
        derror_replica("prepare checkpoint failed, error = {}", ec.to_string());
        tasking::enqueue(LPC_PARTITION_SPLIT,
                         tracker(),
                         std::bind(&replica::parent_prepare_states, this, dir),
                         get_gpid().thread_hash(),
                         std::chrono::seconds(1));
        return;
    }

    std::vector<mutation_ptr> mutation_list;
    std::vector<std::string> files;
    uint64_t total_file_size = 0;
    // get mutation and private log
    _private_log->get_parent_mutations_and_logs(
        get_gpid(), checkpoint_decree + 1, invalid_ballot, mutation_list, files, total_file_size);

    // get prepare list
    std::shared_ptr<prepare_list> plist = std::make_shared<prepare_list>(this, *_prepare_list);
    plist->truncate(last_committed_decree());

    dassert_replica(
        last_committed_decree() == checkpoint_decree || !mutation_list.empty() || !files.empty(),
        "last_committed_decree({}) VS checkpoint_decree({}), mutation_list size={}, files size={}",
        last_committed_decree(),
        checkpoint_decree,
        mutation_list.size(),
        files.size());

    ddebug_replica("prepare state succeed: {} mutations, {} private log files, total file size = "
                   "{}, last_committed_decree = {}",
                   mutation_list.size(),
                   files.size(),
                   total_file_size,
                   last_committed_decree());

    ec = _stub->split_replica_exec(LPC_PARTITION_SPLIT,
                                   _child_gpid,
                                   std::bind(&replica::child_copy_prepare_list,
                                             std::placeholders::_1,
                                             parent_states,
                                             mutation_list,
                                             files,
                                             total_file_size,
                                             std::move(plist)));
    if (ec != ERR_OK) {
        parent_cleanup_split_context();
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::child_copy_prepare_list(learn_state lstate,
                                      std::vector<mutation_ptr> mutation_list,
                                      std::vector<std::string> plog_files,
                                      uint64_t total_file_size,
                                      std::shared_ptr<prepare_list> plist) // on child partition
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_replica("wrong status, status is {}", enum_to_string(status()));
        _stub->split_replica_error_handler(
            _split_states.parent_gpid,
            std::bind(&replica::parent_cleanup_split_context, std::placeholders::_1));
        child_handle_split_error("wrong child status when execute child_copy_prepare_list");
        return;
    }

    // learning parent states is time-consuming, should execute in THREAD_POOL_REPLICATION_LONG
    decree last_committed_decree = plist->last_committed_decree();
    // start async learn task
    _split_states.splitting_start_async_learn_ts_ns = dsn_now_ns();
    _split_states.async_learn_task = tasking::enqueue(LPC_PARTITION_SPLIT_ASYNC_LEARN,
                                                      tracker(),
                                                      std::bind(&replica::child_learn_states,
                                                                this,
                                                                lstate,
                                                                mutation_list,
                                                                plog_files,
                                                                total_file_size,
                                                                last_committed_decree));

    ddebug_replica("start to copy parent prepare list, last_committed_decree={}, prepare list min "
                   "decree={}, max decree={}",
                   last_committed_decree,
                   plist->min_decree(),
                   plist->max_decree());

    // copy parent prepare list
    plist->set_committer(std::bind(&replica::execute_mutation, this, std::placeholders::_1));
    delete _prepare_list;
    _prepare_list = new prepare_list(this, *plist);
    for (decree d = last_committed_decree + 1; d <= _prepare_list->max_decree(); ++d) {
        mutation_ptr mu = _prepare_list->get_mutation_by_decree(d);
        dassert_replica(mu != nullptr, "can not find mutation, dercee={}", d);
        mu->data.header.pid = get_gpid();
        _stub->_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, tracker(), nullptr);
        _private_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, tracker(), nullptr);
        // set mutation has been logged in private log
        if (!mu->is_logged()) {
            mu->set_logged();
        }
    }
    _split_states.is_prepare_list_copied = true;
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
void replica::child_learn_states(learn_state lstate,
                                 std::vector<mutation_ptr> mutation_list,
                                 std::vector<std::string> plog_files,
                                 uint64_t total_file_size,
                                 decree last_committed_decree) // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_learn_states", [](dsn::string_view) {});

    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_replica("wrong status, status is {}", enum_to_string(status()));
        child_handle_async_learn_error();
        return;
    }

    ddebug_replica("start to learn states asynchronously, prepare_list last_committed_decree={}, "
                   "checkpoint decree range=({},{}], private log files count={}, in-memory "
                   "mutation count={}",
                   last_committed_decree,
                   lstate.from_decree_excluded,
                   lstate.to_decree_included,
                   plog_files.size(),
                   mutation_list.size());

    error_code err;
    auto cleanup = defer([this, &err]() {
        if (err != ERR_OK) {
            child_handle_async_learn_error();
        }
    });

    // apply parent checkpoint
    err = _app->apply_checkpoint(replication_app_base::chkpt_apply_mode::learn, lstate);
    if (err != ERR_OK) {
        derror_replica("failed to apply checkpoint, error={}", err);
        return;
    }

    // replay parent private log and learn in-memory mutations
    err =
        child_apply_private_logs(plog_files, mutation_list, total_file_size, last_committed_decree);
    if (err != ERR_OK) {
        derror_replica("failed to replay private log, error={}", err);
        return;
    }

    // generate a checkpoint synchronously
    err = _app->sync_checkpoint();
    if (err != ERR_OK) {
        derror_replica("failed to generate checkpoint synchrounously, error={}", err);
        return;
    }

    err = _app->update_init_info_ballot_and_decree(this);
    if (err != ERR_OK) {
        derror_replica("update_init_info_ballot_and_decree failed, error={}", err);
        return;
    }

    ddebug_replica("learn parent states asynchronously succeed");

    tasking::enqueue(LPC_PARTITION_SPLIT,
                     tracker(),
                     std::bind(&replica::child_catch_up_states, this),
                     get_gpid().thread_hash());
    _split_states.async_learn_task = nullptr;
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
error_code replica::child_apply_private_logs(std::vector<std::string> plog_files,
                                             std::vector<mutation_ptr> mutation_list,
                                             uint64_t total_file_size,
                                             decree last_committed_decree) // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_apply_private_logs", [](dsn::string_view arg) {
        return error_code::try_get(arg.data(), ERR_OK);
    });

    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_replica("wrong status={}", enum_to_string(status()));
        return ERR_INVALID_STATE;
    }

    error_code ec;
    int64_t offset;
    // temp prepare_list used for apply states
    prepare_list plist(this,
                       _app->last_committed_decree(),
                       _options->max_mutation_count_in_prepare_list,
                       [this](mutation_ptr &mu) {
                           if (mu->data.header.decree == _app->last_committed_decree() + 1) {
                               _app->apply_mutation(mu);
                           }
                       });

    // replay private log
    ec = mutation_log::replay(plog_files,
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
    if (ec != ERR_OK) {
        dwarn_replica(
            "replay private_log files failed, file count={}, app last_committed_decree={}",
            plog_files.size(),
            _app->last_committed_decree());
        return ec;
    }

    _split_states.splitting_copy_file_count += plog_files.size();
    _split_states.splitting_copy_file_size += total_file_size;
    _stub->_counter_replicas_splitting_recent_copy_file_count->add(plog_files.size());
    _stub->_counter_replicas_splitting_recent_copy_file_size->add(total_file_size);

    ddebug_replica("replay private_log files succeed, file count={}, app last_committed_decree={}",
                   plog_files.size(),
                   _app->last_committed_decree());

    // apply in-memory mutations if replay private logs succeed
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

    _split_states.splitting_copy_mutation_count += count;
    _stub->_counter_replicas_splitting_recent_copy_mutation_count->add(count);

    plist.commit(last_committed_decree, COMMIT_TO_DECREE_HARD);
    ddebug_replica(
        "apply in-memory mutations succeed, mutation count={}, app last_committed_decree={}",
        count,
        _app->last_committed_decree());

    return ec;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::child_catch_up_states() // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_catch_up_states", [](dsn::string_view) {});

    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_replica("wrong status, status is {}", enum_to_string(status()));
        return;
    }

    // parent will copy mutations to child during async-learn, as a result:
    // - child prepare_list last_committed_decree = parent prepare_list last_committed_decree, also
    // is catch_up goal_decree
    // - local_decree is child local last_committed_decree which is the last decree in async-learn.
    decree goal_decree = _prepare_list->last_committed_decree();
    decree local_decree = _app->last_committed_decree();

    // there are mutations written to parent during async-learn
    // child does not catch up parent, there are still some mutations child not learn
    if (local_decree < goal_decree) {
        if (local_decree >= _prepare_list->min_decree()) {
            // all missing mutations are all in prepare list
            dwarn_replica("there are some in-memory mutations should be learned, app "
                          "last_committed_decree={}, "
                          "goal decree={}, prepare_list min_decree={}",
                          local_decree,
                          goal_decree,
                          _prepare_list->min_decree());
            for (decree d = local_decree + 1; d <= goal_decree; ++d) {
                auto mu = _prepare_list->get_mutation_by_decree(d);
                dassert(mu != nullptr, "");
                error_code ec = _app->apply_mutation(mu);
                if (ec != ERR_OK) {
                    child_handle_split_error("child_catchup failed because apply mutation failed");
                    return;
                }
            }
        } else {
            // some missing mutations have already in private log
            // should call `catch_up_with_private_logs` to catch up all missing mutations
            dwarn_replica(
                "there are some private logs should be learned, app last_committed_decree="
                "{}, prepare_list min_decree={}, please wait",
                local_decree,
                _prepare_list->min_decree());
            _split_states.async_learn_task = tasking::enqueue(
                LPC_CATCHUP_WITH_PRIVATE_LOGS,
                tracker(),
                [this]() {
                    catch_up_with_private_logs(partition_status::PS_PARTITION_SPLIT);
                    _split_states.async_learn_task = nullptr;
                },
                get_gpid().thread_hash());
            return;
        }
    }

    ddebug_replica("child catch up parent states, goal decree={}, local decree={}",
                   _prepare_list->last_committed_decree(),
                   _app->last_committed_decree());
    _split_states.is_caught_up = true;

    child_notify_catch_up();
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::child_notify_catch_up() // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_notify_catch_up", [](dsn::string_view) {});

    std::unique_ptr<notify_catch_up_request> request = make_unique<notify_catch_up_request>();
    request->parent_gpid = _split_states.parent_gpid;
    request->child_gpid = get_gpid();
    request->child_ballot = get_ballot();
    request->child_address = _stub->_primary_address;

    ddebug_replica("send notification to primary: {}@{}, ballot={}",
                   _split_states.parent_gpid,
                   _config.primary.to_string(),
                   get_ballot());

    notify_catch_up_rpc rpc(std::move(request), RPC_SPLIT_NOTIFY_CATCH_UP);
    rpc.call(_config.primary,
             tracker(),
             [this, rpc](error_code ec) mutable {
                 auto response = rpc.response();
                 if (ec == ERR_TIMEOUT) {
                     dwarn_replica("notify primary catch up timeout, please wait and retry");
                     tasking::enqueue(LPC_PARTITION_SPLIT,
                                      tracker(),
                                      std::bind(&replica::child_notify_catch_up, this),
                                      get_gpid().thread_hash(),
                                      std::chrono::seconds(1));
                     return;
                 }
                 if (ec != ERR_OK || response.err != ERR_OK) {
                     error_code err = (ec == ERR_OK) ? response.err : ec;
                     dwarn_replica("failed to notify primary catch up, error={}", err.to_string());
                     _stub->split_replica_error_handler(
                         _split_states.parent_gpid,
                         std::bind(&replica::parent_cleanup_split_context, std::placeholders::_1));
                     child_handle_split_error("notify_primary_split_catch_up");
                     return;
                 }
                 ddebug_replica("notify primary catch up succeed");
             },
             _split_states.parent_gpid.thread_hash());
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::parent_handle_child_catch_up(const notify_catch_up_request &request,
                                           notify_cacth_up_response &response) // on primary parent
{
    if (status() != partition_status::PS_PRIMARY) {
        derror_replica("status is {}", enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.child_ballot != get_ballot()) {
        derror_replica("receive out-date request, request ballot = {}, local ballot = {}",
                       request.child_ballot,
                       get_ballot());
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.child_gpid != _child_gpid) {
        derror_replica(
            "receive wrong child request, request child_gpid = {}, local child_gpid = {}",
            request.child_gpid,
            _child_gpid);
        response.err = ERR_INVALID_STATE;
        return;
    }

    response.err = ERR_OK;
    ddebug_replica("receive catch_up request from {}@{}, current ballot={}",
                   request.child_gpid,
                   request.child_address.to_string(),
                   request.child_ballot);

    _primary_states.caught_up_children.insert(request.child_address);
    // _primary_states.statuses is a map structure: rpc address -> partition_status
    // it stores replica's rpc address and partition_status of this replica group
    for (auto &iter : _primary_states.statuses) {
        if (_primary_states.caught_up_children.find(iter.first) ==
            _primary_states.caught_up_children.end()) {
            // there are child partitions not caught up its parent
            return;
        }
    }

    ddebug_replica("all child partitions catch up");
    _primary_states.caught_up_children.clear();
    _primary_states.sync_send_write_request = true;

    // sync_point is the first decree after parent send write request to child synchronously
    // when sync_point commit, parent consider child has all data it should have during async-learn
    decree sync_point = _prepare_list->max_decree() + 1;
    if (!_options->empty_write_disabled) {
        // empty wirte here to commit sync_point
        mutation_ptr mu = new_mutation(invalid_decree);
        mu->add_client_request(RPC_REPLICATION_WRITE_EMPTY, nullptr);
        init_prepare(mu, false);
        dassert_replica(sync_point == mu->data.header.decree,
                        "sync_point should be equal to mutation's decree, {} vs {}",
                        sync_point,
                        mu->data.header.decree);
    };

    // check if sync_point has been committed
    tasking::enqueue(LPC_PARTITION_SPLIT,
                     tracker(),
                     std::bind(&replica::parent_check_sync_point_commit, this, sync_point),
                     get_gpid().thread_hash(),
                     std::chrono::seconds(1));
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::parent_check_sync_point_commit(decree sync_point) // on primary parent
{
    FAIL_POINT_INJECT_F("replica_parent_check_sync_point_commit", [](dsn::string_view) {});
    ddebug_replica("sync_point = {}, app last_committed_decree = {}",
                   sync_point,
                   _app->last_committed_decree());
    if (_app->last_committed_decree() >= sync_point) {
        update_group_partition_count(_app_info.partition_count * 2, true);
    } else {
        dwarn_replica("sync_point has not been committed, please wait and retry");
        tasking::enqueue(LPC_PARTITION_SPLIT,
                         tracker(),
                         std::bind(&replica::parent_check_sync_point_commit, this, sync_point),
                         get_gpid().thread_hash(),
                         std::chrono::seconds(1));
    }
}

void replica::update_group_partition_count(int new_partition_count,
                                           bool is_update_child) // on primary parent
{
    if (is_update_child && (_child_gpid.get_app_id() == 0 || _child_init_ballot != get_ballot())) {
        dwarn_f("{} receive out-date request, _child_gpid({}.{}), _child_init_ballot is {}, local "
                "ballot is {}",
                name(),
                _child_gpid.get_app_id(),
                _child_gpid.get_partition_index(),
                _child_init_ballot,
                get_ballot());
        _stub->split_replica_error_handler(
            _child_gpid,
            std::bind(&replica::child_handle_split_error,
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
        dwarn_f("{} receive out-dated update_group_partition_count_request, request ballot is {}, "
                "local ballot is {}",
                name(),
                request.config.ballot,
                get_ballot());

        _stub->split_replica_error_handler(_split_states.parent_gpid,
                                           [](replica_ptr r) { r->_child_gpid.set_app_id(0); });
        child_handle_split_error("on_update_group_partition_count coz out-dated ballot");

        response.err = ERR_VERSION_OUTDATED;
        return;
    }

    if (_split_states.parent_gpid.get_app_id() != 0 && _split_states.is_caught_up == false) {
        dwarn_f("{} receive out-dated update_group_partition_count_request, child is not caught "
                "up, request ballot is {},"
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

    ddebug_f("{} process update partition count to {}, primary ={}, ballot = {}, status = {}, "
             "last_committed_decree = {}",
             name(),
             request.app.partition_count,
             request.config.primary.to_string(),
             request.config.ballot,
             enum_to_string(request.config.status),
             request.last_committed_decree);

    // save app_info on disk
    auto info = _app_info;
    if (info.init_partition_count < 1) {
        info.init_partition_count = info.partition_count;
    }
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
        ddebug_f("{}: succeed to update local partition version to {}",
                 name(),
                 _partition_version.load());

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
                ddebug_f("{} finish update child group partition count, is_update_child is {}",
                         name(),
                         is_update_child);
                register_child_on_meta(get_ballot());
            } else { // all parent group reply
                ddebug_f("{} finish update parent group partition count, is_update_child is {}",
                         name(),
                         is_update_child);
                _primary_states.sync_send_write_request = false;
            }
        } else { // not all reply
            ddebug_f("{}, there are still {} replica not update partition count in group",
                     name(),
                     left_replicas->size());
        }
    } else {
        error_code error = (ec == ERR_OK) ? response->err : ec;
        if (error != ERR_TIMEOUT) {
            dwarn_f("{} failed to execute on_update_group_partition_count_reply, error is {}",
                    name(),
                    error.to_string());
            _stub->split_replica_error_handler(_child_gpid,
                                               std::bind(&replica::child_handle_split_error,
                                                         std::placeholders::_1,
                                                         "on_update_group_partition_count_reply"));
            _child_gpid.set_app_id(0);
            return;
        }

        dwarn_f("{} failed to execute on_update_group_partition_count_reply, error is {}, retry",
                name(),
                error.to_string());
        tasking::enqueue(
            LPC_PARTITION_SPLIT,
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
            tasking::enqueue(LPC_PARTITION_SPLIT,
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

    ddebug_f("{} set register child partition({}.{}) request to meta, current ballot is {}, child "
             "ballot is {}",
             name(),
             request->child_config.pid.get_app_id(),
             request->child_config.pid.get_partition_index(),
             request->parent_config.ballot,
             request->child_config.ballot);

    rpc_address meta_address(_stub->_failure_detector->get_servers());
    // TODO(hyc): new change
    _primary_states.register_child_task = rpc::call(
        meta_address,
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

    // handle register failed
    if (ec == ERR_CHILD_DROPPED || ec == ERR_REJECT) {
        dwarn_f("{}: register child({}.{}) failed coz partition split is paused or canceled",
                name(),
                request->child_config.pid.get_app_id(),
                request->child_config.pid.get_partition_index());
        _stub->split_replica_error_handler(
            _child_gpid,
            std::bind(&replica::child_handle_split_error,
                      std::placeholders::_1,
                      "register child failed coz split paused or canceled"));
        _child_gpid.set_app_id(0);
        return;
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

    if (response->parent_config.pid != get_gpid() || response->child_config.pid != _child_gpid) {
        derror_f("{}: remote gpid ({}.{}) VS local gpid ({}.{}), remote child ({}.{}) VS local "
                 "child ({}.{}), something wrong with meta, retry",
                 name(),
                 response->parent_config.pid.get_app_id(),
                 response->parent_config.pid.get_partition_index(),
                 get_gpid().get_app_id(),
                 get_gpid().get_partition_index(),
                 response->child_config.pid.get_app_id(),
                 response->child_config.pid.get_partition_index(),
                 _child_gpid.get_app_id(),
                 _child_gpid.get_partition_index());

        _primary_states.register_child_task = tasking::enqueue(
            LPC_DELAY_UPDATE_CONFIG,
            tracker(),
            [this, request]() {
                rpc_address target(_stub->_failure_detector->get_servers());
                auto rpc_task_ptr =
                    rpc::call(target,
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

    if (ec == ERR_OK && response->err == ERR_OK) {
        ddebug_f(
            "{}: register child({}.{}) succeed, parent ballot is {}, local ballot is {}, local "
            "status {}",
            name(),
            response->child_config.pid.get_app_id(),
            response->child_config.pid.get_partition_index(),
            response->parent_config.ballot,
            get_ballot(),
            enum_to_string(status()));
        dassert(_app_info.partition_count * 2 == response->app.partition_count,
                "local partition count is %d, remote partition count is %d",
                _app_info.partition_count,
                response->app.partition_count);
        // make child replica become available
        _stub->split_replica_exec(LPC_PARTITION_SPLIT,
                                  response->child_config.pid,
                                  std::bind(&replica::child_partition_active,
                                            std::placeholders::_1,
                                            response->child_config));
        update_group_partition_count(response->app.partition_count, false);
    }

    _primary_states.register_child_task = nullptr;
    // TODO(hyc): when to change it into false, should there
    _primary_states.sync_send_write_request = true;
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
        dwarn_f("{} not during partition split or ballot is not match, status is {}, current "
                "ballot is {}, local ballot of "
                "mutation is {}, ignore copy mutation {}",
                name(),
                enum_to_string(status()),
                get_ballot(),
                mu->data.header.ballot,
                mu->name());

        _stub->split_replica_error_handler(_split_states.parent_gpid, [mu](replica_ptr r) {
            r->_child_gpid.set_app_id(0);
            r->on_copy_mutation_reply(ERR_OK, mu->data.header.ballot, mu->data.header.decree);
        });

        return;
    }

    // 2. check status - finish copy prepare list
    if (!_split_states.is_prepare_list_copied) {
        // TODO(hyc): 0115 - fix init bug
        dwarn_f("{} not copy prepare list from parent, ignore mutation {}", name(), mu->name());
        // TODO(hyc): 0219 - try to remove child_temp_mutation_list
        //        dwarn_f("{} not copy prepare list from parent, cache mutation {}", name(),
        //        mu->name());
        //        _split_states.child_temp_mutation_list.emplace_back(mu);
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
        _stub->split_replica_error_handler(_split_states.parent_gpid, [mu](replica_ptr r) {
            r->_child_gpid.set_app_id(0);
            r->on_copy_mutation_reply(ERR_OK, mu->data.header.ballot, mu->data.header.decree);
        });
        child_handle_split_error("on_copy_mutation coz ballot changed");
        return;
    }

    // TODO(hyc): consider
    if (mu->data.header.decree <= _prepare_list->last_committed_decree()) {
        dwarn_f("{}: mu decree {} VS plist last_committed_decree {}, ignore this mutation",
                name(),
                mu->data.header.decree,
                _prepare_list->last_committed_decree());
        return;
    }

    if (mu->is_sync_to_child()) {
        ddebug_f("{} start to sync copy mutation {}", name(), mu->name());
    } else {
        dinfo_f("{} start to copy mutation {} asynchronously", name(), mu->name());
    }

    // 4. prepare mu as secondary
    mu->data.header.pid = get_gpid();
    _prepare_list->prepare(mu, partition_status::PS_SECONDARY);

    if (!mu->is_sync_to_child()) {
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

// ThreadPool: THREAD_POOL_REPLICATION
void replica::parent_cleanup_split_context() // on parent partition
{
    _child_gpid.set_app_id(0);
    _child_init_ballot = 0;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::child_handle_split_error(const std::string &error_msg) // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_handle_split_error", [](dsn::string_view) {});

    if (status() != partition_status::PS_ERROR) {
        dwarn_replica("partition split failed because {}, parent={}, split_duration={}ms, "
                      "async_learn_duration={}ms",
                      error_msg,
                      _split_states.parent_gpid,
                      _split_states.total_ms(),
                      _split_states.async_learn_ms());

        _stub->_counter_replicas_splitting_recent_split_fail_count->increment();
        update_local_configuration_with_no_ballot_change(partition_status::PS_ERROR);
    }
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
void replica::child_handle_async_learn_error() // on child partition
{
    _stub->split_replica_error_handler(
        _split_states.parent_gpid,
        std::bind(&replica::parent_cleanup_split_context, std::placeholders::_1));
    child_handle_split_error("meet error when execute child_learn_states");
    _split_states.async_learn_task = nullptr;
}

void replica::on_stop_split(const stop_split_request &req, stop_split_response &resp)
{
    _checker.only_one_thread_access();

    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("receive control split request with wrong status {}",
                      enum_to_string(status()));
        resp.err = ERR_INVALID_STATE;
        return;
    }

    if (req.type == split_control_type::PSC_PAUSE) {
        if (req.partition_count != _app_info.partition_count * 2) {
            // TODO(heyuchen): update this log
            dwarn_replica("receive pause split request with wrong partition_count");
            resp.err = ERR_NO_NEED_OPERATE;
            return;
        }

        resp.err = ERR_OK;
        ddebug_replica("start to pause partition split");
        if (_child_gpid.get_app_id() > 0) {
            _stub->split_replica_error_handler(_child_gpid,
                                               std::bind(&replica::child_handle_split_error,
                                                         std::placeholders::_1,
                                                         "pause partition split"));
            parent_cleanup_split_context();
        }
    }

    if (req.type == split_control_type::PSC_CANCEL) {
        if (req.partition_count != _app_info.partition_count) {
            // TODO(heyuchen): update this log
            dwarn_replica("receive cancel split request with wrong partition_count");
            resp.err = ERR_NO_NEED_OPERATE;
            return;
        }

        resp.err = ERR_OK;
        ddebug_replica("start to cancel partition split");
        if (_child_gpid.get_app_id() > 0) {
            _stub->split_replica_error_handler(_child_gpid,
                                               std::bind(&replica::child_handle_split_error,
                                                         std::placeholders::_1,
                                                         "cancel partition split"));
            parent_cleanup_split_context();
        }
    }
}

} // namespace replication
} // namespace dsn
