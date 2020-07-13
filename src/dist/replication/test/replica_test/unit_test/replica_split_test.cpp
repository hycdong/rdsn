// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>

#include <dsn/utility/fail_point.h>
#include "replica_test_base.h"

namespace dsn {
namespace replication {

class replica_split_test : public replica_test_base
{
public:
    replica_split_test()
    {
        mock_app_info();
        _parent = stub->generate_replica(
            _app_info, PARENT_GPID, partition_status::PS_PRIMARY, INIT_BALLOT);
        fail::setup();
    }

    ~replica_split_test() { fail::teardown(); }

    void mock_register_child_request()
    {
        partition_configuration &p_config = _register_req.parent_config;
        p_config.pid = PARENT_GPID;
        p_config.ballot = INIT_BALLOT;
        p_config.last_committed_decree = DECREE;

        partition_configuration &c_config = _register_req.child_config;
        c_config.pid = CHILD_GPID;
        c_config.ballot = INIT_BALLOT + 1;
        c_config.last_committed_decree = 0;

        _register_req.app = _app_info;
        _register_req.primary_address = dsn::rpc_address("127.0.0.1", 10086);
    }

    void mock_parent_states()
    {
        mock_shared_log();
        mock_private_log(PARENT_GPID, _parent, true);
        mock_prepare_list(_parent, true);
    }

    primary_context get_replica_primary_context(mock_replica_ptr rep)
    {
        return rep->_primary_states;
    }

    bool is_parent_not_in_split() { return (_parent->_child_gpid.get_app_id() == 0); }

    /// test functions
    void test_try_to_start_split()
    {
        _parent->try_to_start_split(NEW_PARTITION_COUNT);
        _parent->tracker()->wait_outstanding_tasks();
    }

    void test_on_add_child(ballot b, bool is_splitting)
    {
        _parent->set_split_status(is_splitting ? split_status::SPLITTING : split_status::NOT_SPLIT);

        group_check_request req;
        req.child_gpid = CHILD_GPID;
        req.config.ballot = b;
        req.config.status = partition_status::PS_PRIMARY;
        _parent->on_add_child(req);
        _parent->tracker()->wait_outstanding_tasks();
    }

    // TODO(heyuchen):
    bool test_parent_check_states() { return _parent->parent_check_states(); }

    void test_child_copy_prepare_list()
    {
        mock_child_async_learn_states(_parent, false, DECREE);
        std::shared_ptr<prepare_list> plist = std::make_shared<prepare_list>(_parent, *_mock_plist);
        _child->child_copy_prepare_list(_mock_learn_state,
                                        _mutation_list,
                                        _private_log_files,
                                        TOTAL_FILE_SIZE,
                                        std::move(plist));
        _child->tracker()->wait_outstanding_tasks();
    }

    void test_child_learn_states()
    {
        mock_child_async_learn_states(_child, true, DECREE);
        _child->child_learn_states(
            _mock_learn_state, _mutation_list, _private_log_files, TOTAL_FILE_SIZE, DECREE);
        _child->tracker()->wait_outstanding_tasks();
    }

    void test_child_apply_private_logs()
    {
        mock_child_async_learn_states(_child, true, 0);
        _child->child_apply_private_logs(
            _private_log_files, _mutation_list, TOTAL_FILE_SIZE, DECREE);
        _child->tracker()->wait_outstanding_tasks();
    }

    void test_child_catch_up_states(decree local_decree, decree goal_decree, decree min_decree)
    {
        mock_child_async_learn_states(_child, true, 0);
        _child->set_app_last_committed_decree(local_decree);
        if (local_decree < goal_decree) {
            // set prepare_list's start_decree = {min_decree}
            _child->prepare_list_truncate(min_decree);
            // set prepare_list's last_committed_decree = {goal_decree}
            _child->prepare_list_commit_hard(goal_decree);
        }
        _child->child_catch_up_states();
        _child->tracker()->wait_outstanding_tasks();
    }

    // TODO(heyuchen): refactoring
    error_code test_parent_handle_child_catch_up(ballot child_ballot)
    {
        _parent->set_child_gpid(CHILD_GPID);

        notify_catch_up_request req;
        req.child_gpid = CHILD_GPID;
        req.parent_gpid = PARENT_GPID;
        req.child_ballot = child_ballot;
        req.child_address = PRIMARY;

        notify_cacth_up_response resp;
        _parent->parent_handle_child_catch_up(req, resp);
        _parent->tracker()->wait_outstanding_tasks();
        return resp.err;
    }

    void test_update_child_group_partition_count()
    {
        _parent->update_child_group_partition_count(NEW_PARTITION_COUNT);
        _parent->tracker()->wait_outstanding_tasks();
        _child->tracker()->wait_outstanding_tasks();
    }

    void test_register_child_on_meta()
    {
        _parent->register_child_on_meta(INIT_BALLOT);
        _parent->tracker()->wait_outstanding_tasks();
    }

    void test_on_register_child_rely(partition_status::type status, dsn::error_code resp_err)
    {
        mock_register_child_request();
        _parent->_config.status = status;

        register_child_response resp;
        resp.err = resp_err;
        resp.app = _register_req.app;
        resp.app.partition_count *= 2;
        resp.parent_config = _register_req.parent_config;
        resp.child_config = _register_req.child_config;

        _parent->on_register_child_on_meta_reply(ERR_OK, _register_req, resp);
        _parent->tracker()->wait_outstanding_tasks();
    }

    error_code test_child_update_group_partition_count(ballot b)
    {
        update_child_group_partition_count_request req;
        mock_update_child_partition_count_request(req, b);

        update_child_group_partition_count_response resp;
        _child->on_update_child_group_partition_count(req, resp);
        _child->tracker()->wait_outstanding_tasks();
        return resp.err;
    }

    error_code test_on_update_child_group_partition_count(error_code resp_err)
    {
        update_child_group_partition_count_request req;
        mock_update_child_partition_count_request(req, INIT_BALLOT);
        update_child_group_partition_count_response resp;
        resp.err = resp_err;
        auto not_replied_addresses = std::make_shared<std::unordered_set<rpc_address>>();
        not_replied_addresses->insert(PRIMARY);

        _parent->on_update_child_group_partition_count_reply(
            ERR_OK, req, resp, not_replied_addresses);
        _parent->tracker()->wait_outstanding_tasks();
        _child->tracker()->wait_outstanding_tasks();
        return resp.err;
    }

    /// mock functions
    void mock_app_info()
    {
        _app_info.app_id = APP_ID;
        _app_info.app_name = APP_NAME;
        _app_info.app_type = "replica";
        _app_info.is_stateful = true;
        _app_info.max_replica_count = 3;
        _app_info.partition_count = OLD_PARTITION_COUNT;
    }

    void mock_parent_primary_configuration(bool lack_of_secondary = false)
    {
        partition_configuration config;
        config.max_replica_count = 3;
        config.pid = PARENT_GPID;
        config.ballot = INIT_BALLOT;
        config.primary = PRIMARY;
        config.secondaries.emplace_back(SECONDARY);
        if (!lack_of_secondary) {
            config.secondaries.emplace_back(SECONDARY2);
        }
        _parent->set_primary_partition_configuration(config);
    }

    void generate_child(partition_status::type status)
    {
        _child = stub->generate_replica(_app_info, CHILD_GPID, status, INIT_BALLOT);
        _parent->set_split_status(split_status::SPLITTING);
        _parent->set_child_gpid(CHILD_GPID);
        _parent->set_init_child_ballot(INIT_BALLOT);
    }

    void mock_child_split_context(bool is_prepare_list_copied, bool is_caught_up)
    {
        _child->_split_states.parent_gpid = PARENT_GPID;
        _child->_split_states.is_prepare_list_copied = is_prepare_list_copied;
        _child->_split_states.is_caught_up = is_caught_up;
    }

    void
    mock_child_async_learn_states(mock_replica_ptr plist_rep, bool add_to_plog, decree min_decree)
    {
        mock_shared_log();
        mock_private_log(CHILD_GPID, _child, false);
        mock_prepare_list(plist_rep, add_to_plog);
        // mock_learn_state
        _mock_learn_state.to_decree_included = DECREE;
        _mock_learn_state.files.push_back("fake_file_name");
        // mock parent private log files
        _private_log_files.push_back("log.1.0.txt");
        // mock mutation list
        mock_mutation_list(min_decree);
    }

    void mock_shared_log()
    {
        mock_mutation_log_shared_ptr shared_log_mock = new mock_mutation_log_shared("./");
        stub->set_log(shared_log_mock);
    }

    void mock_private_log(gpid pid, mock_replica_ptr rep, bool mock_log_file_flag)
    {
        mock_mutation_log_private_ptr private_log_mock = new mock_mutation_log_private(pid, rep);
        if (mock_log_file_flag) {
            mock_log_file_ptr log_file_mock = new mock_log_file("log.1.0.txt", 0);
            log_file_mock->set_file_size(100);
            private_log_mock->add_log_file(log_file_mock);
        }
        rep->_private_log = private_log_mock;
    }

    void mock_prepare_list(mock_replica_ptr rep, bool add_to_plog)
    {
        _mock_plist = new prepare_list(rep, 1, MAX_COUNT, [](mutation_ptr mu) {});
        for (int i = 1; i < MAX_COUNT + 1; ++i) {
            mutation_ptr mu = new mutation();
            mu->data.header.decree = i;
            mu->data.header.ballot = INIT_BALLOT;
            _mock_plist->put(mu);
            if (add_to_plog) {
                rep->_private_log->append(mu, LPC_WRITE_REPLICATION_LOG_PRIVATE, nullptr, nullptr);
                mu->set_logged();
            }
        }
        rep->_prepare_list = _mock_plist;
    }

    void mock_mutation_list(decree min_decree)
    {
        for (int d = 1; d < MAX_COUNT + 1; ++d) {
            mutation_ptr mu = _mock_plist->get_mutation_by_decree(d);
            if (d > min_decree) {
                _mutation_list.push_back(mu);
            }
        }
    }

    void mock_primary_parent_caught_up_context(bool will_all_caught_up)
    {
        _parent->_primary_states.statuses.clear();
        _parent->_primary_states.caught_up_children.clear();
        _parent->_primary_states.statuses[PRIMARY] = partition_status::PS_PRIMARY;
        _parent->_primary_states.statuses[SECONDARY] = partition_status::PS_SECONDARY;
        _parent->_primary_states.statuses[SECONDARY2] = partition_status::PS_SECONDARY;
        _parent->_primary_states.caught_up_children.insert(SECONDARY);
        if (will_all_caught_up) {
            _parent->_primary_states.caught_up_children.insert(SECONDARY2);
        }
        _parent->_primary_states.sync_send_write_request = false;
    }
    // TODO(heyuchen): refactor
    void mock_primary_update_context(bool has_learner)
    {
        mock_parent_primary_configuration(has_learner);
        _parent->_primary_states.statuses[PRIMARY] = partition_status::PS_PRIMARY;
        _parent->_primary_states.statuses[SECONDARY] = partition_status::PS_SECONDARY;
        _parent->_primary_states.statuses[SECONDARY2] = partition_status::PS_SECONDARY;
        _parent->_primary_states.sync_send_write_request = true;
    }

    void mock_update_child_partition_count_request(update_child_group_partition_count_request &req,
                                                   ballot b)
    {
        req.pid = CHILD_GPID;
        req.ballot = b;
        req.target_address = PRIMARY;
        req.new_partition_count = NEW_PARTITION_COUNT;
    }

    /// helper functions
    void cleanup_prepare_list(mock_replica_ptr rep) { rep->_prepare_list->reset(0); }
    int32_t get_partition_version(mock_replica_ptr rep) { return rep->_partition_version.load(); }

    void parent_cleanup_split_context() { _parent->parent_cleanup_split_context(); }
    bool parent_sync_send_write_request()
    {
        return _parent->_primary_states.sync_send_write_request;
    }

    void cleanup_child_split_context()
    {
        _child->_split_states.cleanup(true);
        _child->tracker()->wait_outstanding_tasks();
    }
    bool child_is_prepare_list_copied() { return _child->_split_states.is_prepare_list_copied; }
    int32_t child_get_prepare_list_count() { return _child->get_plist()->count(); }
    bool child_is_caught_up() { return _child->_split_states.is_caught_up; }

public:
    const std::string APP_NAME = "split_table";
    const int32_t APP_ID = 2;
    const int32_t OLD_PARTITION_COUNT = 8;
    const int32_t NEW_PARTITION_COUNT = 16;
    const rpc_address PRIMARY = rpc_address("127.0.0.1", 18230);
    const rpc_address SECONDARY = rpc_address("127.0.0.2", 10058);
    const rpc_address SECONDARY2 = rpc_address("127.0.0.3", 10805);
    const gpid PARENT_GPID = gpid(APP_ID, 1);
    const gpid CHILD_GPID = gpid(APP_ID, 9);
    const ballot INIT_BALLOT = 3;
    const decree DECREE = 5;
    const int32_t MAX_COUNT = 10;
    const uint64_t TOTAL_FILE_SIZE = 100;

    mock_replica_ptr _parent;
    mock_replica_ptr _child;
    app_info _app_info;
    learn_state _mock_learn_state;
    std::vector<std::string> _private_log_files;
    prepare_list *_mock_plist;
    std::vector<mutation_ptr> _mutation_list;

    register_child_request _register_req;
    // update_group_partition_count_request _update_partition_count_req;
};

// try to start split unit tests
TEST_F(replica_split_test, start_split_test)
{
    fail::cfg("replica_on_add_child", "return()");
    fail::cfg("replica_broadcast_group_check", "return()");

    // Test case:
    // - partition is already splitting
    // - lack of secondary
    // - start split succeed
    struct start_test
    {
        split_status::type status;
        bool lack_of_secondary;
    } tests[] = {{split_status::SPLITTING, false},
                 {split_status::NOT_SPLIT, true},
                 {split_status::NOT_SPLIT, false}};
    for (auto test : tests) {
        parent_cleanup_split_context();
        _parent->set_split_status(test.status);
        mock_parent_primary_configuration(test.lack_of_secondary);
        test_try_to_start_split();
        ASSERT_EQ(_parent->is_splitting(), test.status == split_status::SPLITTING);
    }
}

// add child unit tests
TEST_F(replica_split_test, add_child_test)
{
    fail::cfg("replica_stub_create_child_replica_if_not_found", "return()");
    fail::cfg("replica_child_init_replica", "return()");
    ballot WRONG_BALLOT = 2;

    // Test case:
    // - request has wrong ballot
    // - partition is already splitting
    // - add child succeed
    struct add_child_test
    {
        ballot req_ballot;
        bool is_splitting;
        bool expected_is_splitting;
        bool child_is_nullptr;
    } tests[] = {{WRONG_BALLOT, false, false, true},
                 {INIT_BALLOT, true, true, true},
                 {INIT_BALLOT, false, true, false}};
    for (auto test : tests) {
        parent_cleanup_split_context();
        test_on_add_child(test.req_ballot, test.is_splitting);
        ASSERT_EQ(_parent->is_splitting(), test.expected_is_splitting);
        ASSERT_EQ(stub->get_replica(CHILD_GPID) == nullptr, test.child_is_nullptr);
        if (!test.child_is_nullptr) {
            stub->get_replica(CHILD_GPID)->tracker()->wait_outstanding_tasks();
        }
    }
}

// TODO(heyuchen): refactor this unit test
TEST_F(replica_split_test, parent_check_states_with_wrong_status)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    _parent->set_partition_status(partition_status::PS_POTENTIAL_SECONDARY);

    fail::setup();
    fail::cfg("replica_stub_split_replica_exec", "return()");
    bool flag = test_parent_check_states();
    ASSERT_FALSE(flag);
    fail::teardown();
}

// TODO(heyuchen): refactor this unit test
TEST_F(replica_split_test, parent_check_states)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    bool flag = test_parent_check_states();
    ASSERT_TRUE(flag);
}

TEST_F(replica_split_test, copy_prepare_list_succeed)
{
    fail::cfg("replica_stub_split_replica_exec", "return()");
    fail::cfg("replica_child_learn_states", "return()");

    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(false, false);

    test_child_copy_prepare_list();
    ASSERT_TRUE(child_is_prepare_list_copied());
    ASSERT_EQ(child_get_prepare_list_count(), MAX_COUNT);

    cleanup_prepare_list(_parent);
    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, learn_states_with_replay_private_log_error)
{
    fail::cfg("replica_child_apply_private_logs", "return(ERR_INVALID_STATE)");
    fail::cfg("replica_child_catch_up_states", "return()");
    fail::cfg("replica_stub_split_replica_exec", "return()");

    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(true, false);

    test_child_learn_states();
    ASSERT_EQ(_child->status(), partition_status::PS_ERROR);

    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, learn_states_succeed)
{
    fail::cfg("replica_child_apply_private_logs", "return()");
    fail::cfg("replica_child_catch_up_states", "return()");

    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(true, false);

    test_child_learn_states();
    ASSERT_EQ(_child->status(), partition_status::PS_PARTITION_SPLIT);

    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, child_apply_private_logs_succeed)
{
    fail::cfg("mutation_log_replay_succeed", "return()");
    fail::cfg("replication_app_base_apply_mutation", "return()");

    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(true, false);

    test_child_apply_private_logs();
    ASSERT_EQ(child_get_prepare_list_count(), MAX_COUNT);

    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, catch_up_succeed_with_all_states_learned)
{
    fail::cfg("replica_child_notify_catch_up", "return()");

    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(true, false);

    test_child_catch_up_states(DECREE, DECREE, DECREE);
    ASSERT_TRUE(child_is_caught_up());

    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, catch_up_succeed_with_learn_in_memory_mutations)
{
    fail::cfg("replica_child_notify_catch_up", "return()");
    fail::cfg("replication_app_base_apply_mutation", "return()");

    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(true, false);

    test_child_catch_up_states(DECREE, MAX_COUNT - 1, 1);
    ASSERT_TRUE(child_is_caught_up());

    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, parent_handle_catch_up_test)
{
    fail::cfg("replica_parent_check_sync_point_commit", "return()");
    ballot WRONG_BALLOT = 1;

    // Test case:
    // - request has wrong ballot
    // - not all child caught up
    // - all child caught up
    struct add_child_test
    {
        ballot req_ballot;
        bool all_caught_up;
        error_code expected_err;
        bool sync_send_write_request;
    } tests[] = {{WRONG_BALLOT, false, ERR_INVALID_STATE, false},
                 {INIT_BALLOT, false, ERR_OK, false},
                 {INIT_BALLOT, true, ERR_OK, true}};
    for (auto test : tests) {
        mock_primary_parent_caught_up_context(test.all_caught_up);
        ASSERT_EQ(test_parent_handle_child_catch_up(test.req_ballot), test.expected_err);
        ASSERT_EQ(parent_sync_send_write_request(), test.sync_send_write_request);
    }
}

TEST_F(replica_split_test, update_child_group_partition_count_has_learner)
{
    fail::cfg("replica_parent_update_partition_count_request", "return()");
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_primary_update_context(true);
    test_update_child_group_partition_count();
    ASSERT_EQ(_child->status(), partition_status::PS_ERROR);
    ASSERT_FALSE(parent_sync_send_write_request());
    ASSERT_TRUE(is_parent_not_in_split());
}

TEST_F(replica_split_test, update_child_group_partition_count_test)
{
    fail::cfg("replica_parent_update_partition_count_request", "return()");
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_primary_update_context(false);
    test_update_child_group_partition_count();
    ASSERT_TRUE(parent_sync_send_write_request());
}

TEST_F(replica_split_test, child_update_partition_count_test)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    ballot WRONG_BALLOT = INIT_BALLOT + 1;

    // Test case:
    // - request has wrong ballot
    // - not catch up
    // - update succeed
    struct update_partition_count_test
    {
        ballot req_ballot;
        bool caught_up;
        error_code expected_err;
        int32_t expected_partition_version;
    } tests[] = {{WRONG_BALLOT, true, ERR_VERSION_OUTDATED, OLD_PARTITION_COUNT - 1},
                 {INIT_BALLOT, false, ERR_VERSION_OUTDATED, OLD_PARTITION_COUNT - 1},
                 {INIT_BALLOT, true, ERR_OK, NEW_PARTITION_COUNT - 1}};
    for (auto test : tests) {
        mock_child_split_context(true, test.caught_up);
        ASSERT_EQ(get_partition_version(_child), OLD_PARTITION_COUNT - 1);
        ASSERT_EQ(test_child_update_group_partition_count(test.req_ballot), test.expected_err);
        ASSERT_EQ(get_partition_version(_child), test.expected_partition_version);
    }
}

TEST_F(replica_split_test, parent_update_child_partition_count_with_outdated)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(true, true);
    test_on_update_child_group_partition_count(ERR_VERSION_OUTDATED);
    ASSERT_EQ(_child->status(), partition_status::PS_ERROR);
    ASSERT_TRUE(is_parent_not_in_split());
}

TEST_F(replica_split_test, parent_update_child_partition_count)
{
    fail::cfg("replica_register_child_on_meta", "return()");
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(true, true);
    test_on_update_child_group_partition_count(ERR_OK);
    ASSERT_FALSE(is_parent_not_in_split());
}

TEST_F(replica_split_test, register_child_test)
{
    fail::cfg("replica_parent_send_register_request", "return()");

    test_register_child_on_meta();
    ASSERT_EQ(_parent->status(), partition_status::PS_INACTIVE);
    ASSERT_EQ(get_partition_version(_parent), -1);
}

// TODO(heyuchen): refacor register child reply

TEST_F(replica_split_test, register_child_reply_with_wrong_status)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(true, true);

    test_on_register_child_rely(partition_status::PS_PRIMARY, ERR_OK);
    primary_context parent_primary_states = get_replica_primary_context(_parent);
    ASSERT_EQ(parent_primary_states.register_child_task, nullptr);
}

TEST_F(replica_split_test, register_child_reply_with_child_registered)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(true, true);

    test_on_register_child_rely(partition_status::PS_INACTIVE, ERR_CHILD_REGISTERED);

    primary_context parent_primary_states = get_replica_primary_context(_parent);
    ASSERT_EQ(parent_primary_states.register_child_task, nullptr);
    // ASSERT_TRUE(is_parent_not_in_split());
}

TEST_F(replica_split_test, register_child_reply_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(true, true);

    fail::setup();
    fail::cfg("replica_stub_split_replica_exec", "return()");
    fail::cfg("replica_update_group_partition_count", "return()");
    test_on_register_child_rely(partition_status::PS_INACTIVE, ERR_OK);
    fail::teardown();

    // ASSERT_TRUE(is_parent_not_in_split());
}

} // namespace replication
} // namespace dsn
