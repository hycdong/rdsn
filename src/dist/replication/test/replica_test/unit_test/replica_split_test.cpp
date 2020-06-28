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

    void mock_notify_catch_up_request()
    {
        _parent->set_child_gpid(CHILD_GPID);
        _catch_up_req.child_gpid = CHILD_GPID;
        _catch_up_req.parent_gpid = PARENT_GPID;
        _catch_up_req.child_ballot = INIT_BALLOT;
        _catch_up_req.child_address = dsn::rpc_address("127.0.0.1", 1);
    }

    void mock_register_child_request()
    {
        partition_configuration &p_config = _register_req.parent_config;
        p_config.pid = PARENT_GPID;
        p_config.ballot = INIT_BALLOT;
        p_config.last_committed_decree = _decree;

        partition_configuration &c_config = _register_req.child_config;
        c_config.pid = CHILD_GPID;
        c_config.ballot = INIT_BALLOT + 1;
        c_config.last_committed_decree = 0;

        _register_req.app = _app_info;
        _register_req.primary_address = dsn::rpc_address("127.0.0.1", 10086);
    }

    void mock_update_group_partition_count_request(gpid pid, ballot b)
    {
        _update_partition_count_req.pid = pid;
        _update_partition_count_req.ballot = b;
        _update_partition_count_req.target_address = dsn::rpc_address("127.0.0.1", 1);
        _update_partition_count_req.new_partition_count = NEW_PARTITION_COUNT;
        _update_partition_count_req.update_child_group = (pid == CHILD_GPID);
    }

    void generate_child(partition_status::type status)
    {
        _child = stub->generate_replica(_app_info, CHILD_GPID, status, INIT_BALLOT);
        _parent->set_is_splitting(true);
        _parent->set_child_gpid(CHILD_GPID);
        _parent->set_init_child_ballot(INIT_BALLOT);
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
        _mock_plist = new prepare_list(rep, 1, _max_count, [](mutation_ptr mu) {});
        for (int i = 1; i < _max_count + 1; ++i) {
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

    void mock_parent_states()
    {
        mock_shared_log();
        mock_private_log(PARENT_GPID, _parent, true);
        mock_prepare_list(_parent, true);
    }

    void mock_child_split_context(gpid parent_gpid, bool is_prepare_list_copied, bool is_caught_up)
    {
        _child->_split_states.parent_gpid = parent_gpid;
        _child->_split_states.is_prepare_list_copied = is_prepare_list_copied;
        _child->_split_states.is_caught_up = is_caught_up;
    }

    void mock_mutation_list(decree min_decree)
    {
        // mock mutation list
        for (int d = 1; d < _max_count; ++d) {
            mutation_ptr mu = _mock_plist->get_mutation_by_decree(d);
            if (d > min_decree) {
                _mutation_list.push_back(mu);
            }
        }
    }

    void mock_parent_primary_context(bool will_all_caught_up)
    {
        _parent->_primary_states.statuses[dsn::rpc_address("127.0.0.1", 1)] =
            partition_status::PS_PRIMARY;
        _parent->_primary_states.statuses[dsn::rpc_address("127.0.0.1", 2)] =
            partition_status::PS_SECONDARY;
        _parent->_primary_states.statuses[dsn::rpc_address("127.0.0.1", 3)] =
            partition_status::PS_SECONDARY;
        _parent->_primary_states.caught_up_children.insert(dsn::rpc_address("127.0.0.1", 2));
        if (will_all_caught_up) {
            _parent->_primary_states.caught_up_children.insert(dsn::rpc_address("127.0.0.1", 3));
        }
        _parent->_primary_states.sync_send_write_request = false;
    }

    bool get_sync_send_write_request() { return _parent->_primary_states.sync_send_write_request; }

    void
    mock_child_async_learn_states(mock_replica_ptr plist_rep, bool add_to_plog, decree min_decree)
    {
        mock_shared_log();
        mock_private_log(CHILD_GPID, _child, false);
        mock_prepare_list(plist_rep, add_to_plog);
        // mock_learn_state
        _mock_learn_state.to_decree_included = _decree;
        _mock_learn_state.files.push_back("fake_file_name");
        // mock parent private log files
        _private_log_files.push_back("log.1.0.txt");
        // mock mutation list
        mock_mutation_list(min_decree);
    }

    void cleanup_prepare_list(mock_replica_ptr rep) { rep->_prepare_list->reset(0); }

    void cleanup_child_split_context()
    {
        _child->_split_states.cleanup(true);
        _child->tracker()->wait_outstanding_tasks();
    }

    partition_split_context get_split_context() { return _child->_split_states; }

    primary_context get_replica_primary_context(mock_replica_ptr rep)
    {
        return rep->_primary_states;
    }

    bool is_parent_not_in_split() { return (_parent->_child_gpid.get_app_id() == 0); }

    int32_t get_partition_version(mock_replica_ptr rep) { return rep->_partition_version.load(); }

    /// test functions
    void test_try_to_start_split()
    {
        _parent->try_to_start_split(NEW_PARTITION_COUNT);
        _parent->tracker()->wait_outstanding_tasks();
    }

    void test_on_add_child(ballot b, bool is_splitting)
    {
        _parent->set_is_splitting(is_splitting);

        group_check_request req;
        req.child_gpid = CHILD_GPID;
        req.config.ballot = b;
        req.config.status = partition_status::PS_PRIMARY;
        _parent->on_add_child(req);
        _parent->tracker()->wait_outstanding_tasks();
    }

    bool test_parent_check_states() { return _parent->parent_check_states(); }

    void test_child_copy_prepare_list()
    {
        mock_child_async_learn_states(_parent, false, _decree);
        std::shared_ptr<prepare_list> plist = std::make_shared<prepare_list>(_parent, *_mock_plist);
        _child->child_copy_prepare_list(_mock_learn_state,
                                        _mutation_list,
                                        _private_log_files,
                                        _total_file_size,
                                        std::move(plist));
        _child->tracker()->wait_outstanding_tasks();
    }

    void test_child_learn_states()
    {
        mock_child_async_learn_states(_child, true, _decree);
        _child->child_learn_states(
            _mock_learn_state, _mutation_list, _private_log_files, _total_file_size, _decree);
        _child->tracker()->wait_outstanding_tasks();
    }

    void test_child_apply_private_logs()
    {
        mock_child_async_learn_states(_child, true, 0);
        _child->child_apply_private_logs(
            _private_log_files, _mutation_list, _total_file_size, _decree);
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

    dsn::error_code test_parent_handle_child_catch_up()
    {
        notify_cacth_up_response resp;
        _parent->parent_handle_child_catch_up(_catch_up_req, resp);
        _parent->tracker()->wait_outstanding_tasks();
        return resp.err;
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

    error_code test_on_update_group_partition_count(mock_replica_ptr &rep, ballot b)
    {
        mock_update_group_partition_count_request(rep->get_gpid(), b);

        update_group_partition_count_response resp;
        rep->on_update_group_partition_count(_update_partition_count_req, resp);
        if (_update_partition_count_req.update_child_group) {
            _child->tracker()->wait_outstanding_tasks();
        }
        _parent->tracker()->wait_outstanding_tasks();
        return resp.err;
    }

    int32_t test_on_update_group_partition_count_reply(error_code resp_err, gpid &pid)
    {
        _parent->_primary_states.sync_send_write_request = (pid == PARENT_GPID);
        mock_update_group_partition_count_request(pid, INIT_BALLOT);
        update_group_partition_count_response resp;
        resp.err = resp_err;

        auto not_replied_address = std::make_shared<std::unordered_set<rpc_address>>();
        not_replied_address->insert(dsn::rpc_address("127.0.0.1", 1));
        _parent->on_update_group_partition_count_reply(
            ERR_OK, _update_partition_count_req, resp, not_replied_address);
        _parent->tracker()->wait_outstanding_tasks();
        if (_update_partition_count_req.update_child_group) {
            _child->tracker()->wait_outstanding_tasks();
        }

        return not_replied_address->size();
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
        if (lack_of_secondary) {
            config.secondaries.emplace_back(SECONDARY2);
        }
        _parent->set_primary_partition_configuration(config);
    }

    /// helper functions
    void parent_cleanup_split_context() { _parent->parent_cleanup_split_context(); }

public:
    mock_replica_ptr _parent;
    mock_replica_ptr _child;

    app_info _app_info;

    decree _decree = 5;

    std::string APP_NAME = "split_table";
    int32_t APP_ID = 2;
    int32_t OLD_PARTITION_COUNT = 8;
    int32_t NEW_PARTITION_COUNT = 16;
    rpc_address PRIMARY = rpc_address("127.0.0.1", 18230);
    rpc_address SECONDARY = rpc_address("127.0.0.2", 10058);
    rpc_address SECONDARY2 = rpc_address("127.0.0.3", 10805);
    gpid PARENT_GPID = gpid(APP_ID, 1);
    gpid CHILD_GPID = gpid(APP_ID, 9);
    ballot INIT_BALLOT = 3;

    notify_catch_up_request _catch_up_req;
    register_child_request _register_req;
    update_group_partition_count_request _update_partition_count_req;
    std::vector<std::string> _private_log_files;
    std::vector<mutation_ptr> _mutation_list;
    const uint32_t _max_count = 10;
    prepare_list *_mock_plist;
    const uint64_t _total_file_size = 100;
    learn_state _mock_learn_state;
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
        bool is_splitting;
        bool lack_of_secondary;
    } tests[] = {{true, false}, {false, true}, {false, false}};
    for (auto test : tests) {
        parent_cleanup_split_context();
        _parent->set_is_splitting(test.is_splitting);
        mock_parent_primary_configuration(test.lack_of_secondary);
        test_try_to_start_split();
        ASSERT_EQ(_parent->is_splitting(), test.is_splitting);
    }
}

// add child unit tests
TEST_F(replica_split_test, add_child_test)
{
    fail::cfg("replica_stub_create_child_replica_if_not_found", "return()");
    fail::cfg("replica_child_init_replica", "return()");
    fail::cfg("replica_child_check_split_context", "return()");
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

TEST_F(replica_split_test, parent_check_states)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    bool flag = test_parent_check_states();
    ASSERT_TRUE(flag);
}

TEST_F(replica_split_test, copy_prepare_list_with_wrong_status)
{
    generate_child(partition_status::PS_INACTIVE);
    mock_child_split_context(PARENT_GPID, false, false);

    fail::setup();
    fail::cfg("replica_stub_split_replica_exec", "return()");
    test_child_copy_prepare_list();
    fail::teardown();

    cleanup_prepare_list(_parent);
    // TODO(heyuchen): child should be equal to error(after implement child_handle_split_error)
}

TEST_F(replica_split_test, copy_prepare_list_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, false, false);

    fail::setup();
    fail::cfg("replica_stub_split_replica_exec", "return()");
    fail::cfg("replica_child_learn_states", "return()");
    test_child_copy_prepare_list();
    fail::teardown();

    partition_split_context split_context = get_split_context();
    ASSERT_EQ(split_context.is_prepare_list_copied, true);
    ASSERT_EQ(_child->get_plist()->count(), _max_count);

    cleanup_prepare_list(_parent);
    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, learn_states_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, true, false);

    fail::setup();
    fail::cfg("replica_child_apply_private_logs", "return()");
    fail::cfg("replica_child_catch_up_states", "return()");
    test_child_learn_states();
    fail::teardown();

    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, learn_states_with_replay_private_log_error)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, true, false);

    fail::setup();
    fail::cfg("replica_child_apply_private_logs", "return(error)");
    fail::cfg("replica_child_catch_up_states", "return()");
    test_child_learn_states();
    fail::teardown();

    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, child_apply_private_logs_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, true, false);

    fail::setup();
    fail::cfg("mutation_log_replay_succeed", "return()");
    fail::cfg("replication_app_base_apply_mutation", "return()");
    test_child_apply_private_logs();
    fail::teardown();

    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, catch_up_succeed_with_all_states_learned)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, true, false);

    fail::setup();
    fail::cfg("replica_child_notify_catch_up", "return()");
    test_child_catch_up_states(_decree, _decree, _decree);
    fail::teardown();

    partition_split_context split_context = get_split_context();
    ASSERT_EQ(split_context.is_caught_up, true);

    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, catch_up_succeed_with_learn_in_memory_mutations)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, true, false);

    fail::setup();
    fail::cfg("replica_child_notify_catch_up", "return()");
    fail::cfg("replication_app_base_apply_mutation", "return()");
    test_child_catch_up_states(_decree, _max_count - 1, 1);
    fail::teardown();

    partition_split_context split_context = get_split_context();
    ASSERT_EQ(split_context.is_caught_up, true);

    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, handle_catch_up_with_ballot_wrong)
{
    mock_notify_catch_up_request();
    _catch_up_req.child_ballot = 1;

    fail::setup();
    fail::cfg("replica_parent_check_sync_point_commit", "return()");
    dsn::error_code err = test_parent_handle_child_catch_up();
    fail::teardown();

    ASSERT_EQ(err, ERR_INVALID_STATE);
}

TEST_F(replica_split_test, handle_catch_up_with_not_all_caught_up)
{
    mock_parent_primary_context(false);
    mock_notify_catch_up_request();

    fail::setup();
    fail::cfg("replica_parent_check_sync_point_commit", "return()");
    dsn::error_code err = test_parent_handle_child_catch_up();
    fail::teardown();

    ASSERT_EQ(err, ERR_OK);
    ASSERT_FALSE(get_sync_send_write_request());
}

TEST_F(replica_split_test, handle_catch_up_with_all_caught_up)
{
    mock_parent_primary_context(true);
    mock_notify_catch_up_request();

    fail::setup();
    fail::cfg("replica_parent_check_sync_point_commit", "return()");
    dsn::error_code err = test_parent_handle_child_catch_up();
    fail::teardown();

    ASSERT_EQ(err, ERR_OK);
    ASSERT_TRUE(get_sync_send_write_request());
}

TEST_F(replica_split_test, register_child_test)
{
    fail::setup();
    fail::cfg("replica_parent_send_register_request", "return()");
    test_register_child_on_meta();
    fail::teardown();

    ASSERT_EQ(_parent->status(), partition_status::PS_INACTIVE);
    ASSERT_EQ(get_partition_version(_parent), -1);
}

TEST_F(replica_split_test, register_child_reply_with_wrong_status)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, true, true);

    test_on_register_child_rely(partition_status::PS_PRIMARY, ERR_OK);
    primary_context parent_primary_states = get_replica_primary_context(_parent);
    ASSERT_EQ(parent_primary_states.register_child_task, nullptr);
}

TEST_F(replica_split_test, register_child_reply_with_child_registered)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, true, true);

    test_on_register_child_rely(partition_status::PS_INACTIVE, ERR_CHILD_REGISTERED);

    primary_context parent_primary_states = get_replica_primary_context(_parent);
    ASSERT_EQ(parent_primary_states.register_child_task, nullptr);
    // ASSERT_TRUE(is_parent_not_in_split());
}

TEST_F(replica_split_test, register_child_reply_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, true, true);

    fail::setup();
    fail::cfg("replica_stub_split_replica_exec", "return()");
    fail::cfg("replica_update_group_partition_count", "return()");
    test_on_register_child_rely(partition_status::PS_INACTIVE, ERR_OK);
    fail::teardown();

    // ASSERT_TRUE(is_parent_not_in_split());
}

TEST_F(replica_split_test, child_update_count_with_not_caught_up)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, true, false);

    error_code err = test_on_update_group_partition_count(_child, INIT_BALLOT);
    ASSERT_EQ(err, ERR_VERSION_OUTDATED);
    ASSERT_TRUE(is_parent_not_in_split());
}

TEST_F(replica_split_test, child_update_count_with_wrong_ballot)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, true, true);

    error_code err = test_on_update_group_partition_count(_child, INIT_BALLOT - 1);
    ASSERT_EQ(err, ERR_VERSION_OUTDATED);
    ASSERT_TRUE(is_parent_not_in_split());
}

TEST_F(replica_split_test, child_update_count_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, true, true);

    error_code err = test_on_update_group_partition_count(_child, INIT_BALLOT);
    ASSERT_EQ(err, ERR_OK);
    ASSERT_EQ(get_partition_version(_child), NEW_PARTITION_COUNT - 1);
}

TEST_F(replica_split_test, parent_update_count_with_wrong_ballot)
{
    error_code err = test_on_update_group_partition_count(_parent, INIT_BALLOT - 1);
    ASSERT_EQ(err, ERR_VERSION_OUTDATED);
    ASSERT_EQ(_parent->status(), partition_status::PS_ERROR);
}

TEST_F(replica_split_test, parent_update_count_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(PARENT_GPID, true, true);

    error_code err = test_on_update_group_partition_count(_parent, INIT_BALLOT);
    ASSERT_EQ(err, ERR_OK);
    ASSERT_EQ(get_partition_version(_parent), NEW_PARTITION_COUNT - 1);
    ASSERT_TRUE(is_parent_not_in_split());
}

TEST_F(replica_split_test, update_count_with_child_not_found)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);

    int32_t not_replied_size =
        test_on_update_group_partition_count_reply(ERR_OBJECT_NOT_FOUND, CHILD_GPID);
    ASSERT_EQ(not_replied_size, 1);
    ASSERT_TRUE(is_parent_not_in_split());
}

TEST_F(replica_split_test, update_child_group_count_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);

    fail::setup();
    fail::cfg("replica_parent_send_register_request", "return()");
    int32_t not_replied_size = test_on_update_group_partition_count_reply(ERR_OK, CHILD_GPID);
    fail::teardown();

    ASSERT_EQ(not_replied_size, 0);
    ASSERT_EQ(get_partition_version(_parent), -1);
}

TEST_F(replica_split_test, update_parent_group_count_succeed)
{
    int32_t not_replied_size = test_on_update_group_partition_count_reply(ERR_OK, PARENT_GPID);
    ASSERT_EQ(not_replied_size, 0);
    ASSERT_FALSE(get_sync_send_write_request());
}

} // namespace replication
} // namespace dsn
