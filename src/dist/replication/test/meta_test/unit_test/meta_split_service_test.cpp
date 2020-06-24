// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "meta_service_test_app.h"
#include "meta_test_base.h"

#include <dsn/utility/fail_point.h>
#include <gtest/gtest.h>

namespace dsn {
namespace replication {
class meta_split_service_test : public meta_test_base
{
public:
    void SetUp()
    {
        meta_test_base::SetUp();
        create_app(NAME, PARTITION_COUNT);
        app = find_app(NAME);
    }

    void TearDown()
    {
        app.reset();
        meta_test_base::TearDown();
    }

    error_code start_partition_split(const std::string &app_name, int new_partition_count)
    {
        auto request = dsn::make_unique<start_partition_split_request>();
        request->app_name = app_name;
        request->new_partition_count = new_partition_count;

        start_split_rpc rpc(std::move(request), RPC_CM_START_PARTITION_SPLIT);
        split_svc().start_partition_split(rpc);
        wait_all();
        return rpc.response().err;
    }

    query_split_response query_partition_split(const std::string &app_name)
    {
        auto request = dsn::make_unique<query_split_request>();
        request->app_name = app_name;

        query_split_rpc rpc(std::move(request), RPC_CM_QUERY_PARTITION_SPLIT);
        split_svc().query_partition_split(rpc);
        wait_all();
        return rpc.response();
    }

    error_code pause_partition_split(const std::string &app_name, const int32_t pidx)
    {
        return control_partition_split(app_name, split_control_type::PSC_PAUSE, pidx);
    }

    error_code restart_partition_split(const std::string &app_name, const int32_t pidx)
    {
        return control_partition_split(app_name, split_control_type::PSC_RESTART, pidx);
    }

    error_code cancel_partition_split(const std::string &app_name,
                                      const int32_t old_partition_count = 0)
    {
        return control_partition_split(
            app_name, split_control_type::PSC_CANCEL, -1, old_partition_count);
    }

    error_code control_partition_split(const std::string &app_name,
                                       split_control_type::type type,
                                       const int32_t pidx,
                                       const int32_t old_partition_count = 0)
    {
        auto req = make_unique<control_split_request>();
        req->__set_app_name(app_name);
        req->__set_control_type(type);
        req->__set_parent_pidx(pidx);
        req->__set_old_partition_count(old_partition_count);

        control_split_rpc rpc(std::move(req), RPC_CM_CONTROL_PARTITION_SPLIT);
        split_svc().control_partition_split(rpc);
        wait_all();

        return rpc.response().err;
    }

    error_code register_child(int32_t parent_index, ballot req_parent_ballot, bool wait_zk)
    {
        partition_configuration parent_config;
        parent_config.ballot = req_parent_ballot;
        parent_config.last_committed_decree = 5;
        parent_config.max_replica_count = 3;
        parent_config.pid = dsn::gpid(app->app_id, parent_index);

        partition_configuration child_config;
        child_config.ballot = PARENT_BALLOT + 1;
        child_config.last_committed_decree = 5;
        child_config.pid = dsn::gpid(app->app_id, parent_index + PARTITION_COUNT);

        // mock node state
        node_state node;
        node.put_partition(dsn::gpid(app->app_id, PARENT_INDEX), true);
        mock_node_state(dsn::rpc_address("127.0.0.1", 10086), node);

        auto request = dsn::make_unique<register_child_request>();
        request->app.app_name = app->app_name;
        request->app.app_id = app->app_id;
        request->parent_config = parent_config;
        request->child_config = child_config;
        request->primary_address = dsn::rpc_address("127.0.0.1", 10086);

        register_child_rpc rpc(std::move(request), RPC_CM_REGISTER_CHILD_REPLICA);
        split_svc().register_child_on_meta(rpc);
        wait_all();
        if (wait_zk) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return rpc.response().err;
    }

    void mock_app_partition_split_context()
    {
        app->partition_count = NEW_PARTITION_COUNT;
        app->partitions.resize(app->partition_count);
        app->helpers->contexts.resize(app->partition_count);
        app->helpers->split_states.splitting_count = app->partition_count / 2;
        for (int i = 0; i < app->partition_count; ++i) {
            app->helpers->contexts[i].config_owner = &app->partitions[i];
            app->partitions[i].pid = dsn::gpid(app->app_id, i);
            if (i >= app->partition_count / 2) {
                app->partitions[i].ballot = invalid_ballot;
            } else {
                app->partitions[i].ballot = PARENT_BALLOT;
                app->helpers->contexts[i].stage = config_status::not_pending;
                app->helpers->split_states.status[i] = split_status::splitting;
            }
        }
    }

    void mock_child_registered()
    {
        app->partitions[CHILD_INDEX].ballot = PARENT_BALLOT;
        app->helpers->split_states.splitting_count--;
        app->helpers->split_states.status.erase(PARENT_INDEX);
    }

    void mock_split_states(split_status::type status, int32_t parent_index = -1)
    {
        if (parent_index != -1) {
            app->helpers->split_states.status[parent_index] = status;
        } else {
            auto partition_count = app->partition_count;
            for (auto i = 0; i < partition_count / 2; ++i) {
                app->helpers->split_states.status[i] = status;
            }
        }
    }

    bool check_split_status(split_status::type status, int32_t parent_index = -1)
    {
        auto app = find_app(NAME);
        if (parent_index != -1) {
            return (app->helpers->split_states.status[parent_index] == status);
        } else {
            for (const auto kv : app->helpers->split_states.status) {
                if (kv.second != status) {
                    return false;
                }
            }
            return true;
        }
    }

    const std::string NAME = "split_table";
    const int32_t PARTITION_COUNT = 4;
    const int32_t NEW_PARTITION_COUNT = 8;
    const int32_t PARENT_BALLOT = 3;
    const int32_t PARENT_INDEX = 0;
    const int32_t CHILD_INDEX = 4;
    std::shared_ptr<app_state> app;
};

// start split unit tests
TEST_F(meta_split_service_test, start_split_test)
{
    // Test case:
    // - app not existed
    // - wrong partition_count
    // - app already splitting
    // - start split succeed
    struct start_test
    {
        std::string app_name;
        int32_t new_partition_count;
        bool need_mock_splitting;
        error_code expected_err;
        int32_t expected_partition_count;
    } tests[] = {{"table_not_exist", PARTITION_COUNT, false, ERR_APP_NOT_EXIST, PARTITION_COUNT},
                 {NAME, PARTITION_COUNT, false, ERR_INVALID_PARAMETERS, PARTITION_COUNT},
                 {NAME, NEW_PARTITION_COUNT, true, ERR_BUSY, PARTITION_COUNT},
                 {NAME, NEW_PARTITION_COUNT, false, ERR_OK, NEW_PARTITION_COUNT}};

    for (auto test : tests) {
        auto app = find_app(NAME);
        app->helpers->split_states.splitting_count = test.need_mock_splitting ? PARTITION_COUNT : 0;
        ASSERT_EQ(start_partition_split(test.app_name, test.new_partition_count),
                  test.expected_err);
        ASSERT_EQ(app->partition_count, test.expected_partition_count);
    }
}

// query split unit tests
TEST_F(meta_split_service_test, query_split_with_not_existed_app)
{
    auto resp = query_partition_split("table_not_exist");
    ASSERT_EQ(resp.err, ERR_APP_NOT_EXIST);
}

TEST_F(meta_split_service_test, query_split_with_app_not_splitting)
{
    auto resp = query_partition_split(NAME);
    ASSERT_EQ(resp.err, ERR_INVALID_STATE);
}

TEST_F(meta_split_service_test, query_split_succeed)
{
    mock_app_partition_split_context();
    auto resp = query_partition_split(NAME);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.new_partition_count, NEW_PARTITION_COUNT);
    ASSERT_EQ(resp.status.size(), PARTITION_COUNT);
}

// pause split unit tests
TEST_F(meta_split_service_test, pause_split_test)
{
    fail::setup();
    fail::cfg("meta_split_send_stop_split_request", "return()");

    // Test case:
    // - app not existed
    // - app is not splitting
    // - pause singe partition whose child registered
    // - pause singe partition who has already paused
    // - pause singe partition succeed
    // - pause all splitting partitions succeed
    struct pause_test
    {
        std::string app_name;
        int32_t parent_pidx;
        bool mock_app_splitting;
        bool mock_child_registered;
        bool mock_parent_paused;
        error_code expected_err;
        bool check_status;
    } tests[] = {
        {"table_not_exist", -1, false, false, false, ERR_APP_NOT_EXIST, false},
        {NAME, PARENT_INDEX, false, false, false, ERR_INVALID_STATE, false},
        {NAME, PARENT_INDEX, true, true, false, ERR_CHILD_REGISTERED, false},
        {NAME, PARENT_INDEX, true, false, true, ERR_OK, false},
        {NAME, PARENT_INDEX, true, false, false, ERR_OK, true},
        {NAME, -1, true, false, false, ERR_OK, true},
    };

    for (auto test : tests) {
        if (test.mock_app_splitting) {
            mock_app_partition_split_context();
        }
        if (test.mock_child_registered) {
            mock_child_registered();
        }
        if (test.mock_parent_paused) {
            mock_split_states(split_status::paused, test.parent_pidx);
        }
        ASSERT_EQ(pause_partition_split(test.app_name, test.parent_pidx), test.expected_err);
        if (test.check_status) {
            ASSERT_TRUE(check_split_status(split_status::paused, test.parent_pidx));
        }
    }
    fail::teardown();
}

// restart split unit tests
TEST_F(meta_split_service_test, restart_split_test)
{
    // Test case:
    // - invalid parent index
    // - restart singe partition whose child registered
    // - restart singe partition succeed
    // - restart all paused partitions succeed
    struct restart_test
    {
        int32_t parent_pidx;
        bool mock_child_registered;
        bool mock_parent_paused;
        error_code expected_err;
        bool check_status;
    } tests[] = {
        {NEW_PARTITION_COUNT, false, false, ERR_INVALID_PARAMETERS, false},
        {PARENT_INDEX, true, false, ERR_INVALID_STATE, false},
        {PARENT_INDEX, false, true, ERR_OK, true},
        {-1, true, true, ERR_OK, true},
    };

    for (auto test : tests) {
        mock_app_partition_split_context();
        if (test.mock_child_registered) {
            mock_child_registered();
        }
        if (test.mock_parent_paused) {
            mock_split_states(split_status::paused, test.parent_pidx);
        }
        ASSERT_EQ(restart_partition_split(NAME, test.parent_pidx), test.expected_err);
        if (test.check_status) {
            ASSERT_TRUE(check_split_status(split_status::splitting, test.parent_pidx));
        }
    }
}

// cancel split unit tests
TEST_F(meta_split_service_test, cancel_split_test)
{
    fail::setup();
    fail::cfg("meta_split_send_stop_split_request", "return()");

    // Test case:
    // - wrong partition count
    // - cancel split with child registered
    // - cancel succeed
    struct cancel_test
    {
        int32_t old_partition_count;
        bool mock_child_registered;
        error_code expected_err;
        bool check_status;
    } tests[] = {{NEW_PARTITION_COUNT, false, ERR_INVALID_PARAMETERS, false},
                 {PARTITION_COUNT, true, ERR_CHILD_REGISTERED, false},
                 {PARTITION_COUNT, false, ERR_OK, true}};

    for (auto test : tests) {
        mock_app_partition_split_context();
        if (test.mock_child_registered) {
            mock_child_registered();
        }
        ASSERT_EQ(cancel_partition_split(NAME, test.old_partition_count), test.expected_err);
        if (test.check_status) {
            auto app = find_app(NAME);
            ASSERT_EQ(app->partition_count, test.old_partition_count);
            ASSERT_EQ(app->helpers->split_states.splitting_count, 0);
        }
    }
    fail::teardown();
}

TEST_F(meta_split_service_test, register_child_test)
{
    // Test case:
    // - request is out-dated
    // - child has been registered
    // - parent partition has been paused splitting
    // - parent partition is sync config to remote storage
    // - register child succeed
    struct register_test
    {
        int32_t parent_ballot;
        bool mock_child_registered;
        bool mock_parent_paused;
        bool mock_pending;
        error_code expected_err;
        bool wait_zk;
    } tests[] = {
        {PARENT_BALLOT - 1, false, false, false, ERR_INVALID_VERSION, false},
        {PARENT_BALLOT, true, false, false, ERR_CHILD_REGISTERED, false},
        {PARENT_BALLOT, false, true, false, ERR_INVALID_STATE, false},
        {PARENT_BALLOT, false, false, true, ERR_IO_PENDING, false},
        {PARENT_BALLOT, false, false, false, ERR_OK, true},
    };

    for (auto test : tests) {
        mock_app_partition_split_context();
        if (test.mock_child_registered) {
            mock_child_registered();
        }
        if (test.mock_parent_paused) {
            mock_split_states(split_status::paused, PARENT_INDEX);
        }
        if (test.mock_pending) {
            app->helpers->contexts[PARENT_INDEX].stage = config_status::pending_remote_sync;
        }
        ASSERT_EQ(register_child(PARENT_INDEX, test.parent_ballot, test.wait_zk),
                  test.expected_err);
    }
}

} // namespace replication
} // namespace dsn
