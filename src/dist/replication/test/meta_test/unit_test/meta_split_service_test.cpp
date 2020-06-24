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

#include <gtest/gtest.h>
#include <dsn/service_api_c.h>

//<<<<<<< HEAD
//#include "dist/replication/meta_server/meta_service.h" //TODO(heyuchen):delete
//#include "meta_service_test_app.h"
//#include "meta_test_base.h"
//#include "meta_split_service_test_helper.h" // TODO(heyuchen):delete

// using namespace ::dsn::replication; // TODO(heyuchen):delete

//// create mock meta service
// std::shared_ptr<meta_service> meta_service_test_app::create_mock_meta_svc()
//{
//    std::shared_ptr<app_state> app = app_state::create(create_mock_app_info());
//    // create meta_service
//    std::shared_ptr<meta_service> meta_svc = std::make_shared<meta_service>();
//    meta_svc->_meta_opts.cluster_root = "/meta_test";
//    meta_svc->_meta_opts.meta_state_service_type = "meta_state_service_simple";
//    meta_svc->remote_storage_initialize();
//    meta_svc->_split_svc = dsn::make_unique<meta_split_service>(meta_svc.get());
//    // initialize server_state
//    std::string apps_root = "/meta_test/apps";
//    std::shared_ptr<server_state> ss = meta_svc->_state;
//    ss->initialize(meta_svc.get(), apps_root);
//    ss->_all_apps.emplace(std::make_pair(app->app_id, app));
//    ss->sync_apps_to_remote_storage();

//    return meta_svc;
//}

// void meta_service_test_app::register_child_test()
//{
//    std::shared_ptr<meta_service> meta_svc = create_mock_meta_svc();
//    std::shared_ptr<app_state> app = meta_svc->get_server_state()->get_app(TNAME);
//    int parent_index = 1;
//    int child_index = parent_index + app->partition_count;

//    // update app partition config
//    app->partition_count *= 2;
//    app->partitions.resize(app->partition_count);
//    app->helpers->contexts.resize(app->partition_count);
//    for (int i = 0; i < app->partition_count; ++i) {
//        app->helpers->contexts[i].config_owner = &app->partitions[i];
//        app->partitions[i].pid = dsn::gpid(app->app_id, i);
//        if (i >= app->partition_count / 2) {
//            app->partitions[i].ballot = invalid_ballot;
//        } else {
//            app->partitions[i].ballot = 2;
//        }
//    }

//    // mock node_state
//    node_state node;
//    node.put_partition(dsn::gpid(app->app_id, parent_index), true);
//    meta_svc->get_server_state()->_nodes.insert(
//        std::pair<dsn::rpc_address, node_state>(dsn::rpc_address("127.0.0.1", 8704), node));
//    meta_svc->get_server_state()->_nodes.insert(
//        std::pair<dsn::rpc_address, node_state>(dsn::rpc_address("127.0.0.2", 8704), node));
//    meta_svc->get_server_state()->_nodes.insert(
//        std::pair<dsn::rpc_address, node_state>(dsn::rpc_address("127.0.0.3", 8704), node));

//    // mock parent_config
//    dsn::partition_configuration parent_config;
//    parent_config.ballot = 2;
//    parent_config.last_committed_decree = 5;
//    parent_config.max_replica_count = 3;
//    parent_config.pid = dsn::gpid(app->app_id, parent_index);
//    parent_config.primary = dsn::rpc_address("127.0.0.1", 8704);
//    parent_config.secondaries.emplace_back(dsn::rpc_address("127.0.0.2", 8704));
//    parent_config.secondaries.emplace_back(dsn::rpc_address("127.0.0.3", 8704));

//    // mock child_config
//    dsn::partition_configuration child_config = parent_config;
//    child_config.ballot++;
//    child_config.last_committed_decree = 0;
//    child_config.pid = dsn::gpid(app->app_id, child_index);

//    register_child_request request;
//    request.app = create_mock_app_info();
//    request.child_config = child_config;
//    request.parent_config = parent_config;
//    request.primary_address = dsn::rpc_address("127.0.0.1", 8704);

//    std::cout << "case1. parent ballot not match" << std::endl;
//    {
//        request.parent_config.ballot = 1;
//        auto response = send_request(
//            RPC_CM_REGISTER_CHILD_REPLICA, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_INVALID_VERSION);
//        request.parent_config.ballot = 2;
//    }

//    std::cout << "case2. child ballot is not invalid" << std::endl;
//    {
//        app->partitions[child_index].ballot = 2;
//        auto response = send_request(
//            RPC_CM_REGISTER_CHILD_REPLICA, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_CHILD_REGISTERED);
//        app->partitions[child_index].ballot = invalid_ballot;
//    }

//    std::cout << "case3. sync task exist" << std::endl;
//    {
//        app->helpers->contexts[parent_index].stage = config_status::pending_remote_sync;
//        send_request(RPC_CM_REGISTER_CHILD_REPLICA, request, meta_svc,
//        meta_svc->_split_svc.get());
//        app->helpers->contexts[parent_index].stage = config_status::not_pending;
//    }

//    std::cout << "case4. split paused" << std::endl;
//    {
//        app->partitions[parent_index].partition_flags |= pc_flags::child_dropped;
//        auto response = send_request(
//            RPC_CM_REGISTER_CHILD_REPLICA, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_CHILD_DROPPED);
//        app->partitions[parent_index].partition_flags &= (~pc_flags::child_dropped);
//    }

//    std::cout << "case5. split canceled" << std::endl;
//    {
//        app->partition_count /= 2;
//        auto response = send_request(
//            RPC_CM_REGISTER_CHILD_REPLICA, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_REJECT);
//        app->partition_count *= 2;
//    }

//    std::cout << "case6. succeed" << std::endl;
//    {
//        dsn::message_ex *recv_msg = create_recv_msg(RPC_CM_REGISTER_CHILD_REPLICA, request);
//        register_child_rpc rpc(recv_msg);
//        meta_svc->_split_svc->register_child_on_meta(rpc);
//        meta_svc->tracker()->wait_outstanding_tasks();
//        std::this_thread::sleep_for(std::chrono::milliseconds(100));
//        auto response = rpc.response();
//        ASSERT_EQ(response.err, dsn::ERR_OK);
//    }
//}

// void meta_service_test_app::on_query_child_state_test()
//{
//    std::shared_ptr<meta_service> meta_svc = create_mock_meta_svc();
//    std::shared_ptr<app_state> app = meta_svc->get_server_state()->get_app(TNAME);

//    // mock request and rpc msg
//    dsn::gpid parent_gpid(1, 1);
//    query_child_state_request request;
//    request.parent_gpid = parent_gpid;

//    // mock app
//    int partition_count = COUNT;
//    app->partitions.resize(COUNT * 2);

//    dsn::partition_configuration parent_config;
//    parent_config.ballot = 3;
//    parent_config.pid = parent_gpid;
//    app->partitions[parent_gpid.get_partition_index()] = parent_config;

//    dsn::partition_configuration child_config;
//    child_config.ballot = invalid_ballot;
//    child_config.pid = dsn::gpid(app->app_id, parent_gpid.get_partition_index() +
//    partition_count);
//    app->partitions[child_config.pid.get_partition_index()] = child_config;

//    std::cout << "case1. pending_sync_task exist" << std::endl;
//    {
//        app->helpers->contexts[parent_gpid.get_partition_index()].pending_sync_task =
//            dsn::tasking::enqueue(
//                LPC_META_STATE_HIGH,
//                meta_svc->tracker(),
//                []() { std::cout << "This is mock pending_sync_task" << std::endl; },
//                parent_gpid.thread_hash());

//        auto response =
//            send_request(RPC_CM_QUERY_CHILD_STATE, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(dsn::ERR_TRY_AGAIN, response.err);

//        app->helpers->contexts[parent_gpid.get_partition_index()].pending_sync_task = nullptr;
//    }

//    std::cout << "case2. equal partition count, not during split or finish split" << std::endl;
//    {
//        dsn::partition_configuration config;
//        config.ballot = 3;
//        config.pid =
//            dsn::gpid(app->app_id, parent_gpid.get_partition_index() + partition_count / 2);
//        app->partitions[config.pid.get_partition_index()] = config;

//        auto response =
//            send_request(RPC_CM_QUERY_CHILD_STATE, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(dsn::ERR_OK, response.err);
//        ASSERT_EQ(partition_count, response.partition_count);
//        ASSERT_EQ(3, response.ballot);
//    }

//    app->partition_count *= 2;

//    std::cout << "case3. child ballot is invalid" << std::endl;
//    {
//        auto response =
//            send_request(RPC_CM_QUERY_CHILD_STATE, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(dsn::ERR_OK, response.err);
//        ASSERT_EQ(partition_count * 2, response.partition_count);
//        ASSERT_EQ(invalid_ballot, response.ballot);
//    }

//    std::cout << "case4. child ballot is valid" << std::endl;
//    {
//        app->partitions[child_config.pid.get_partition_index()].ballot = 4;
//        auto response =
//            send_request(RPC_CM_QUERY_CHILD_STATE, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(dsn::ERR_OK, response.err);
//        ASSERT_EQ(partition_count * 2, response.partition_count);
//        ASSERT_EQ(4, response.ballot);
//    }
//}

// void meta_service_test_app::pause_single_partition_split_test()
//{
//    std::shared_ptr<meta_service> meta_svc = create_mock_meta_svc();
//    std::shared_ptr<app_state> app = meta_svc->get_server_state()->get_app(TNAME);

//    // mock partition_configuration
//    int target_partition = 0;
//    mock_partition_config(app);

//    control_single_partition_split_request request;
//    request.app_name = app->app_name;
//    request.parent_partition_index = target_partition;
//    request.is_pause = true;

//    std::cout << "case1. pause partition split with wrong partition index" << std::endl;
//    {
//        request.parent_partition_index = COUNT - 1;
//        auto response = send_request(
//            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_INVALID_PARAMETERS);
//        request.parent_partition_index = target_partition;
//    }

//    std::cout << "case2. pause partition split succeed" << std::endl;
//    {
//        auto response = send_request(
//            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_OK);
//    }

//    std::cout << "case3. pause partition split which has been paused" << std::endl;
//    {
//        auto response = send_request(
//            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_NO_NEED_OPERATE);
//    }

//    std::cout << "case4. pause partition split which finish split" << std::endl;
//    {
//        // clear paused flag in case2
//        app->partitions[target_partition].partition_flags = 0;
//        for (int i = COUNT / 2; i < COUNT; ++i) {
//            app->partitions[i].ballot = 3;
//        }

//        auto response = send_request(
//            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_CHILD_REGISTERED);
//    }
//}

// void meta_service_test_app::restart_single_partition_split_test()
//{
//    std::shared_ptr<meta_service> meta_svc = create_mock_meta_svc();
//    std::shared_ptr<app_state> app = meta_svc->get_server_state()->get_app(TNAME);

//    int target_partition = 0;
//    mock_partition_config(app);

//    control_single_partition_split_request request;
//    request.app_name = app->app_name;
//    request.parent_partition_index = target_partition;
//    request.is_pause = false;

//    std::cout << "case1. restart partition split with wrong partition index" << std::endl;
//    {
//        request.parent_partition_index = -1;
//        auto response = send_request(
//            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_INVALID_PARAMETERS);
//        request.parent_partition_index = target_partition;
//    }

//    std::cout << "case2. restart partition which permit split" << std::endl;
//    {
//        auto response = send_request(
//            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_NO_NEED_OPERATE);
//    }

//    std::cout << "case3. restart partition split succeed" << std::endl;
//    {
//        app->partitions[target_partition].partition_flags |= pc_flags::child_dropped;
//        auto response = send_request(
//            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_OK);
//    }
//}

// void meta_service_test_app::cancel_app_partition_split_test()
//{
//    std::shared_ptr<meta_service> meta_svc = create_mock_meta_svc();
//    std::shared_ptr<app_state> app = meta_svc->get_server_state()->get_app(TNAME);

//    mock_partition_config(app);

//    int target_partition = 0;
//    int original_partition_count = app->partition_count / 2;
//    cancel_app_partition_split_request request;
//    request.app_name = app->app_name;
//    request.original_partition_count = original_partition_count;
//    request.is_force = false;

//    std::cout << "case1. cancel with wrong partition count" << std::endl;
//    {
//        request.original_partition_count = app->partition_count;
//        auto response = send_request(
//            RPC_CM_CANCEL_APP_PARTITION_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_INVALID_PARAMETERS);
//        request.original_partition_count = original_partition_count;
//    }

//    std::cout << "case2. cancel with some child partitions registered but not force cancel"
//              << std::endl;
//    {
//        app->partitions[target_partition + original_partition_count].ballot = 3;
//        auto response = send_request(
//            RPC_CM_CANCEL_APP_PARTITION_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_REJECT);
//        app->partitions[target_partition + original_partition_count].ballot = -1;
//    }

//    std::cout << "case3. cancel with all child registered" << std::endl;
//    {
//        for (int i = original_partition_count; i < app->partition_count; ++i) {
//            app->partitions[i].ballot = 3;
//        }
//        request.is_force = true;
//        auto response = send_request(
//            RPC_CM_CANCEL_APP_PARTITION_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_CHILD_REGISTERED);
//    }

//    std::cout << "case4. cancel partition split succeed" << std::endl;
//    {
//        app->partitions[target_partition + original_partition_count].ballot = -1;
//        auto response = send_request(
//            RPC_CM_CANCEL_APP_PARTITION_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
//        ASSERT_EQ(response.err, dsn::ERR_OK);
//    }
//}

// void meta_service_test_app::clear_split_flags_test()
//{
//    std::shared_ptr<meta_service> meta_svc = create_mock_meta_svc();
//    std::shared_ptr<app_state> app = meta_svc->get_server_state()->get_app(TNAME);

//    mock_partition_config(app);

//    clear_partition_split_flag_request request;
//    request.app_name = app->app_name;

//    std::cout << "clear flags succeed" << std::endl;
//    {
//        for (int i = 0; i < app->partition_count / 2; ++i) {
//            app->partitions[i].partition_flags |= pc_flags::child_dropped;
//        }

//        dsn::message_ex *recv_msg = create_recv_msg(RPC_CM_CLEAR_PARTITION_SPLIT_FLAG, request);
//        clear_partition_split_flag_rpc rpc(recv_msg);
//        meta_svc->_split_svc->clear_partition_split_flag(rpc);
//        meta_svc->tracker()->wait_outstanding_tasks();
//        auto response = rpc.response();
//        ASSERT_EQ(response.err, dsn::ERR_OK);
//    }
//}
//=======
#include "meta_service_test_app.h"
#include "meta_test_base.h"
// >>>>>>> v1.12.3

#include <dsn/utility/fail_point.h>

namespace dsn {
namespace replication {
class meta_split_service_test : public meta_test_base
{
public:
    void SetUp()
    {
        meta_test_base::SetUp();
        create_app(NAME, PARTITION_COUNT);
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

    register_child_response
    register_child(ballot req_parent_ballot, ballot child_ballot, bool wait_zk = false)
    {
        // mock local app info
        auto app = find_app(NAME);
        app->partition_count *= 2;
        app->partitions.resize(app->partition_count);
        app->helpers->contexts.resize(app->partition_count);
        for (int i = 0; i < app->partition_count; ++i) {
            app->helpers->contexts[i].config_owner = &app->partitions[i];
            app->partitions[i].pid = dsn::gpid(app->app_id, i);
            if (i >= app->partition_count / 2) {
                app->partitions[i].ballot = invalid_ballot;
            } else {
                app->partitions[i].ballot = PARENT_BALLOT;
            }
        }
        app->partitions[CHILD_INDEX].ballot = child_ballot;

        // mock node state
        node_state node;
        node.put_partition(dsn::gpid(app->app_id, PARENT_INDEX), true);
        mock_node_state(dsn::rpc_address("127.0.0.1", 10086), node);

        // mock register_child_request
        partition_configuration parent_config;
        parent_config.ballot = req_parent_ballot;
        parent_config.last_committed_decree = 5;
        parent_config.max_replica_count = 3;
        parent_config.pid = dsn::gpid(app->app_id, PARENT_INDEX);

        dsn::partition_configuration child_config;
        child_config.ballot = PARENT_BALLOT + 1;
        child_config.last_committed_decree = 5;
        child_config.pid = dsn::gpid(app->app_id, CHILD_INDEX);

        // register_child_request request;
        auto request = dsn::make_unique<register_child_request>();
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
        return rpc.response();
    }

    void mock_app_partition_split_context()
    {
        auto app = find_app(NAME);
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
                app->helpers->split_states.status[i] = split_status::splitting;
            }
        }
    }

    void mock_child_registered()
    {
        auto app = find_app(NAME);
        app->partitions[CHILD_INDEX].ballot = PARENT_BALLOT;
        app->helpers->split_states.splitting_count--;
        app->helpers->split_states.status.erase(PARENT_INDEX);
    }

    void mock_split_states(split_status::type status, int32_t parent_index = -1)
    {
        auto app = find_app(NAME);
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

// TODO(heyuchen): fix regisiter unit tests error
// TEST_F(meta_split_service_test, register_child_with_wrong_ballot)
//{
//    auto resp = register_child(PARENT_BALLOT - 1, invalid_ballot);
//    ASSERT_EQ(resp.err, ERR_INVALID_VERSION);
//}

// TEST_F(meta_split_service_test, register_child_with_child_registered)
//{
//    auto resp = register_child(PARENT_BALLOT, PARENT_BALLOT + 1);
//    ASSERT_EQ(resp.err, ERR_CHILD_REGISTERED);
//}

// TEST_F(meta_split_service_test, register_child_succeed)
//{
//    auto resp = register_child(PARENT_BALLOT, invalid_ballot, true);
//    ASSERT_EQ(resp.err, ERR_OK);
//}

} // namespace replication
} // namespace dsn
