#include <gtest/gtest.h>
#include <dsn/service_api_c.h>

#include "dist/replication/meta_server/meta_service.h"
#include "meta_service_test_app.h"
#include "meta_split_service_test_helper.h"

using namespace ::dsn::replication;

// create mock meta service
std::shared_ptr<meta_service> meta_service_test_app::create_mock_meta_svc()
{
    std::shared_ptr<app_state> app = app_state::create(create_mock_app_info());
    // create meta_service
    std::shared_ptr<meta_service> meta_svc = std::make_shared<meta_service>();
    meta_svc->_meta_opts.cluster_root = "/meta_test";
    meta_svc->_meta_opts.meta_state_service_type = "meta_state_service_simple";
    meta_svc->remote_storage_initialize();
    meta_svc->_split_svc = dsn::make_unique<meta_split_service>(meta_svc.get());
    // initialize server_state
    std::string apps_root = "/meta_test/apps";
    std::shared_ptr<server_state> ss = meta_svc->_state;
    ss->initialize(meta_svc.get(), apps_root);
    ss->_all_apps.emplace(std::make_pair(app->app_id, app));
    ss->sync_apps_to_remote_storage();

    return meta_svc;
}

void meta_service_test_app::app_partition_split_test()
{
    std::shared_ptr<meta_service> meta_svc = create_mock_meta_svc();
    std::shared_ptr<app_state> app = meta_svc->get_server_state()->get_app(NAME);

    app_partition_split_request request;
    request.app_name = NAME;
    request.new_partition_count = PARTITION_COUNT * 2;

    std::cout << "case1. app_partition_split invalid params" << std::endl;
    {
        request.new_partition_count = PARTITION_COUNT;
        auto response =
            send_request(RPC_CM_APP_PARTITION_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_INVALID_PARAMETERS);
        request.new_partition_count = PARTITION_COUNT * 2;
    }

    std::cout << "case2. app_partition_split wrong table" << std::endl;
    {
        request.app_name = "table_not_exist";
        auto response =
            send_request(RPC_CM_APP_PARTITION_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_APP_NOT_EXIST);
        request.app_name = NAME;
    }

    std::cout << "case3. app_partition_split successful" << std::endl;
    {
        auto response =
            send_request(RPC_CM_APP_PARTITION_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_OK);
        ASSERT_EQ(response.partition_count, PARTITION_COUNT * 2);
    }

    std::cout << "case4. app_partition_split with paused partition" << std::endl;
    {
        // mock parent_config
        dsn::partition_configuration config;
        config.pid = dsn::gpid(app->app_id, 0);
        config.partition_flags |= pc_flags::child_dropped;
        app->partitions[0] = config;
        // case3 lead partition count double
        request.new_partition_count = PARTITION_COUNT * 4;

        auto response =
            send_request(RPC_CM_APP_PARTITION_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_CHILD_DROPPED);
    }
}

void meta_service_test_app::register_child_test()
{
    std::shared_ptr<meta_service> meta_svc = create_mock_meta_svc();
    std::shared_ptr<app_state> app = meta_svc->get_server_state()->get_app(NAME);
    int parent_index = 1;
    int child_index = parent_index + app->partition_count;

    // update app partition config
    app->partition_count *= 2;
    app->partitions.resize(app->partition_count);
    app->helpers->contexts.resize(app->partition_count);
    for (int i = 0; i < app->partition_count; ++i) {
        app->helpers->contexts[i].config_owner = &app->partitions[i];
        app->partitions[i].pid = dsn::gpid(app->app_id, i);
        if (i >= app->partition_count / 2) {
            app->partitions[i].ballot = invalid_ballot;
        } else {
            app->partitions[i].ballot = 2;
        }
    }

    // mock node_state
    node_state node;
    node.put_partition(dsn::gpid(app->app_id, parent_index), true);
    meta_svc->get_server_state()->_nodes.insert(
        std::pair<dsn::rpc_address, node_state>(dsn::rpc_address("127.0.0.1", 8704), node));
    meta_svc->get_server_state()->_nodes.insert(
        std::pair<dsn::rpc_address, node_state>(dsn::rpc_address("127.0.0.2", 8704), node));
    meta_svc->get_server_state()->_nodes.insert(
        std::pair<dsn::rpc_address, node_state>(dsn::rpc_address("127.0.0.3", 8704), node));

    // mock parent_config
    dsn::partition_configuration parent_config;
    parent_config.ballot = 2;
    parent_config.last_committed_decree = 5;
    parent_config.max_replica_count = 3;
    parent_config.pid = dsn::gpid(app->app_id, parent_index);
    parent_config.primary = dsn::rpc_address("127.0.0.1", 8704);
    parent_config.secondaries.emplace_back(dsn::rpc_address("127.0.0.2", 8704));
    parent_config.secondaries.emplace_back(dsn::rpc_address("127.0.0.3", 8704));

    // mock child_config
    dsn::partition_configuration child_config = parent_config;
    child_config.ballot++;
    child_config.last_committed_decree = 0;
    child_config.pid = dsn::gpid(app->app_id, child_index);

    register_child_request request;
    request.app = create_mock_app_info();
    request.child_config = child_config;
    request.parent_config = parent_config;
    request.primary_address = dsn::rpc_address("127.0.0.1", 8704);

    std::cout << "case1. parent ballot not match" << std::endl;
    {
        request.parent_config.ballot = 1;
        auto response = send_request(
            RPC_CM_REGISTER_CHILD_REPLICA, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_INVALID_VERSION);
        request.parent_config.ballot = 2;
    }

    std::cout << "case2. child ballot is not invalid" << std::endl;
    {
        app->partitions[child_index].ballot = 2;
        auto response = send_request(
            RPC_CM_REGISTER_CHILD_REPLICA, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_CHILD_REGISTERED);
        app->partitions[child_index].ballot = invalid_ballot;
    }

    std::cout << "case3. sync task exist" << std::endl;
    {
        app->helpers->contexts[parent_index].stage = config_status::pending_remote_sync;
        send_request(RPC_CM_REGISTER_CHILD_REPLICA, request, meta_svc, meta_svc->_split_svc.get());
        app->helpers->contexts[parent_index].stage = config_status::not_pending;
    }

    std::cout << "case5. split paused" << std::endl;
    {
        app->partitions[parent_index].partition_flags |= pc_flags::child_dropped;
        auto response = send_request(
            RPC_CM_REGISTER_CHILD_REPLICA, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_CHILD_DROPPED);
        app->partitions[parent_index].partition_flags &= (~pc_flags::child_dropped);
    }

    std::cout << "case4. succeed" << std::endl;
    {
        dsn::message_ex *recv_msg = create_recv_msg(RPC_CM_REGISTER_CHILD_REPLICA, request);
        register_child_rpc rpc(recv_msg);
        meta_svc->_split_svc->register_child_on_meta(rpc);
        meta_svc->tracker()->wait_outstanding_tasks();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        auto response = rpc.response();
        ASSERT_EQ(response.err, dsn::ERR_OK);
    }
}

void meta_service_test_app::on_query_child_state_test()
{
    std::shared_ptr<meta_service> meta_svc = create_mock_meta_svc();
    std::shared_ptr<app_state> app = meta_svc->get_server_state()->get_app(NAME);

    // mock request and rpc msg
    dsn::gpid parent_gpid(1, 1);
    query_child_state_request request;
    request.parent_gpid = parent_gpid;

    // mock app
    int partition_count = PARTITION_COUNT;
    app->partitions.resize(PARTITION_COUNT * 2);

    dsn::partition_configuration parent_config;
    parent_config.ballot = 3;
    parent_config.pid = parent_gpid;
    app->partitions[parent_gpid.get_partition_index()] = parent_config;

    dsn::partition_configuration child_config;
    child_config.ballot = invalid_ballot;
    child_config.pid = dsn::gpid(app->app_id, parent_gpid.get_partition_index() + partition_count);
    app->partitions[child_config.pid.get_partition_index()] = child_config;

    std::cout << "case1. pending_sync_task exist" << std::endl;
    {
        app->helpers->contexts[parent_gpid.get_partition_index()].pending_sync_task =
            dsn::tasking::enqueue(
                LPC_META_STATE_HIGH,
                meta_svc->tracker(),
                []() { std::cout << "This is mock pending_sync_task" << std::endl; },
                parent_gpid.thread_hash());

        auto response =
            send_request(RPC_CM_QUERY_CHILD_STATE, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(dsn::ERR_TRY_AGAIN, response.err);

        app->helpers->contexts[parent_gpid.get_partition_index()].pending_sync_task = nullptr;
    }

    std::cout << "case2. equal partition count, not during split or finish split" << std::endl;
    {
        dsn::partition_configuration config;
        config.ballot = 3;
        config.pid = dsn::gpid(app->app_id, parent_gpid.get_partition_index() + partition_count / 2);
        app->partitions[config.pid.get_partition_index()] = config;

        auto response =
            send_request(RPC_CM_QUERY_CHILD_STATE, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(dsn::ERR_OK, response.err);
        ASSERT_EQ(partition_count, response.partition_count);
        ASSERT_EQ(3, response.ballot);
    }

    app->partition_count *= 2;

    std::cout << "case3. child ballot is invalid" << std::endl;
    {
        auto response =
            send_request(RPC_CM_QUERY_CHILD_STATE, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(dsn::ERR_OK, response.err);
        ASSERT_EQ(partition_count * 2, response.partition_count);
        ASSERT_EQ(invalid_ballot, response.ballot);
    }

    std::cout << "case4. child ballot is valid" << std::endl;
    {
        app->partitions[child_config.pid.get_partition_index()].ballot = 4;
        auto response =
            send_request(RPC_CM_QUERY_CHILD_STATE, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(dsn::ERR_OK, response.err);
        ASSERT_EQ(partition_count * 2, response.partition_count);
        ASSERT_EQ(4, response.ballot);
    }
}

void meta_service_test_app::pause_single_partition_split_test()
{
    std::shared_ptr<meta_service> meta_svc = create_mock_meta_svc();
    std::shared_ptr<app_state> app = meta_svc->get_server_state()->get_app(NAME);

    // mock partition_configuration
    int target_partition = 0;
    mock_partition_config(app);

    control_single_partition_split_request request;
    request.app_name = app->app_name;
    request.parent_partition_index = target_partition;
    request.is_pause = true;

    std::cout << "case1. pause partition split with wrong partition index" << std::endl;
    {
        request.parent_partition_index = PARTITION_COUNT - 1;
        auto response = send_request(
            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_INVALID_PARAMETERS);
        request.parent_partition_index = target_partition;
    }

    std::cout << "case2. pause partition split succeed" << std::endl;
    {
        auto response = send_request(
            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_OK);
    }

    std::cout << "case3. pause partition split which has been paused" << std::endl;
    {
        auto response = send_request(
            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_NO_NEED_OPERATE);
    }

    std::cout << "case4. pause partition split which finish split" << std::endl;
    {
        // clear paused flag in case2
        app->partitions[target_partition].partition_flags = 0;
        for (int i = PARTITION_COUNT / 2; i < PARTITION_COUNT; ++i) {
            app->partitions[i].ballot = 3;
        }

        auto response = send_request(
            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_CHILD_REGISTERED);
    }
}

void meta_service_test_app::restart_single_partition_split_test()
{
    std::shared_ptr<meta_service> meta_svc = create_mock_meta_svc();
    std::shared_ptr<app_state> app = meta_svc->get_server_state()->get_app(NAME);

    int target_partition = 0;
    mock_partition_config(app);

    control_single_partition_split_request request;
    request.app_name = app->app_name;
    request.parent_partition_index = target_partition;
    request.is_pause = false;

    std::cout << "case1. restart partition split with wrong partition index" << std::endl;
    {
        request.parent_partition_index = -1;
        auto response = send_request(
            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_INVALID_PARAMETERS);
        request.parent_partition_index = target_partition;
    }

    std::cout << "case2. restart partition which permit split" << std::endl;
    {
        auto response = send_request(
            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_NO_NEED_OPERATE);
    }

    std::cout << "case3. restart partition split succeed" << std::endl;
    {
        app->partitions[target_partition].partition_flags |= pc_flags::child_dropped;
        auto response = send_request(
            RPC_CM_CONTROL_SINGLE_SPLIT, request, meta_svc, meta_svc->_split_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_OK);
    }
}
