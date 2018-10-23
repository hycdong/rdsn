#include <gtest/gtest.h>
#include <dsn/service_api_c.h>

#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/server_state.h"
#include "dist/replication/meta_server/meta_split_service.h"

#include "meta_service_test_app.h"

#define PARTITION_COUNT 8

using namespace ::dsn::replication;

void meta_service_test_app::app_partition_split_test()
{
    // create a fake app
    dsn::app_info info;
    info.is_stateful = true;
    info.app_id = 1;
    info.app_type = "simple_kv";
    info.app_name = "app";
    info.max_replica_count = 3;
    info.partition_count = PARTITION_COUNT;
    info.status = dsn::app_status::AS_CREATING;
    info.envs.clear();
    std::shared_ptr<app_state> fake_app = app_state::create(info);

    // create meta_service
    std::shared_ptr<meta_service> meta_svc = std::make_shared<meta_service>();
    meta_service *svc = meta_svc.get();

    svc->_meta_opts.cluster_root = "/meta_test";
    svc->_meta_opts.meta_state_service_type = "meta_state_service_simple";
    svc->remote_storage_initialize();

    std::string apps_root = "/meta_test/apps";
    std::shared_ptr<server_state> ss = svc->_state;
    ss->initialize(svc, apps_root);

    ss->_all_apps.emplace(std::make_pair(fake_app->app_id, fake_app));
    dsn::error_code ec = ss->sync_apps_to_remote_storage();
    ASSERT_EQ(ec, dsn::ERR_OK);

    std::cout << "case1. app_partition_split invalid params" << std::endl;
    {
        app_partition_split_request request;
        request.app_name = fake_app->app_name;
        request.new_partition_count = PARTITION_COUNT;

        dsn::message_ex *binary_req = dsn::message_ex::create_request(RPC_CM_APP_PARTITION_SPLIT);
        dsn::marshall(binary_req, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(binary_req);
        app_partition_split_rpc rpc(recv_msg); // don't need reply

        svc->_split_svc = dsn::make_unique<meta_split_service>(svc);
        meta_split_service *split_srv = svc->_split_svc.get();
        ASSERT_NE(split_srv, nullptr);

        split_srv->app_partition_split(rpc);
        svc->tracker()->wait_outstanding_tasks();

        auto response = rpc.response();

        ASSERT_EQ(response.err, dsn::ERR_INVALID_PARAMETERS);
    }

    std::cout << "case2. app_partition_split wrong table" << std::endl;
    {
        app_partition_split_request request;
        request.app_name = "table_not_exist";
        request.new_partition_count = PARTITION_COUNT * 2;

        dsn::message_ex *binary_req = dsn::message_ex::create_request(RPC_CM_APP_PARTITION_SPLIT);
        dsn::marshall(binary_req, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(binary_req);
        app_partition_split_rpc rpc(recv_msg); // don't need reply

        svc->_split_svc = dsn::make_unique<meta_split_service>(svc);
        meta_split_service *split_srv = svc->_split_svc.get();
        ASSERT_NE(split_srv, nullptr);

        split_srv->app_partition_split(rpc);
        svc->tracker()->wait_outstanding_tasks();

        auto response = rpc.response();

        ASSERT_EQ(response.err, dsn::ERR_APP_NOT_EXIST);
    }

    std::cout << "case3. app_partition_split successful" << std::endl;
    {
        app_partition_split_request request;
        request.app_name = fake_app->app_name;
        request.new_partition_count = PARTITION_COUNT * 2;

        dsn::message_ex *binary_req = dsn::message_ex::create_request(RPC_CM_APP_PARTITION_SPLIT);
        dsn::marshall(binary_req, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(binary_req);
        app_partition_split_rpc rpc(recv_msg); // don't need reply

        svc->_split_svc = dsn::make_unique<meta_split_service>(svc);
        meta_split_service *split_srv = svc->_split_svc.get();
        ASSERT_NE(split_srv, nullptr);

        split_srv->app_partition_split(rpc);
        svc->tracker()->wait_outstanding_tasks();

        auto response = rpc.response();

        ASSERT_EQ(response.err, dsn::ERR_OK);
        ASSERT_EQ(response.partition_count, PARTITION_COUNT * 2);
    }
}

void meta_service_test_app::register_child_test()
{
    // create a fake app
    dsn::app_info info;
    info.is_stateful = true;
    info.app_id = 1;
    info.app_type = "simple_kv";
    info.app_name = "app";
    info.max_replica_count = 3;
    info.partition_count = PARTITION_COUNT;
    info.status = dsn::app_status::AS_CREATING;
    info.envs.clear();
    std::shared_ptr<app_state> fake_app = app_state::create(info);
    int parent_index = 1;
    int child_index = parent_index + info.partition_count;

    // create meta_service
    std::shared_ptr<meta_service> meta_svc = std::make_shared<meta_service>();
    meta_service *svc = meta_svc.get();

    svc->_meta_opts.cluster_root = "/meta_test";
    svc->_meta_opts.meta_state_service_type = "meta_state_service_simple";
    svc->remote_storage_initialize();

    svc->_split_svc = dsn::make_unique<meta_split_service>(svc);
    meta_split_service *split_srv = svc->_split_svc.get();
    ASSERT_NE(split_srv, nullptr);

    std::string apps_root = "/meta_test/apps";
    std::shared_ptr<server_state> ss = svc->_state;
    ss->initialize(svc, apps_root);

    ss->_all_apps.emplace(std::make_pair(fake_app->app_id, fake_app));
    dsn::error_code ec = ss->sync_apps_to_remote_storage();
    ASSERT_EQ(ec, dsn::ERR_OK);

    // update app partition config
    std::shared_ptr<app_state> app = ss->get_app(info.app_id);
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
    ss->_nodes.insert(
        std::pair<dsn::rpc_address, node_state>(dsn::rpc_address("127.0.0.1", 8704), node));
    ss->_nodes.insert(
        std::pair<dsn::rpc_address, node_state>(dsn::rpc_address("127.0.0.2", 8704), node));
    ss->_nodes.insert(
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
    request.app = info;
    request.child_config = child_config;
    request.parent_config = parent_config;
    request.primary_address = dsn::rpc_address("127.0.0.1", 8704);

    std::cout << "case1. parent ballot not match" << std::endl;
    {
        request.parent_config.ballot = 1;

        dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_REGISTER_CHILD_REPLICA);
        dsn::marshall(msg, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(msg);

        register_child_rpc rpc(recv_msg);
        split_srv->register_child_on_meta(rpc);
        svc->tracker()->wait_outstanding_tasks();

        auto response = rpc.response();
        ASSERT_EQ(response.err, dsn::ERR_INVALID_VERSION);

        request.parent_config.ballot = 2;
    }

    std::cout << "case2. child ballot is not invalid" << std::endl;
    {
        app->partitions[9].ballot = 2;

        dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_REGISTER_CHILD_REPLICA);
        dsn::marshall(msg, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(msg);

        register_child_rpc rpc(recv_msg);
        split_srv->register_child_on_meta(rpc);
        svc->tracker()->wait_outstanding_tasks();

        auto response = rpc.response();
        ASSERT_EQ(response.err, dsn::ERR_CHILD_REGISTERED);

        app->partitions[9].ballot = invalid_ballot;
    }

    std::cout << "case3. sync task exist" << std::endl;
    {
        app->helpers->contexts[parent_index].stage = config_status::pending_remote_sync;

        dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_REGISTER_CHILD_REPLICA);
        dsn::marshall(msg, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(msg);

        register_child_rpc rpc(recv_msg);
        split_srv->register_child_on_meta(rpc);
        svc->tracker()->wait_outstanding_tasks();

        app->helpers->contexts[parent_index].stage = config_status::not_pending;
    }

    std::cout << "case4. succeed" << std::endl;
    {
        dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_REGISTER_CHILD_REPLICA);
        dsn::marshall(msg, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(msg);

        register_child_rpc rpc(recv_msg);
        split_srv->register_child_on_meta(rpc);
        svc->tracker()->wait_outstanding_tasks();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        auto response = rpc.response();
        ASSERT_EQ(response.err, dsn::ERR_OK);
    }
}

void meta_service_test_app::on_query_child_state_test()
{
    // create a fake app
    dsn::app_info info;
    info.is_stateful = true;
    info.app_id = 1;
    info.app_type = "simple_kv";
    info.app_name = "app";
    info.max_replica_count = 3;
    info.partition_count = PARTITION_COUNT;
    info.status = dsn::app_status::AS_CREATING;
    info.envs.clear();
    std::shared_ptr<app_state> fake_app = app_state::create(info);

    // create meta_service
    std::shared_ptr<meta_service> meta_svc = std::make_shared<meta_service>();
    meta_service *svc = meta_svc.get();
    svc->_meta_opts.cluster_root = "/meta_test";
    svc->_meta_opts.meta_state_service_type = "meta_state_service_simple";
    svc->remote_storage_initialize();
    svc->_split_svc = dsn::make_unique<meta_split_service>(svc);
    meta_split_service *split_srv = svc->_split_svc.get();
    ASSERT_NE(split_srv, nullptr);

    // create server_state
    std::string apps_root = "/meta_test/apps";
    std::shared_ptr<server_state> ss = svc->_state;
    ss->initialize(svc, apps_root);
    ss->_all_apps.emplace(std::make_pair(fake_app->app_id, fake_app));
    dsn::error_code ec = ss->sync_apps_to_remote_storage();
    ASSERT_EQ(ec, dsn::ERR_OK);

    // mock request and rpc msg
    dsn::gpid parent_gpid(1, 1);
    query_child_state_request request;
    request.parent_gpid = parent_gpid;

    // mock app
    std::shared_ptr<app_state> app = ss->get_app(info.app_id);
    int partition_count = info.partition_count;
    app->partitions.resize(partition_count * 2);

    dsn::partition_configuration parent_config;
    parent_config.ballot = 3;
    parent_config.pid = parent_gpid;
    dsn::partition_configuration child_config;
    child_config.ballot = invalid_ballot;
    child_config.pid = dsn::gpid(info.app_id, parent_gpid.get_partition_index() + partition_count);
    dsn::partition_configuration config = parent_config;
    config.pid = dsn::gpid(info.app_id, parent_gpid.get_partition_index() + partition_count / 2);

    app->partitions[parent_gpid.get_partition_index()] = parent_config;
    app->partitions[child_config.pid.get_partition_index()] = child_config;
    app->partitions[config.pid.get_partition_index()] = parent_config;

    std::cout << "case1. pending_sync_task exist" << std::endl;
    {
        app->helpers->contexts[parent_gpid.get_partition_index()].pending_sync_task =
            dsn::tasking::enqueue(
                LPC_META_STATE_HIGH,
                svc->tracker(),
                []() { std::cout << "This is mock pending_sync_task" << std::endl; },
                parent_gpid.thread_hash());

        dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_QUERY_CHILD_STATE);
        dsn::marshall(msg, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(msg);
        query_child_state_rpc rpc(recv_msg);
        split_srv->on_query_child_state(rpc);
        svc->tracker()->wait_outstanding_tasks();

        auto response = rpc.response();
        ASSERT_EQ(dsn::ERR_TRY_AGAIN, response.err);

        app->helpers->contexts[parent_gpid.get_partition_index()].pending_sync_task = nullptr;
    }

    std::cout << "case2. equal partition count, not during split or finish split" << std::endl;
    {
        dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_QUERY_CHILD_STATE);
        dsn::marshall(msg, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(msg);
        query_child_state_rpc rpc(recv_msg);
        split_srv->on_query_child_state(rpc);
        svc->tracker()->wait_outstanding_tasks();

        auto response = rpc.response();
        ASSERT_EQ(dsn::ERR_OK, response.err);
        ASSERT_EQ(partition_count, response.partition_count);
        ASSERT_EQ(3, response.ballot);
    }

    app->partition_count *= 2;

    std::cout << "case3. child ballot is invalid" << std::endl;
    {
        dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_QUERY_CHILD_STATE);
        dsn::marshall(msg, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(msg);
        query_child_state_rpc rpc(recv_msg);
        split_srv->on_query_child_state(rpc);
        svc->tracker()->wait_outstanding_tasks();

        auto response = rpc.response();
        ASSERT_EQ(dsn::ERR_OK, response.err);
        ASSERT_EQ(partition_count * 2, response.partition_count);
        ASSERT_EQ(invalid_ballot, response.ballot);
    }

    std::cout << "case4. child ballot is valid" << std::endl;
    {
        app->partitions[child_config.pid.get_partition_index()].ballot = 4;

        dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_QUERY_CHILD_STATE);
        dsn::marshall(msg, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(msg);

        query_child_state_rpc rpc(recv_msg);
        split_srv->on_query_child_state(rpc);
        svc->tracker()->wait_outstanding_tasks();

        auto response = rpc.response();
        ASSERT_EQ(dsn::ERR_OK, response.err);
        ASSERT_EQ(partition_count * 2, response.partition_count);
        ASSERT_EQ(4, response.ballot);
    }
}
