#include <gtest/gtest.h>
#include <dsn/service_api_c.h>

#include "dist/replication/lib/replica.h"
#include "dist/replication/lib/replica_stub.h"

#include "replication_service_test_app.h"
#include "replica_split_mock.h"

#define BALLOT 1

using namespace ::dsn::replication;

void replication_service_test_app::on_add_child_test()
{
    auto stub = new replica_stub_mock();
    auto parent_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    group_check_request request;
    request.child_gpid = child_gpid;
    request.config.ballot = BALLOT;
    ASSERT_EQ(nullptr, stub->get_replica(child_gpid, false));

    substitutes["init_child_replica"] = nullptr;

    {
        std::cout << "case1. failed - ballot not match" << std::endl;
        request.config.ballot = BALLOT+1;
        parent_replica->on_add_child(request);
        parent_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(nullptr, stub->get_replica(child_gpid, false));
        request.config.ballot = BALLOT;
    }

    {
        std::cout << "case2. failed - child_gpid alreay exist" << std::endl;
        parent_replica->_child_gpid = child_gpid;
        parent_replica->on_add_child(request);
        parent_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(nullptr, stub->get_replica(child_gpid, false));
    }

    {
        std::cout << "case3. failed - partition count not match" << std::endl;
        dsn::app_info info = parent_replica->_app_info;
        info.partition_count = partition_count*2;
        parent_replica->_app_info = info;
        parent_replica->_child_gpid.set_app_id(0);
        parent_replica->on_add_child(request);
        parent_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(nullptr, stub->get_replica(child_gpid, false));

        info.partition_count = partition_count;
        parent_replica->_app_info = info;
    }

    {
        std::cout << "cas4. succeed - add child" << std::endl;
        parent_replica->_child_gpid.set_app_id(0);
        parent_replica->on_add_child(request);
        parent_replica->tracker()->wait_outstanding_tasks();

        replica_ptr child = stub->get_replica(child_gpid, false);
        ASSERT_NE(nullptr, child);
        ASSERT_EQ(child_gpid, child->get_gpid());
        ASSERT_EQ(BALLOT, child->get_ballot());
        ASSERT_EQ(partition_count, child->get_app_info()->partition_count);
    }

    substitutes.erase("init_child_replica");
}

void replication_service_test_app::init_child_replica_test()
{
    auto stub = new replica_stub_mock();
    auto parent_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_INACTIVE);

    substitutes["prepare_copy_parent_state"] = nullptr;
    substitutes["check_child_state"] = nullptr;

    {
        std::cout << "case1. failed - child status is not inactive" << std::endl;
        stub->replicas[parent_gpid] = parent_replica;
        stub->replicas[child_gpid] = child_replica;
        parent_replica->_child_gpid = child_gpid;
        child_replica->_config.status = partition_status::PS_SECONDARY;

        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT);
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());
    }

    {
        std::cout << "case2. succeed - init child replica" << std::endl;
        child_replica->_config.status = partition_status::PS_INACTIVE;

        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT);
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(partition_status::PS_PARTITION_SPLIT, child_replica->_config.status);
        ASSERT_EQ(parent_gpid, child_replica->_split_states.parent_gpid);
        ASSERT_FALSE(child_replica->_split_states.is_prepare_list_copied);
        ASSERT_FALSE(child_replica->_split_states.is_caught_up);
    }

    substitutes.erase("prepare_copy_parent_state");
    substitutes.erase("check_child_state");
}

void replication_service_test_app::check_child_state_test()
{
    auto stub = new replica_stub_mock();
    auto parent_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_INACTIVE);
    stub->replicas[parent_gpid] = parent_replica;
    stub->replicas[child_gpid] = child_replica;
    parent_replica->_child_gpid = child_gpid;
    substitutes["prepare_copy_parent_state"] = nullptr;

    {
        std::cout << "case1. failed - invalid child state" << std::endl;
        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT);
        child_replica->_config.status = partition_status::PS_INACTIVE;
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(nullptr, child_replica->_split_states.check_state_task);
    }

    {
        std::cout << "case2. failed - invalid ballot" << std::endl;
        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT+1);
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(partition_status::PS_ERROR, child_replica->_config.status);
        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());
        parent_replica->_child_gpid = child_gpid;
    }

    {
        std::cout << "case3. failed - invalid child gpid" << std::endl;
        child_replica->_config.status = partition_status::PS_INACTIVE;
        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT);
        parent_replica->_child_gpid = parent_gpid;
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(partition_status::PS_ERROR, child_replica->_config.status);
        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());
        parent_replica->_child_gpid = child_gpid;
    }

    {
        std::cout << "case4. failed - invalid parent state" << std::endl;
        child_replica->_config.status = partition_status::PS_INACTIVE;
        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT);
        parent_replica->_config.status = partition_status::PS_POTENTIAL_SECONDARY;
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(partition_status::PS_ERROR, child_replica->_config.status);
        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());
        parent_replica->_child_gpid = child_gpid;
    }

    substitutes.erase("prepare_copy_parent_state");
}
