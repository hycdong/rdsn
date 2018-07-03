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
        request.config.ballot = BALLOT + 1;
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
        info.partition_count = partition_count * 2;
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
        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT + 1);
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

void replication_service_test_app::prepare_copy_parent_state_test()
{
    auto stub = new replica_stub_mock();
    auto parent_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_PARTITION_SPLIT);
    stub->replicas[parent_gpid] = parent_replica;
    stub->replicas[child_gpid] = child_replica;
    parent_replica->_child_gpid = child_gpid;

    // mock file list
    log_file_mock *lf = new log_file_mock("log.1.0.txt");
    lf->set_file_size(10);

    // mock private log
    mutation_log_private_mock *private_log_mock =
        new mutation_log_private_mock(parent_replica->get_gpid(), parent_replica);
    private_log_mock->add_log(lf);
    parent_replica->_private_log = private_log_mock;

    // mock prepare list
    prepare_list *plist_mock = new prepare_list(0, 10, nullptr);
    for (int i = 0; i < 10; ++i) {
        mutation_ptr mu = new mutation();
        mu->data.header.decree = i;
        mu->data.header.ballot = BALLOT;
        plist_mock->put(mu);
        parent_replica->_private_log->append(
            mu, LPC_WRITE_REPLICATION_LOG_COMMON, nullptr, nullptr);
    }
    parent_replica->_prepare_list = plist_mock;

    substitutes["copy_parent_state"] = nullptr;

    {
        std::cout << "case1. failed - invalid parent state" << std::endl;
        parent_replica->_config.status = partition_status::PS_POTENTIAL_SECONDARY;

        const std::string &dir = child_replica->_app->learn_dir();
        parent_replica->prepare_copy_parent_state(dir, child_gpid, child_replica->get_ballot());
        parent_replica->tracker()->wait_outstanding_tasks();
        child_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());

        parent_replica->_config.status = partition_status::PS_PRIMARY;
        parent_replica->_child_gpid = child_gpid;
    }

    {
        std::cout << "case2. succeed" << std::endl;

        const std::string &dir = child_replica->_app->learn_dir();
        parent_replica->prepare_copy_parent_state(dir, child_gpid, child_replica->get_ballot());
        parent_replica->tracker()->wait_outstanding_tasks();
        child_replica->tracker()->wait_outstanding_tasks();
    }

    substitutes.erase("copy_parent_state");
}

void replication_service_test_app::copy_parent_state_test()
{
    auto stub = new replica_stub_mock();
    auto parent_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);

    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_PARTITION_SPLIT);
    child_replica->_split_states.parent_gpid = parent_gpid;
    child_replica->_split_states.is_caught_up = false;
    child_replica->_split_states.is_prepare_list_copied = false;

    stub->replicas[parent_gpid] = parent_replica;
    stub->replicas[child_gpid] = child_replica;
    parent_replica->_child_gpid = child_gpid;

    // mock shared log and private log
    mutation_log_private_mock *log_mock =
        new mutation_log_private_mock(child_replica->get_gpid(), child_replica);
    stub->set_log(log_mock);
    child_replica->_private_log = log_mock;

    // mock error code
    dsn::error_code ec = dsn::ERR_OK;

    // mock files
    std::vector<std::string> files;
    files.push_back("log.1.0.txt");

    // mock prepare list and mutation_list
    std::vector<mutation_ptr> mutation_list;
    prepare_list *plist_mock = new prepare_list(0, 10, nullptr);
    for (int i = 0; i < 10; ++i) {
        mutation_ptr mu = new mutation();
        mu->data.header.decree = i;
        mu->data.header.ballot = BALLOT;
        plist_mock->put(mu);

        if (i > DECREE) {
            mutation_list.push_back(mu);
        }
    }

    // mock learn_state
    learn_state lstate;
    lstate.to_decree_included = DECREE;
    lstate.files.push_back("./learn_dummy");

    substitutes["apply_parent_state"] = nullptr;

    {
        std::cout << "case1. failed - invalid child state" << std::endl;
        child_replica->_config.status = partition_status::PS_SECONDARY;

        child_replica->copy_parent_state(ec, lstate, mutation_list, files, plist_mock);
        child_replica->tracker()->wait_outstanding_tasks();

        child_replica->_config.status = partition_status::PS_PARTITION_SPLIT;
    }

    {
        std::cout << "case2. failed - error code is not ERR_OK" << std::endl;
        ec = dsn::ERR_GET_LEARN_STATE_FAILED;
        substitutes["prepare_copy_parent_state"] = nullptr;

        child_replica->copy_parent_state(ec, lstate, mutation_list, files, plist_mock);
        parent_replica->tracker()->wait_outstanding_tasks();
        child_replica->tracker()->wait_outstanding_tasks();

        ec = dsn::ERR_OK;
        substitutes.erase("prepare_copy_parent_state");
    }

    {
        std::cout << "case3. succeed" << std::endl;

        child_replica->copy_parent_state(ec, lstate, mutation_list, files, plist_mock);
        child_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(plist_mock, child_replica->_prepare_list);
        ASSERT_EQ(true, child_replica->_split_states.is_prepare_list_copied);
        ASSERT_NE(nullptr, child_replica->_split_states.async_learn_task);
    }

    substitutes.erase("apply_parent_state");
}

void replication_service_test_app::apply_parent_state_test()
{
    auto stub = new replica_stub_mock();
    auto parent_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_PARTITION_SPLIT);
    stub->replicas[parent_gpid] = parent_replica;
    stub->replicas[child_gpid] = child_replica;
    child_replica->_split_states.parent_gpid = parent_gpid;
    child_replica->_split_states.is_caught_up = false;
    parent_replica->_child_gpid = child_gpid;

    mutation_log_private_mock *log_mock = new mutation_log_private_mock(child_gpid, child_replica);
    child_replica->_private_log = log_mock;
    parent_replica->_private_log = log_mock;
    stub->set_log(log_mock);

    // mock error_code
    dsn::error_code ec = dsn::ERR_OK;

    // mock learn_state
    learn_state lstate;
    lstate.to_decree_included = DECREE;
    lstate.files.push_back("./learn_dummy");

    // mock files
    std::vector<std::string> files;
    files.push_back("log.1.0.txt");

    // mock prepare list and mutation_list
    std::vector<mutation_ptr> mutation_list;
    prepare_list *plist_mock = new prepare_list(0, 10, nullptr);

    for (int i = 0; i < 10; ++i) {
        mutation_ptr mu = new mutation();
        mu->data.header.decree = i;
        mu->data.header.ballot = BALLOT;
        plist_mock->put(mu);
        if (i > DECREE) {
            mutation_list.push_back(mu);
        }
        child_replica->_private_log->append(
            mu, LPC_WRITE_REPLICATION_LOG_COMMON, child_replica->tracker(), nullptr);
        mu->set_logged();
    }
    child_replica->_prepare_list = plist_mock;

    // mock aysnc_learn_mutation_private_log, remove learn from private log files
    std::function<dsn::error_code(
        std::vector<mutation_ptr>, std::vector<std::string>, decree, bool)>
        mock_async_learn_mutation_private_log =
            [child_replica](std::vector<mutation_ptr> mutation_list,
                            std::vector<std::string> files,
                            decree last_committed_decree,
                            bool is_mock_failure) {

                prepare_list plist(child_replica->_app->last_committed_decree(),
                                   child_replica->_options->max_mutation_count_in_prepare_list,
                                   [child_replica](mutation_ptr mu) {
                                       if (mu->data.header.decree ==
                                           child_replica->_app->last_committed_decree() + 1) {
                                           child_replica->_app->apply_mutation(mu);
                                       }
                                   });

                int count = 0;
                for (mutation_ptr &mu : mutation_list) {
                    decree d = mu->data.header.decree;
                    if (d <= plist.last_committed_decree()) {
                        continue;
                    }

                    mutation_ptr origin_mu = plist.get_mutation_by_decree(d);
                    if (origin_mu != nullptr &&
                        origin_mu->data.header.ballot >= mu->data.header.ballot) {
                        continue;
                    }

                    if (!mu->is_logged()) {
                        mu->set_logged();
                    }

                    plist.prepare(mu, partition_status::PS_SECONDARY);
                    ++count;
                }

                ddebug("{} apply mutations in memory, count is {}, app last_committed_decree is {}",
                       child_replica->name(),
                       count,
                       child_replica->_app->last_committed_decree());

                plist.commit(last_committed_decree, COMMIT_TO_DECREE_HARD);

                if (is_mock_failure) {
                    return dsn::ERR_FILE_OPERATION_FAILED;
                } else {
                    return dsn::ERR_OK;
                }
            };

    substitutes["async_learn_mutation_private_log"] = &mock_async_learn_mutation_private_log;
    substitutes["child_catch_up"] = nullptr;

    {
        std::cout << "case1. succeed" << std::endl;

        child_replica->apply_parent_state(ec, lstate, mutation_list, files, DECREE);
        child_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(nullptr, child_replica->_split_states.async_learn_task);
        ASSERT_EQ(BALLOT, child_replica->get_ballot());
    }

    {
        std::cout << "case2. child replica status wrong" << std::endl;
        child_replica->_config.status = partition_status::PS_SECONDARY;

        child_replica->apply_parent_state(ec, lstate, mutation_list, files, DECREE);
        child_replica->tracker()->wait_outstanding_tasks();

        child_replica->_config.status = partition_status::PS_PARTITION_SPLIT;
    }

    {
        std::cout << "case3. checkpoint failed" << std::endl;

        substitutes["async_learn_mutation_private_log_mock_failure"] = nullptr;

        child_replica->apply_parent_state(ec, lstate, mutation_list, files, DECREE);
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(partition_status::PS_ERROR, child_replica->_config.status);
        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());

        substitutes.erase("async_learn_mutation_private_log_mock_failure");
    }

    substitutes.erase("async_learn_mutation_private_log");
    substitutes.erase("child_catch_up");
}

void replication_service_test_app::child_catch_up_test()
{
    auto stub = new replica_stub_mock();
    auto parent_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_PARTITION_SPLIT);
    stub->replicas[parent_gpid] = parent_replica;
    stub->replicas[child_gpid] = child_replica;
    parent_replica->_child_gpid = child_gpid;

    child_replica->_split_states.parent_gpid = parent_gpid;
    child_replica->_split_states.is_prepare_list_copied = true;
    child_replica->_split_states.is_caught_up = false;
    child_replica->_split_states.async_learn_task = nullptr;

    mutation_log_private_mock *log_mock = new mutation_log_private_mock(child_gpid, child_replica);
    child_replica->_private_log = log_mock;
    parent_replica->_private_log = log_mock;
    stub->set_log(log_mock);

    // mock prepare list
    prepare_list *plist = new prepare_list(0, 10, nullptr);
    prepare_list *plist2 = new prepare_list(6, 10, nullptr);
    for (int i = 0; i < 10; ++i) {
        mutation_ptr mu = new mutation();
        mu->data.header.decree = i;
        mu->data.header.ballot = BALLOT;
        plist->put(mu);
        child_replica->_private_log->append(
            mu, LPC_WRITE_REPLICATION_LOG_COMMON, child_replica->tracker(), nullptr);
        mu->set_logged();
    }
    child_replica->_prepare_list = plist;

    substitutes["notify_primary_split_catch_up"] = nullptr;

    {
        std::cout << "case1. succeed - no missing mutations" << std::endl;

        child_replica->child_catch_up();
        child_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(true, child_replica->_split_states.is_caught_up);
    }

    {
        learn_state lstate;
        lstate.to_decree_included = 0;
        lstate.files.push_back("./learn_dummy");
        // mock change _app->last_committed_decree
        dsn::error_code error = child_replica->_app->apply_checkpoint(
            replication_app_base::chkpt_apply_mode::copy, lstate);
        ASSERT_EQ(dsn::ERR_OK, error);

        child_replica->_prepare_list = plist2;

        std::cout << "case2. succeed - mutations in files and memory" << std::endl;

        child_replica->child_catch_up();
        substitutes["child_catch_up"] = nullptr;
        child_replica->tracker()->wait_outstanding_tasks();
        substitutes.erase("child_catch_up");
    }

    {
        learn_state lstate;
        lstate.to_decree_included = 7;
        lstate.files.push_back("./learn_dummy");
        dsn::error_code error = child_replica->_app->apply_checkpoint(
            replication_app_base::chkpt_apply_mode::copy, lstate);
        ASSERT_EQ(dsn::ERR_OK, error);

        std::cout << "case3. succeed - mutations all in memory" << std::endl;

        child_replica->child_catch_up();
        child_replica->tracker()->wait_outstanding_tasks();
    }

    substitutes.erase("notify_primary_split_catch_up");
}

void replication_service_test_app::notify_primary_split_catch_up_test()
{

    auto stub = new replica_stub_mock();
    stub->set_address(dsn::rpc_address("127.0.0.1", 8702));

    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_PARTITION_SPLIT);
    child_replica->_split_states.parent_gpid = parent_gpid;
    child_replica->_split_states.is_caught_up = true;
    stub->replicas[child_gpid] = child_replica;

    substitutes["on_notify_primary_split_catch_up"] = nullptr;

    {
        std::cout << "case1. primary is local parent" << std::endl;

        auto parent_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
        stub->replicas[parent_gpid] = parent_replica;
        parent_replica->_child_gpid = child_gpid;

        replica_configuration primary_config = parent_replica->get_config();
        primary_config.primary = dsn::rpc_address("127.0.0.1", 8702);
        child_replica->set_config(primary_config);

        child_replica->notify_primary_split_catch_up();
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        child_replica->_config.status = partition_status::PS_PARTITION_SPLIT;
        parent_replica->_child_gpid = child_gpid;
    }

    {
        std::cout << "case2. primary is remote parent" << std::endl;

        auto primary_stub = new replica_stub_mock();
        auto primary_replica =
            primary_stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
        primary_stub->replicas[parent_gpid] = primary_replica;

        replica_configuration primary_config = primary_replica->get_config();
        primary_config.primary = dsn::rpc_address("127.0.0.2", 8702);
        child_replica->set_config(primary_config);

        child_replica->notify_primary_split_catch_up();
        child_replica->tracker()->wait_outstanding_tasks();
        primary_replica->tracker()->wait_outstanding_tasks();

        child_replica->_config.status = partition_status::PS_PARTITION_SPLIT;
    }

    substitutes.erase("on_notify_primary_split_catch_up");
}

void replication_service_test_app::on_notify_primary_split_catch_up_test()
{
    auto stub = new replica_stub_mock();
    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_PARTITION_SPLIT);
    stub->replicas[child_gpid] = child_replica;

    auto primary_stub = new replica_stub_mock();
    auto primary_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    primary_stub->replicas[parent_gpid] = primary_replica;
    primary_replica->_child_gpid = child_gpid;
    primary_replica->_primary_states.is_sync_to_child = false;
    primary_replica->_primary_states.statuses.insert(
        std::pair<dsn::rpc_address, partition_status::type>(dsn::rpc_address("127.0.0.2", 8702),
                                                            partition_status::PS_PRIMARY));
    primary_replica->_primary_states.statuses.insert(
        std::pair<dsn::rpc_address, partition_status::type>(dsn::rpc_address("127.0.0.1", 8702),
                                                            partition_status::PS_SECONDARY));
    primary_replica->_primary_states.statuses.insert(
        std::pair<dsn::rpc_address, partition_status::type>(dsn::rpc_address("127.0.0.3", 8702),
                                                            partition_status::PS_SECONDARY));

    notify_catch_up_request request;
    request.child_gpid = child_gpid;
    request.child_ballot = BALLOT;
    request.primary_parent_gpid = parent_gpid;
    request.child_address = dsn::rpc_address("127.0.0.1", 8702);

    substitutes["check_sync_point"] = nullptr;

    {
        std::cout << "case1. parent status wrong" << std::endl;

        notify_cacth_up_response response;
        primary_replica->_config.status = partition_status::PS_SECONDARY;
        primary_replica->on_notify_primary_split_catch_up(request, response);
        primary_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(dsn::ERR_INVALID_STATE, response.err);
        primary_replica->_config.status = partition_status::PS_PRIMARY;
    }

    {
        std::cout << "case2. out-dated ballot" << std::endl;

        notify_cacth_up_response response;
        request.child_ballot = 0;
        primary_replica->on_notify_primary_split_catch_up(request, response);
        primary_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(dsn::ERR_INVALID_STATE, response.err);
        request.child_ballot = BALLOT;
    }

    {
        std::cout << "case3. child_gpid not match" << std::endl;

        notify_cacth_up_response response;
        request.child_gpid = dsn::gpid(1, 5);
        primary_replica->on_notify_primary_split_catch_up(request, response);
        primary_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(dsn::ERR_INVALID_STATE, response.err);
        request.child_gpid = child_gpid;
    }

    {
        std::cout << "case4. succeed - not all child catch up" << std::endl;

        notify_cacth_up_response response;
        primary_replica->on_notify_primary_split_catch_up(request, response);
        primary_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(dsn::ERR_OK, response.err);
        ASSERT_FALSE(primary_replica->_primary_states.is_sync_to_child);
    }

    {
        std::cout << "case5. succeed - all child catch up" << std::endl;

        notify_cacth_up_response response;
        primary_replica->_primary_states.child_address.insert(dsn::rpc_address("127.0.0.1", 8702));
        primary_replica->_primary_states.child_address.insert(dsn::rpc_address("127.0.0.2", 8702));
        primary_replica->_primary_states.child_address.insert(dsn::rpc_address("127.0.0.3", 8702));

        primary_replica->on_notify_primary_split_catch_up(request, response);
        primary_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(dsn::ERR_OK, response.err);
        ASSERT_EQ(0, primary_replica->_primary_states.child_address.size());
        ASSERT_TRUE(primary_replica->_primary_states.is_sync_to_child);
    }

    substitutes.erase("check_sync_point");
}

void replication_service_test_app::check_sync_point_test()
{

    auto stub = new replica_stub_mock();
    auto primary_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    stub->replicas[parent_gpid] = primary_replica;

    learn_state lstate;
    lstate.to_decree_included = DECREE;
    lstate.files.push_back("./learn_dummy");
    dsn::error_code error = primary_replica->_app->apply_checkpoint(
        replication_app_base::chkpt_apply_mode::copy, lstate);
    ASSERT_EQ(dsn::ERR_OK, error);

    substitutes["update_group_partition_count"] = nullptr;

    {
        std::cout << "case1. normal case" << std::endl;
        primary_replica->check_sync_point(DECREE);
        primary_replica->tracker()->wait_outstanding_tasks();
    }

    {
        std::cout << "case2. retry case" << std::endl;
        primary_replica->check_sync_point(1);
        substitutes["check_sync_point"] = nullptr;
        primary_replica->tracker()->wait_outstanding_tasks();
        substitutes.erase("check_sync_point");
    }

    substitutes.erase("update_group_partition_count");
}

void replication_service_test_app::update_group_partition_count_test()
{
    auto stub = new replica_stub_mock();
    auto primary_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_PARTITION_SPLIT);
    stub->replicas[parent_gpid] = primary_replica;
    stub->replicas[child_gpid] = child_replica;
    primary_replica->_child_gpid = child_gpid;

    // mock nodes
    primary_replica->_primary_states.statuses.insert(
        std::pair<dsn::rpc_address, partition_status::type>(dsn::rpc_address("127.0.0.1", 8702),
                                                            partition_status::PS_PRIMARY));
    primary_replica->_primary_states.statuses.insert(
        std::pair<dsn::rpc_address, partition_status::type>(dsn::rpc_address("127.0.0.2", 8702),
                                                            partition_status::PS_SECONDARY));
    primary_replica->_primary_states.statuses.insert(
        std::pair<dsn::rpc_address, partition_status::type>(dsn::rpc_address("127.0.0.3", 8702),
                                                            partition_status::PS_SECONDARY));
    // mock primary replica_configuration
    dsn::partition_configuration partition_config;
    partition_config.pid = parent_gpid;
    partition_config.primary = dsn::rpc_address("127.0.0.1", 8702);
    partition_config.ballot = BALLOT;
    primary_replica->_primary_states.membership = partition_config;

    substitutes["on_update_group_partition_count"] = nullptr;
    substitutes["on_update_group_partition_count_reply"] = nullptr;

    {
        std::cout << "case1. child replica is not right one" << std::endl;
        primary_replica->_child_ballot = 0;

        primary_replica->update_group_partition_count(partition_count * 2);
        primary_replica->tracker()->wait_outstanding_tasks();
        child_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(partition_status::PS_ERROR, child_replica->_config.status);
        ASSERT_EQ(0, primary_replica->_child_gpid.get_app_id());

        child_replica->_config.status = partition_status::PS_PARTITION_SPLIT;
        primary_replica->_child_ballot = BALLOT;
        primary_replica->_child_gpid = child_gpid;
    }

    {
        std::cout << "case2. succeed" << std::endl;
        primary_replica->update_group_partition_count(partition_count * 2);
        primary_replica->tracker()->wait_outstanding_tasks();
        child_replica->tracker()->wait_outstanding_tasks();
    }

    substitutes.erase("on_update_group_partition_count");
    substitutes.erase("on_update_group_partition_count_reply");
}

void replication_service_test_app::on_update_group_partition_count_test()
{
    auto stub = new replica_stub_mock();
    auto parent_replica = stub->generate_replica(parent_gpid, partition_status::PS_SECONDARY);
    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_PARTITION_SPLIT);
    stub->replicas[parent_gpid] = parent_replica;
    stub->replicas[child_gpid] = child_replica;
    parent_replica->_child_gpid = child_gpid;
    child_replica->_split_states.parent_gpid = parent_gpid;
    child_replica->_split_states.is_caught_up = true;

    // mock app_info
    dsn::app_info info;
    info.app_id = 1;
    info.partition_count = partition_count * 2;
    // mock replica_configuration
    replica_configuration config;
    config.ballot = BALLOT;
    config.pid = parent_gpid;

    update_group_partition_count_request request;
    request.app = info;
    request.config = config;
    request.last_committed_decree = DECREE;

    {
        std::cout << "case1. out-dated ballot" << std::endl;
        update_group_partition_count_response response;
        request.config.ballot = 0;

        child_replica->on_update_group_partition_count(request, response);
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(dsn::ERR_VERSION_OUTDATED, response.err);
        ASSERT_EQ(partition_status::PS_ERROR, child_replica->_config.status);
        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());

        request.config.ballot = BALLOT;
        child_replica->_config.status = partition_status::PS_PARTITION_SPLIT;
        parent_replica->_child_gpid = child_gpid;
    }

    {
        std::cout << "case2. child not catch up" << std::endl;
        update_group_partition_count_response response;
        child_replica->_split_states.parent_gpid.set_app_id(0);
        child_replica->_split_states.is_caught_up = false;

        child_replica->on_update_group_partition_count(request, response);
        child_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(dsn::ERR_VERSION_OUTDATED, response.err);

        child_replica->_split_states.parent_gpid.set_app_id(1);
        child_replica->_split_states.is_caught_up = true;
    }

    {
        std::cout << "case3. succeed" << std::endl;
        update_group_partition_count_response response;
        child_replica->_app_info = info;
        child_replica->_app_info.partition_count = partition_count;
        ASSERT_EQ(partition_count, child_replica->_app_info.partition_count);

        child_replica->on_update_group_partition_count(request, response);
        child_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(dsn::ERR_OK, response.err);
        ASSERT_EQ(partition_count * 2, child_replica->_app_info.partition_count);
        ASSERT_EQ(partition_count * 2 - 1, child_replica->_partition_version);
    }
}

void replication_service_test_app::on_update_group_partition_count_reply_test()
{
    auto primary_stub = new replica_stub_mock();
    auto primary_replica =
        primary_stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    primary_stub->replicas[parent_gpid] = primary_replica;

    auto stub = new replica_stub_mock();
    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_PARTITION_SPLIT);
    stub->replicas[child_gpid] = child_replica;

    // mock app
    dsn::app_info info;
    info.app_id = 1;
    info.partition_count = partition_count * 2;
    // mock config
    replica_configuration config;
    config.ballot = BALLOT;
    config.pid = parent_gpid;

    dsn::error_code ec = dsn::ERR_OK;

    std::shared_ptr<update_group_partition_count_request> request(
        new update_group_partition_count_request);
    request->app = info;
    request->config = config;
    request->last_committed_decree = DECREE;
    request->target_address = dsn::rpc_address("127.0.0.3", 8703);

    std::shared_ptr<update_group_partition_count_response> response(
        new update_group_partition_count_response);
    response->err = dsn::ERR_OK;

    std::shared_ptr<std::set<dsn::rpc_address>> replica_addresses =
        std::make_shared<std::set<dsn::rpc_address>>();
    replica_addresses->insert(dsn::rpc_address("127.0.0.3", 8703));
    replica_addresses->insert(dsn::rpc_address("127.0.0.2", 8703));
    replica_addresses->insert(dsn::rpc_address("127.0.0.1", 8703));

    {
        std::cout << "case1. replica is not primary" << std::endl;
        auto child_address = dsn::rpc_address("127.0.0.3", 8703);
        primary_replica->_config.status = partition_status::PS_INACTIVE;

        primary_replica->on_update_group_partition_count_reply(
            ec, request, response, replica_addresses, child_address);
        primary_replica->tracker()->wait_outstanding_tasks();

        primary_replica->_config.status = partition_status::PS_PRIMARY;
    }

    {
        std::cout << "case2. ballot not match" << std::endl;
        auto child_address = dsn::rpc_address("127.0.0.3", 8703);
        request->config.ballot = 0;

        primary_replica->on_update_group_partition_count_reply(
            ec, request, response, replica_addresses, child_address);
        primary_replica->tracker()->wait_outstanding_tasks();

        request->config.ballot = BALLOT;
    }

    {
        std::cout << "case3. retry case" << std::endl;
        auto child_address = dsn::rpc_address("127.0.0.3", 8703);
        response->err = dsn::ERR_VERSION_OUTDATED;

        primary_replica->on_update_group_partition_count_reply(
            ec, request, response, replica_addresses, child_address);

        substitutes["on_update_group_partition_count"] = nullptr;
        substitutes["on_update_group_partition_count_reply"] = nullptr;

        primary_replica->tracker()->wait_outstanding_tasks();

        response->err = dsn::ERR_OK;
        substitutes.erase("on_update_group_partition_count");
        substitutes.erase("on_update_group_partition_count_reply");
    }

    {
        std::cout << "case4. not all update partition count" << std::endl;
        auto child_address = dsn::rpc_address("127.0.0.3", 8703);
        primary_replica->on_update_group_partition_count_reply(
            ec, request, response, replica_addresses, child_address);
        primary_replica->tracker()->wait_outstanding_tasks();
    }

    {
        std::cout << "case5. succeed" << std::endl;
        auto child_address = dsn::rpc_address("127.0.0.1", 8703);
        replica_addresses->erase(dsn::rpc_address("127.0.0.2", 8703));
        primary_replica->on_update_group_partition_count_reply(
            ec, request, response, replica_addresses, child_address);
        primary_replica->tracker()->wait_outstanding_tasks();
    }
}
