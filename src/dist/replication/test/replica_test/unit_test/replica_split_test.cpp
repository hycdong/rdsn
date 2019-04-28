#include <gtest/gtest.h>
#include <dsn/service_api_c.h>

#include "dist/replication/lib/replica.h"
#include "dist/replication/lib/replica_stub.h"

#include "replication_service_test_app.h"
#include "replica_split_mock.h"

using namespace ::dsn::replication;

void replication_service_test_app::on_add_child_test()
{
    auto stub = new replica_stub_mock();
    auto parent_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);

    group_check_request request;
    request.child_gpid = child_gpid;
    request.config.ballot = BALLOT;
    ASSERT_EQ(nullptr, stub->get_replica(child_gpid, false));
    mock_funcs["init_child_replica"] = nullptr;

    std::cout << "case1. failed - ballot not match" << std::endl;
    {
        request.config.ballot = BALLOT + 1;
        parent_replica->on_add_child(request);
        parent_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(nullptr, stub->get_replica(child_gpid, false));
        request.config.ballot = BALLOT;
    }

    std::cout << "case2. failed - child_gpid alreay exist" << std::endl;
    {        
        parent_replica->_child_gpid = child_gpid;
        parent_replica->on_add_child(request);
        parent_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(nullptr, stub->get_replica(child_gpid, false));
    }

    std::cout << "case3. failed - partition count not match" << std::endl;
    {
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

    std::cout << "cas4. succeed - add child" << std::endl;
    {
        parent_replica->_child_gpid.set_app_id(0);
        parent_replica->on_add_child(request);
        parent_replica->tracker()->wait_outstanding_tasks();
        replica_ptr child = stub->get_replica(child_gpid, false);
        ASSERT_NE(nullptr, child);
        ASSERT_EQ(child_gpid, child->get_gpid());
        ASSERT_EQ(BALLOT, child->get_ballot());
        ASSERT_EQ(partition_count, child->get_app_info()->partition_count);
    }

    mock_funcs.erase("init_child_replica");
}

void replication_service_test_app::init_child_replica_test()
{
    auto stub = new replica_stub_mock();
    auto parent_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_INACTIVE);
    mock_funcs["prepare_copy_parent_state"] = nullptr;
    mock_funcs["check_child_state"] = nullptr;

    std::cout << "case1. failed - child status is not inactive" << std::endl;
    {
        stub->replicas[parent_gpid] = parent_replica;
        stub->replicas[child_gpid] = child_replica;
        parent_replica->_child_gpid = child_gpid;
        child_replica->_config.status = partition_status::PS_SECONDARY;

        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT);
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());
    }

    std::cout << "case2. succeed - init child replica" << std::endl;
    {
        child_replica->_config.status = partition_status::PS_INACTIVE;

        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT);
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(partition_status::PS_PARTITION_SPLIT, child_replica->_config.status);
        ASSERT_EQ(parent_gpid, child_replica->_split_states.parent_gpid);
        ASSERT_FALSE(child_replica->_split_states.is_prepare_list_copied);
        ASSERT_FALSE(child_replica->_split_states.is_caught_up);
    }

    mock_funcs.erase("prepare_copy_parent_state");
    mock_funcs.erase("check_child_state");
}

void replication_service_test_app::check_child_state_test()
{
    auto stub = new replica_stub_mock();
    auto parent_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_INACTIVE);
    stub->replicas[parent_gpid] = parent_replica;
    stub->replicas[child_gpid] = child_replica;
    parent_replica->_child_gpid = child_gpid;
    mock_funcs["prepare_copy_parent_state"] = nullptr;

    std::cout << "case1. failed - invalid child state" << std::endl;
    {
        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT);
        child_replica->_config.status = partition_status::PS_INACTIVE;
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(nullptr, child_replica->_split_states.check_state_task);
    }

    std::cout << "case2. failed - invalid ballot" << std::endl;
    {
        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT + 1);
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(partition_status::PS_ERROR, child_replica->_config.status);
        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());
        parent_replica->_child_gpid = child_gpid;
    }

    std::cout << "case3. failed - invalid child gpid" << std::endl;
    {
        child_replica->_config.status = partition_status::PS_INACTIVE;
        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT);
        parent_replica->_child_gpid = parent_gpid;
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(partition_status::PS_ERROR, child_replica->_config.status);
        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());
        parent_replica->_child_gpid = child_gpid;
    }

    std::cout << "case4. failed - invalid parent state" << std::endl;
    {
        child_replica->_config.status = partition_status::PS_INACTIVE;
        child_replica->init_child_replica(parent_gpid, ::dsn::rpc_address(), BALLOT);
        parent_replica->_config.status = partition_status::PS_POTENTIAL_SECONDARY;
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(partition_status::PS_ERROR, child_replica->_config.status);
        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());
        parent_replica->_child_gpid = child_gpid;
    }

    mock_funcs.erase("prepare_copy_parent_state");
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
    prepare_list *plist_mock = new prepare_list(parent_replica, 0, 10, nullptr);
    for (int i = 0; i < 10; ++i) {
        mutation_ptr mu = new mutation();
        mu->data.header.decree = i;
        mu->data.header.ballot = BALLOT;
        plist_mock->put(mu);
        parent_replica->_private_log->append(
            mu, LPC_WRITE_REPLICATION_LOG_COMMON, nullptr, nullptr);
    }

    parent_replica->_prepare_list = plist_mock;
    mock_funcs["copy_parent_state"] = nullptr;

    std::cout << "case1. failed - invalid parent state" << std::endl;
    {
        parent_replica->_config.status = partition_status::PS_POTENTIAL_SECONDARY;

        const std::string &dir = child_replica->_app->learn_dir();
        parent_replica->prepare_copy_parent_state(dir, child_gpid, child_replica->get_ballot());
        parent_replica->tracker()->wait_outstanding_tasks();
        child_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());

        parent_replica->_config.status = partition_status::PS_PRIMARY;
        parent_replica->_child_gpid = child_gpid;
    }

    std::cout << "case2. succeed" << std::endl;
    {
        const std::string &dir = child_replica->_app->learn_dir();
        parent_replica->prepare_copy_parent_state(dir, child_gpid, child_replica->get_ballot());
        parent_replica->tracker()->wait_outstanding_tasks();
        child_replica->tracker()->wait_outstanding_tasks();
    }

    mock_funcs.erase("copy_parent_state");
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
    uint64_t total_file_size = 0;

    // mock prepare list and mutation_list
    std::vector<mutation_ptr> mutation_list;
    prepare_list *plist_mock = new prepare_list(parent_replica, 0, 10, nullptr);
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

    mock_funcs["apply_parent_state"] = nullptr;

    std::cout << "case1. failed - invalid child state" << std::endl;
    {
        child_replica->_config.status = partition_status::PS_SECONDARY;

        child_replica->copy_parent_state(
            ec, lstate, mutation_list, files, total_file_size, plist_mock);
        child_replica->tracker()->wait_outstanding_tasks();

        child_replica->_config.status = partition_status::PS_PARTITION_SPLIT;
    }

    std::cout << "case2. failed - error code is not ERR_OK" << std::endl;
    {
        ec = dsn::ERR_GET_LEARN_STATE_FAILED;
        mock_funcs["prepare_copy_parent_state"] = nullptr;

        child_replica->copy_parent_state(
            ec, lstate, mutation_list, files, total_file_size, plist_mock);
        parent_replica->tracker()->wait_outstanding_tasks();
        child_replica->tracker()->wait_outstanding_tasks();

        ec = dsn::ERR_OK;
        mock_funcs.erase("prepare_copy_parent_state");
    }

    std::cout << "case3. succeed" << std::endl;
    {
        child_replica->copy_parent_state(
            ec, lstate, mutation_list, files, total_file_size, plist_mock);
        child_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(true, child_replica->_split_states.is_prepare_list_copied);
        ASSERT_NE(nullptr, child_replica->_split_states.async_learn_task);
    }

    mock_funcs.erase("apply_parent_state");
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
    uint64_t total_file_size = 0;

    // mock prepare list and mutation_list
    std::vector<mutation_ptr> mutation_list;
    prepare_list *plist_mock = new prepare_list(child_replica, 0, 10, nullptr);

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
        std::vector<mutation_ptr>, std::vector<std::string>, uint64_t, decree, bool)>
        mock_async_learn_mutation_private_log =
            [child_replica](std::vector<mutation_ptr> mutation_list,
                            std::vector<std::string> files,
                            uint64_t total_file_size,
                            decree last_committed_decree,
                            bool is_mock_failure) {

                prepare_list plist(child_replica,
                                   child_replica->_app->last_committed_decree(),
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

    mock_funcs["async_learn_mutation_private_log"] = &mock_async_learn_mutation_private_log;
    mock_funcs["child_catch_up"] = nullptr;

    std::cout << "case1. succeed" << std::endl;
    {
        child_replica->apply_parent_state(
            ec, lstate, mutation_list, files, total_file_size, DECREE);
        child_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(nullptr, child_replica->_split_states.async_learn_task);
        ASSERT_EQ(BALLOT, child_replica->get_ballot());
    }

    std::cout << "case2. child replica status wrong" << std::endl;
    {
        child_replica->_config.status = partition_status::PS_SECONDARY;

        child_replica->apply_parent_state(
            ec, lstate, mutation_list, files, total_file_size, DECREE);
        child_replica->tracker()->wait_outstanding_tasks();

        child_replica->_config.status = partition_status::PS_PARTITION_SPLIT;
    }

    std::cout << "case3. checkpoint failed" << std::endl;
    {
        mock_funcs["async_learn_mutation_private_log_mock_failure"] = nullptr;

        child_replica->apply_parent_state(
            ec, lstate, mutation_list, files, total_file_size, DECREE);
        child_replica->tracker()->wait_outstanding_tasks();
        parent_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(partition_status::PS_ERROR, child_replica->_config.status);
        ASSERT_EQ(0, parent_replica->_child_gpid.get_app_id());

        mock_funcs.erase("async_learn_mutation_private_log_mock_failure");
    }

    mock_funcs.erase("async_learn_mutation_private_log");
    mock_funcs.erase("child_catch_up");
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
    prepare_list *plist = new prepare_list(child_replica, 0, 10, nullptr);
    prepare_list *plist2 = new prepare_list(child_replica, 6, 10, nullptr);
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

    mock_funcs["notify_primary_split_catch_up"] = nullptr;

    std::cout << "case1. succeed - no missing mutations" << std::endl;
    {
        child_replica->child_catch_up();
        child_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(true, child_replica->_split_states.is_caught_up);
    }

    std::cout << "case2. succeed - mutations in files and memory" << std::endl;
    {
        learn_state lstate;
        lstate.to_decree_included = 0;
        lstate.files.push_back("./learn_dummy");
        // mock change _app->last_committed_decree
        dsn::error_code error = child_replica->_app->apply_checkpoint(
            replication_app_base::chkpt_apply_mode::copy, lstate);
        ASSERT_EQ(dsn::ERR_OK, error);

        child_replica->_prepare_list = plist2;

        child_replica->child_catch_up();
        mock_funcs["child_catch_up"] = nullptr;
        child_replica->tracker()->wait_outstanding_tasks();
        mock_funcs.erase("child_catch_up");
    }

    std::cout << "case3. succeed - mutations all in memory" << std::endl;
    {
        learn_state lstate;
        lstate.to_decree_included = 7;
        lstate.files.push_back("./learn_dummy");
        dsn::error_code error = child_replica->_app->apply_checkpoint(
            replication_app_base::chkpt_apply_mode::copy, lstate);
        ASSERT_EQ(dsn::ERR_OK, error);

        child_replica->child_catch_up();
        child_replica->tracker()->wait_outstanding_tasks();
    }

    mock_funcs.erase("notify_primary_split_catch_up");
}

void replication_service_test_app::notify_primary_split_catch_up_test()
{

    auto stub = new replica_stub_mock();
    stub->set_address(dsn::rpc_address("127.0.0.1", 8702));

    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_PARTITION_SPLIT);
    child_replica->_split_states.parent_gpid = parent_gpid;
    child_replica->_split_states.is_caught_up = true;
    stub->replicas[child_gpid] = child_replica;

    mock_funcs["on_notify_primary_split_catch_up"] = nullptr;

    std::cout << "case1. primary is local parent" << std::endl;
    {
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

    std::cout << "case2. primary is remote parent" << std::endl;
    {
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

    mock_funcs.erase("on_notify_primary_split_catch_up");
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

    mock_funcs["check_sync_point"] = nullptr;

    std::cout << "case1. parent status wrong" << std::endl;
    {
        notify_cacth_up_response response;
        primary_replica->_config.status = partition_status::PS_SECONDARY;
        primary_replica->on_notify_primary_split_catch_up(request, response);
        primary_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(dsn::ERR_INVALID_STATE, response.err);
        primary_replica->_config.status = partition_status::PS_PRIMARY;
    }

    std::cout << "case2. out-dated ballot" << std::endl;
    {
        notify_cacth_up_response response;
        request.child_ballot = 0;
        primary_replica->on_notify_primary_split_catch_up(request, response);
        primary_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(dsn::ERR_INVALID_STATE, response.err);
        request.child_ballot = BALLOT;
    }

    std::cout << "case3. child_gpid not match" << std::endl;
    {
        notify_cacth_up_response response;
        request.child_gpid = dsn::gpid(1, 5);
        primary_replica->on_notify_primary_split_catch_up(request, response);
        primary_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(dsn::ERR_INVALID_STATE, response.err);
        request.child_gpid = child_gpid;
    }

    std::cout << "case4. succeed - not all child catch up" << std::endl;
    {
        notify_cacth_up_response response;
        primary_replica->on_notify_primary_split_catch_up(request, response);
        primary_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(dsn::ERR_OK, response.err);
        ASSERT_FALSE(primary_replica->_primary_states.is_sync_to_child);
    }

    std::cout << "case5. succeed - all child catch up" << std::endl;
    {
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

    mock_funcs.erase("check_sync_point");
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

    mock_funcs["update_group_partition_count"] = nullptr;

    std::cout << "case1. normal case" << std::endl;
    {
        primary_replica->check_sync_point(DECREE);
        primary_replica->tracker()->wait_outstanding_tasks();
    }

    std::cout << "case2. retry case" << std::endl;
    {
        primary_replica->check_sync_point(1);
        mock_funcs["check_sync_point"] = nullptr;
        primary_replica->tracker()->wait_outstanding_tasks();
        mock_funcs.erase("check_sync_point");
    }

    mock_funcs.erase("update_group_partition_count");
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

    mock_funcs["on_update_group_partition_count"] = nullptr;
    mock_funcs["on_update_group_partition_count_reply"] = nullptr;

    std::cout << "case1. child replica is not right one" << std::endl;
    {
        primary_replica->_child_ballot = 0;

        primary_replica->update_group_partition_count(partition_count * 2, true);
        primary_replica->tracker()->wait_outstanding_tasks();
        child_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(partition_status::PS_ERROR, child_replica->_config.status);
        ASSERT_EQ(0, primary_replica->_child_gpid.get_app_id());

        child_replica->_config.status = partition_status::PS_PARTITION_SPLIT;
        primary_replica->_child_ballot = BALLOT;
        primary_replica->_child_gpid = child_gpid;
    }

    std::cout << "case2. succeed" << std::endl;
    {
        primary_replica->update_group_partition_count(partition_count * 2, true);
        primary_replica->tracker()->wait_outstanding_tasks();
        child_replica->tracker()->wait_outstanding_tasks();
    }

    mock_funcs.erase("on_update_group_partition_count");
    mock_funcs.erase("on_update_group_partition_count_reply");
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

    std::cout << "case1. out-dated ballot" << std::endl;
    {
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

    std::cout << "case2. child not catch up" << std::endl;
    {
        update_group_partition_count_response response;
        child_replica->_split_states.is_caught_up = false;

        child_replica->on_update_group_partition_count(request, response);
        child_replica->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(dsn::ERR_VERSION_OUTDATED, response.err);

        child_replica->_split_states.is_caught_up = true;
    }

    std::cout << "case3. succeed" << std::endl;
    {
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

    mock_funcs["register_child_on_meta"] = nullptr;

    std::cout << "case1. replica is not primary" << std::endl;
    {
        auto child_address = dsn::rpc_address("127.0.0.3", 8703);
        primary_replica->_config.status = partition_status::PS_INACTIVE;

        primary_replica->on_update_group_partition_count_reply(
            ec, request, response, replica_addresses, child_address, true);
        primary_replica->tracker()->wait_outstanding_tasks();

        primary_replica->_config.status = partition_status::PS_PRIMARY;
    }

    std::cout << "case2. ballot not match" << std::endl;
    {
        auto child_address = dsn::rpc_address("127.0.0.3", 8703);
        request->config.ballot = 0;

        primary_replica->on_update_group_partition_count_reply(
            ec, request, response, replica_addresses, child_address, true);
        primary_replica->tracker()->wait_outstanding_tasks();

        request->config.ballot = BALLOT;
    }

    std::cout << "case3. retry case" << std::endl;
    {
        auto child_address = dsn::rpc_address("127.0.0.3", 8703);
        response->err = dsn::ERR_VERSION_OUTDATED;

        primary_replica->on_update_group_partition_count_reply(
            ec, request, response, replica_addresses, child_address, true);

        mock_funcs["on_update_group_partition_count"] = nullptr;
        mock_funcs["on_update_group_partition_count_reply"] = nullptr;

        primary_replica->tracker()->wait_outstanding_tasks();

        response->err = dsn::ERR_OK;
        mock_funcs.erase("on_update_group_partition_count");
        mock_funcs.erase("on_update_group_partition_count_reply");
    }

    std::cout << "case4. not all update partition count" << std::endl;
    {
        auto child_address = dsn::rpc_address("127.0.0.3", 8703);
        primary_replica->on_update_group_partition_count_reply(
            ec, request, response, replica_addresses, child_address, true);
        primary_replica->tracker()->wait_outstanding_tasks();
    }

    std::cout << "case5. succeed child group" << std::endl;
    {
        auto child_address = dsn::rpc_address("127.0.0.1", 8703);
        replica_addresses->erase(dsn::rpc_address("127.0.0.2", 8703));
        primary_replica->on_update_group_partition_count_reply(
            ec, request, response, replica_addresses, child_address, true);
        primary_replica->tracker()->wait_outstanding_tasks();
    }

    mock_funcs.erase("register_child_on_meta");

    std::cout << "case6. succeed parent group" << std::endl;
    {
        auto child_address = dsn::rpc_address("127.0.0.1", 8705);
        replica_addresses->insert(dsn::rpc_address("127.0.0.1", 8705));
        primary_replica->on_update_group_partition_count_reply(
            ec, request, response, replica_addresses, child_address, false);
        primary_replica->tracker()->wait_outstanding_tasks();
    }
}

void replication_service_test_app::register_child_on_meta_test()
{
    auto stub = new replica_stub_mock();
    auto primary_replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_PARTITION_SPLIT);
    stub->replicas[parent_gpid] = primary_replica;
    stub->replicas[child_gpid] = child_replica;
    stub->set_meta_server(dsn::rpc_address("127.0.0.1", 8705));

    mock_funcs["on_register_child_on_meta_reply"] = nullptr;

    std::cout << "case1. replica not primary" << std::endl;
    {
        primary_replica->_config.status = partition_status::PS_SECONDARY;
        primary_replica->register_child_on_meta(BALLOT);
        primary_replica->tracker()->wait_outstanding_tasks();
        primary_replica->_config.status = partition_status::PS_PRIMARY;
    }

    std::cout << "case2. replica is reconfiguration" << std::endl;
    {
        dsn::task_ptr fake_reconfig_ptr = dsn::tasking::enqueue(
            LPC_SPLIT_PARTITION,
            primary_replica->tracker(),
            []() { std::cout << "This is a fake reconfiguration task_ptr" << std::endl; },
            parent_gpid.thread_hash());
        primary_replica->_primary_states.reconfiguration_task = fake_reconfig_ptr;
        primary_replica->register_child_on_meta(BALLOT);

        mock_funcs["register_child_on_meta"] = nullptr;

        primary_replica->tracker()->wait_outstanding_tasks();
        primary_replica->_primary_states.reconfiguration_task = nullptr;

        mock_funcs.erase("register_child_on_meta");
    }

    std::cout << "case3. ballot not match" << std::endl;
    {
        primary_replica->register_child_on_meta(0);
        primary_replica->tracker()->wait_outstanding_tasks();
    }

    std::cout << "case4. succeed" << std::endl;
    {
        primary_replica->register_child_on_meta(1);
        primary_replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(partition_status::PS_INACTIVE, primary_replica->_config.status);

        primary_replica->_config.status = partition_status::PS_PRIMARY;
    }

    mock_funcs.erase("on_register_child_on_meta_reply");
}

void replication_service_test_app::on_register_child_on_meta_reply_test()
{
    auto stub = new replica_stub_mock();
    auto primary_parent = stub->generate_replica(parent_gpid, partition_status::PS_INACTIVE);
    stub->replicas[parent_gpid] = primary_parent;
    stub->set_connected();
    stub->set_meta_server(dsn::rpc_address("127.0.0.1", 8706));
    primary_parent->_child_gpid = child_gpid;
    primary_parent->_primary_states.register_child_task = dsn::tasking::enqueue(
        LPC_SPLIT_PARTITION,
        primary_parent->tracker(),
        []() { std::cout << "This is a mock register child task" << std::endl; },
        primary_parent->get_gpid().thread_hash());

    dsn::app_info app;
    app.app_id = parent_gpid.get_app_id();
    app.partition_count = partition_count;
    primary_parent->_app_info = app;
    app.partition_count *= 2;

    dsn::partition_configuration parent_config;
    parent_config.ballot = BALLOT;
    parent_config.last_committed_decree = DECREE;
    parent_config.pid = parent_gpid;

    dsn::partition_configuration child_config = parent_config;
    child_config.ballot++;
    child_config.last_committed_decree = 0;
    child_config.pid = child_gpid;

    std::shared_ptr<register_child_request> request(new register_child_request);
    request->app = app;
    request->child_config = child_config;
    request->parent_config = parent_config;

    std::shared_ptr<register_child_response> response(new register_child_response);
    response->err = dsn::ERR_OK;
    response->app = app;
    response->child_config = child_config;
    response->parent_config = parent_config;

    dsn::error_code ec = dsn::ERR_OK;

    mock_funcs["update_group_partition_count"] = nullptr;

    std::cout << "case1. wrong status" << std::endl;
    {
        primary_parent->_config.status = partition_status::PS_PRIMARY;

        primary_parent->on_register_child_on_meta_reply(ec, request, response);
        primary_parent->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(nullptr, primary_parent->_primary_states.register_child_task);
        primary_parent->_config.status = partition_status::PS_INACTIVE;
    }

    std::cout << "case2. retry case" << std::endl;
    {
        ec = dsn::ERR_TIMEOUT;

        primary_parent->on_register_child_on_meta_reply(ec, request, response);

        std::function<void(dsn::error_code,
                           std::shared_ptr<register_child_request>,
                           std::shared_ptr<register_child_response>)>
            mock_reply = [](dsn::error_code ec,
                            std::shared_ptr<register_child_request> request,
                            std::shared_ptr<register_child_response> response) {
                std::cout << "Mock on_register_child_on_meta_reply is called" << std::endl;
            };
        mock_funcs["on_register_child_on_meta_reply"] = &mock_reply;

        primary_parent->tracker()->wait_outstanding_tasks();

        ec = dsn::ERR_OK;
        mock_funcs.erase("on_register_child_on_meta_reply");
    }

    std::cout << "case3. child registered" << std::endl;
    {
        response->err = dsn::ERR_CHILD_REGISTERED;

        primary_parent->on_register_child_on_meta_reply(ec, request, response);
        primary_parent->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(nullptr, primary_parent->_primary_states.register_child_task);
        ASSERT_TRUE(primary_parent->_primary_states.is_sync_to_child);
        ASSERT_EQ(0, primary_parent->_child_gpid.get_app_id());

        response->err = dsn::ERR_OK;
        primary_parent->_child_gpid = child_gpid;
    }

    std::cout << "case4. split paused" << std::endl;
    {
        response->err = dsn::ERR_CHILD_DROPPED;

        primary_parent->on_register_child_on_meta_reply(ec, request, response);
        primary_parent->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(nullptr, primary_parent->_primary_states.register_child_task);
        ASSERT_TRUE(primary_parent->_primary_states.is_sync_to_child);
        ASSERT_EQ(0, primary_parent->_child_gpid.get_app_id());

        response->err = dsn::ERR_OK;
        primary_parent->_child_gpid = child_gpid;
    }

    std::cout << "case5. split canceled" << std::endl;
    {
        response->err = dsn::ERR_REJECT;

        primary_parent->on_register_child_on_meta_reply(ec, request, response);
        primary_parent->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(nullptr, primary_parent->_primary_states.register_child_task);
        ASSERT_TRUE(primary_parent->_primary_states.is_sync_to_child);
        ASSERT_EQ(0, primary_parent->_child_gpid.get_app_id());

        response->err = dsn::ERR_OK;
        primary_parent->_child_gpid = child_gpid;
    }


    std::cout << "case6. succeed case" << std::endl;
    {
        primary_parent->on_register_child_on_meta_reply(ec, request, response);
        primary_parent->tracker()->wait_outstanding_tasks();

        ASSERT_EQ(nullptr, primary_parent->_primary_states.register_child_task);
        ASSERT_TRUE(primary_parent->_primary_states.is_sync_to_child);
        ASSERT_EQ(0, primary_parent->_child_gpid.get_app_id());
    }

   mock_funcs.erase("update_group_partition_count");
}

void replication_service_test_app::check_partition_state_test()
{
    // not use parent_gpid, coz it may be registered
    dsn::gpid pid = dsn::gpid(1,0);
    auto stub = new replica_stub_mock();
    auto replica = stub->generate_replica(pid, partition_status::PS_PRIMARY);
    stub->replicas[pid] = replica;

    dsn::app_info info;
    info.app_id = 1;
    info.partition_count = partition_count;
    replica->_app_info = info;

    dsn::partition_configuration config;
    config.pid = pid;
    config.partition_flags &= (~pc_flags::child_dropped);

    mock_funcs["query_child_state"] = nullptr;

    std::cout << "case1. equal partition count" << std::endl;
    {
        replica->check_partition_state(partition_count, config);
    }

    std::cout << "case2. child_gpid valid" << std::endl;
    {
        replica->_child_gpid = child_gpid;
        replica->check_partition_state(partition_count * 2, config);
        replica->_child_gpid.set_app_id(0);
    }

    std::cout << "case3. cancel split while no child register" << std::endl;
    {
        replica->_child_gpid = child_gpid;
        replica->check_partition_state(partition_count, config);
        ASSERT_EQ(0, replica->_child_gpid.get_app_id());
    }

    std::cout << "case4. pause split" << std::endl;
    {
        config.partition_flags |= pc_flags::child_dropped;
        replica->check_partition_state(partition_count * 2, config);
        ASSERT_EQ(0, replica->_child_gpid.get_app_id());
        config.partition_flags &= (~pc_flags::child_dropped);
    }

    mock_funcs["update_group_partition_count"] = nullptr;

    std::cout << "case5. cancel split with parent partition" << std::endl;
    {
        replica->check_partition_state(partition_count / 2, config);
    }

    std::cout << "case6. pass check for partition split" << std::endl;
    {
        replica->check_partition_state(partition_count * 2, config);
        ASSERT_EQ(-1, replica->_partition_version);
    }

    std::cout << "case7. cancel split with child partition" << std::endl;
    {
        auto child_replica = stub->generate_replica(child_gpid, partition_status::PS_PRIMARY);
        stub->replicas[child_gpid] = child_replica;
        child_replica->check_partition_state(partition_count / 2, config);
        ASSERT_EQ(child_replica->status(), partition_status::PS_ERROR);
    }

    mock_funcs.erase("query_child_state");
    mock_funcs.erase("update_group_partition_count");
}

void replication_service_test_app::query_child_state_test()
{
    auto stub = new replica_stub_mock();
    auto replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    stub->replicas[parent_gpid] = replica;
    stub->set_meta_server(dsn::rpc_address("127.0.0.1", 8710));

    mock_funcs["on_query_child_state_reply"] = nullptr;

    std::cout << "case1. replica is not primary" << std::endl;
    {
        replica->_config.status = partition_status::PS_PARTITION_SPLIT;
        replica->query_child_state();
        replica->tracker()->wait_outstanding_tasks();
        replica->_config.status = partition_status::PS_PRIMARY;
    }

    std::cout << "case2. query_child_state_task is not nullptr" << std::endl;
    {
        replica->_primary_states.query_child_state_task = dsn::tasking::enqueue(
            LPC_SPLIT_PARTITION,
            replica->tracker(),
            []() { std::cout << "This is mock query_child_state_task ptr" << std::endl; },
            parent_gpid.thread_hash());
        replica->query_child_state();
        replica->tracker()->wait_outstanding_tasks();
        replica->_primary_states.query_child_state_task = nullptr;
    }

    std::cout << "case3. succeed" << std::endl;
    {
        replica->query_child_state();
        replica->tracker()->wait_outstanding_tasks();
    }

    mock_funcs.erase("on_query_child_state_reply");
}

void replication_service_test_app::on_query_child_state_reply_test()
{
    auto stub = new replica_stub_mock();
    auto replica = stub->generate_replica(parent_gpid, partition_status::PS_PRIMARY);
    replica->_app_info.partition_count = partition_count;
    stub->replicas[parent_gpid] = replica;
    stub->set_meta_server(dsn::rpc_address("127.0.0.1", 8710));

    dsn::error_code ec = dsn::ERR_OK;

    std::shared_ptr<query_child_state_request> request(new query_child_state_request);
    request->parent_gpid = parent_gpid;

    std::shared_ptr<query_child_state_response> response(new query_child_state_response);
    response->err = dsn::ERR_OK;
    response->ballot = invalid_ballot;
    response->partition_count = partition_count * 2;

    std::cout << "case1. replica is not primary" << std::endl;
    {
        replica->_config.status = partition_status::PS_PARTITION_SPLIT;
        replica->on_query_child_state_reply(ec, request, response);
        replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(nullptr, replica->_primary_states.query_child_state_task);
        replica->_config.status = partition_status::PS_PRIMARY;
    }

    std::cout << "case2. child gpid exist" << std::endl;
    {
        replica->_child_gpid = child_gpid;
        replica->on_query_child_state_reply(ec, request, response);
        replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(nullptr, replica->_primary_states.query_child_state_task);
        replica->_child_gpid.set_app_id(0);
    }

    std::cout << "case3. retry case" << std::endl;
    {
        std::function<void(dsn::error_code,
                           std::shared_ptr<query_child_state_request>,
                           std::shared_ptr<query_child_state_response>)>
            mock_func = [replica](dsn::error_code err,
                                      std::shared_ptr<query_child_state_request> req,
                                      std::shared_ptr<query_child_state_response> resp) {
                std::cout << "This is mock function of on_query_child_state_reply" << std::endl;
            };

        response->err = dsn::ERR_TRY_AGAIN;
        replica->on_query_child_state_reply(ec, request, response);

        mock_funcs["on_query_child_state_reply"] = &mock_func;

        replica->tracker()->wait_outstanding_tasks();

        response->err = dsn::ERR_OK;
        mock_funcs.erase("on_query_child_state_reply");
    }

    std::cout << "case4. equal partition count, partition split finish" << std::endl;
    {
        response->ballot = 2;
        replica->_app_info.partition_count = partition_count * 2;

        replica->on_query_child_state_reply(ec, request, response);
        replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(partition_count * 2 - 1, replica->_partition_version);

        response->ballot = invalid_ballot;
        replica->_app_info.partition_count = partition_count;
    }

    std::cout << "case5. ballot is valid" << std::endl;
    {
        response->ballot = 2;
        mock_funcs["update_group_partition_count"] = nullptr;

        replica->on_query_child_state_reply(ec, request, response);
        replica->tracker()->wait_outstanding_tasks();

        response->ballot = invalid_ballot;
        mock_funcs.erase("update_group_partition_count");
    }

    std::cout << "case6. learner exist" << std::endl;
    {
        dsn::partition_configuration config;
        config.max_replica_count = 3;
        config.secondaries.push_back(dsn::rpc_address("127.0.0.2", 8710));
        replica->_primary_states.membership = config;

        replica->on_query_child_state_reply(ec, request, response);
        replica->tracker()->wait_outstanding_tasks();
        ASSERT_EQ(partition_count - 1, replica->_partition_version);

        replica->_primary_states.membership.secondaries.push_back(
            dsn::rpc_address("127.0.0.3", 8710));
    }
}
