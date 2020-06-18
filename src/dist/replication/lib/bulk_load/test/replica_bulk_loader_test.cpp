// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "dist/replication/lib/bulk_load/replica_bulk_loader.h"
#include "dist/replication/test/replica_test/unit_test/replica_test_base.h"

#include <fstream>

#include <dsn/utility/fail_point.h>
#include <gtest/gtest.h>

namespace dsn {
namespace replication {

class replica_bulk_loader_test : public replica_test_base
{
public:
    replica_bulk_loader_test()
    {
        _replica = create_mock_replica(stub.get());
        _bulk_loader = make_unique<replica_bulk_loader>(_replica.get());
        fail::setup();
    }

    ~replica_bulk_loader_test() { fail::teardown(); }

public:
    /// test helper functions
    error_code test_on_bulk_load()
    {
        bulk_load_response resp;
        _bulk_loader->on_bulk_load(_req, resp);
        return resp.err;
    }

    error_code test_on_group_bulk_load(bulk_load_status::type status, ballot b)
    {
        create_group_bulk_load_request(status, b);
        group_bulk_load_response resp;
        _bulk_loader->on_group_bulk_load(_group_req, resp);
        return resp.err;
    }

    error_code test_parse_bulk_load_metadata(const std::string &file_path)
    {
        return _bulk_loader->parse_bulk_load_metadata(file_path);
    }

    error_code test_start_downloading()
    {
        return _bulk_loader->start_download(APP_NAME, CLUSTER, PROVIDER);
    }

    void test_rollback_to_downloading(bulk_load_status::type cur_status)
    {
        switch (cur_status) {
        case bulk_load_status::BLS_DOWNLOADING:
            mock_group_progress(bulk_load_status::BLS_DOWNLOADING, 30, 30, 30);
            break;
        case bulk_load_status::BLS_DOWNLOADED:
            mock_group_progress(bulk_load_status::BLS_DOWNLOADED);
            break;
        case bulk_load_status::BLS_INGESTING:
            mock_group_ingestion_states(
                ingestion_status::IS_SUCCEED, ingestion_status::IS_SUCCEED, true);
            break;
        case bulk_load_status::BLS_SUCCEED:
            mock_group_cleanup_flag(bulk_load_status::BLS_SUCCEED);
            break;
        case bulk_load_status::BLS_PAUSED:
            mock_group_progress(bulk_load_status::BLS_DOWNLOADING, 30, 100, 100);
            break;
        default:
            return;
        }
        create_bulk_load_request(bulk_load_status::BLS_DOWNLOADING);
        test_start_downloading();
    }

    void test_update_download_progress(uint64_t file_size)
    {
        _bulk_loader->update_bulk_load_download_progress(file_size, "test_file_name");
        _bulk_loader->tracker()->wait_outstanding_tasks();
    }

    void test_start_ingestion() { _bulk_loader->start_ingestion(); }

    void test_handle_bulk_load_finish(bulk_load_status::type status,
                                      int32_t download_progress,
                                      ingestion_status::type istatus,
                                      bool is_bulk_load_ingestion,
                                      bulk_load_status::type req_status)
    {
        mock_replica_bulk_load_varieties(
            status, download_progress, istatus, is_bulk_load_ingestion);
        _bulk_loader->handle_bulk_load_finish(req_status);
    }

    void test_pause_bulk_load(bulk_load_status::type status, int32_t progress)
    {
        mock_replica_bulk_load_varieties(status, progress, ingestion_status::IS_INVALID);
        _bulk_loader->pause_bulk_load();
    }

    int32_t test_report_group_download_progress(bulk_load_status::type status,
                                                int32_t p_progress,
                                                int32_t s1_progress,
                                                int32_t s2_progress)
    {
        mock_group_progress(status, p_progress, s1_progress, s2_progress);
        bulk_load_response response;
        _bulk_loader->report_group_download_progress(response);
        return response.total_download_progress;
    }

    bool test_report_group_ingestion_status(ingestion_status::type primary,
                                            ingestion_status::type secondary1,
                                            ingestion_status::type secondary2,
                                            bool is_empty_prepare_sent,
                                            bool replica_is_ingestion)
    {
        _replica->set_is_ingestion(replica_is_ingestion);
        _replica->set_ingestion_status(primary);
        mock_secondary_ingestion_states(secondary1, secondary2, is_empty_prepare_sent);
        bulk_load_response response;
        _bulk_loader->report_group_ingestion_status(response);
        return response.is_group_ingestion_finished;
    }

    bool test_report_group_cleaned_up()
    {
        bulk_load_response response;
        _bulk_loader->report_group_cleaned_up(response);
        return response.is_group_bulk_load_context_cleaned_up;
    }

    bool test_report_group_is_paused(bulk_load_status::type status)
    {
        mock_group_progress(status, 10, 50, 50);
        partition_bulk_load_state state;
        state.__set_is_paused(true);
        _replica->_primary_states.secondary_bulk_load_states[SECONDARY] = state;
        _replica->_primary_states.secondary_bulk_load_states[SECONDARY2] = state;
        bulk_load_response response;
        _bulk_loader->report_group_is_paused(response);
        return response.is_group_bulk_load_paused;
    }

    void test_on_group_bulk_load_reply(bulk_load_status::type req_status,
                                       ballot req_ballot,
                                       error_code resp_error = ERR_OK,
                                       error_code rpc_error = ERR_OK)
    {
        create_group_bulk_load_request(req_status, req_ballot);
        group_bulk_load_response resp;
        resp.err = resp_error;
        _bulk_loader->on_group_bulk_load_reply(rpc_error, _group_req, resp);
    }

    bool validate_status(bulk_load_status::type meta_status, bulk_load_status::type local_status)
    {
        return _bulk_loader->validate_bulk_load_status(meta_status, local_status) == ERR_OK;
    }

    /// mock structure functions
    void create_local_file(const std::string &file_name)
    {
        std::string whole_name = utils::filesystem::path_combine(LOCAL_DIR, file_name);
        utils::filesystem::create_file(whole_name);
        std::ofstream test_file;
        test_file.open(whole_name);
        test_file << "write some data.\n";
        test_file.close();

        _file_meta.name = whole_name;
        utils::filesystem::md5sum(whole_name, _file_meta.md5);
        utils::filesystem::file_size(whole_name, _file_meta.size);
    }

    error_code create_local_metadata_file()
    {
        create_local_file(FILE_NAME);
        _metadata.files.emplace_back(_file_meta);
        _metadata.file_total_size = _file_meta.size;

        std::string whole_name = utils::filesystem::path_combine(LOCAL_DIR, METADATA);
        utils::filesystem::create_file(whole_name);
        std::ofstream os(whole_name.c_str(),
                         (std::ofstream::out | std::ios::binary | std::ofstream::trunc));
        if (!os.is_open()) {
            derror("open file %s failed", whole_name.c_str());
            return ERR_FILE_OPERATION_FAILED;
        }

        blob bb = json::json_forwarder<bulk_load_metadata>::encode(_metadata);
        os.write((const char *)bb.data(), (std::streamsize)bb.length());
        if (os.bad()) {
            derror("write file %s failed", whole_name.c_str());
            return ERR_FILE_OPERATION_FAILED;
        }
        os.close();

        return ERR_OK;
    }

    void
    construct_file_meta(file_meta &f, const std::string &name, int64_t size, const std::string &md5)
    {
        f.name = name;
        f.size = size;
        f.md5 = md5;
    }

    bool validate_metadata()
    {
        auto target = _bulk_loader->_metadata;
        if (target.file_total_size != _metadata.file_total_size) {
            return false;
        }
        if (target.files.size() != _metadata.files.size()) {
            return false;
        }
        for (int i = 0; i < target.files.size(); ++i) {
            if (target.files[i].name != _metadata.files[i].name) {
                return false;
            }
            if (target.files[i].size != _metadata.files[i].size) {
                return false;
            }
            if (target.files[i].md5 != _metadata.files[i].md5) {
                return false;
            }
        }
        return true;
    }

    void
    create_bulk_load_request(bulk_load_status::type status, ballot b, int32_t downloading_count = 0)
    {
        _req.app_name = APP_NAME;
        _req.ballot = b;
        _req.cluster_name = CLUSTER;
        _req.meta_bulk_load_status = status;
        _req.pid = PID;
        _req.remote_provider_name = PROVIDER;
        stub->set_bulk_load_recent_downloading_replica_count(downloading_count);
    }

    void create_bulk_load_request(bulk_load_status::type status, int32_t downloading_count = 0)
    {
        if (status != bulk_load_status::BLS_DOWNLOADING) {
            downloading_count = 0;
        }
        create_bulk_load_request(status, BALLOT, downloading_count);
    }

    void create_group_bulk_load_request(bulk_load_status::type status, ballot b)
    {
        _group_req.app_name = APP_NAME;
        _group_req.meta_bulk_load_status = status;
        _group_req.config.status = partition_status::PS_SECONDARY;
        _group_req.config.ballot = b;
        _group_req.target_address = SECONDARY;
    }

    void mock_downloading_progress(uint64_t file_total_size,
                                   uint64_t cur_downloaded_size,
                                   int32_t download_progress)
    {
        _bulk_loader->_status = bulk_load_status::type::BLS_DOWNLOADING;
        _bulk_loader->_metadata.file_total_size = file_total_size;
        _bulk_loader->_cur_downloaded_size = cur_downloaded_size;
        _bulk_loader->_download_progress = download_progress;
    }

    void mock_replica_config(partition_status::type status)
    {
        replica_configuration rconfig;
        rconfig.ballot = BALLOT;
        rconfig.pid = PID;
        rconfig.primary = PRIMARY;
        rconfig.status = status;
        _replica->_config = rconfig;
    }

    void mock_primary_states()
    {
        mock_replica_config(partition_status::PS_PRIMARY);
        partition_configuration config;
        config.max_replica_count = 3;
        config.pid = PID;
        config.ballot = BALLOT;
        config.primary = PRIMARY;
        config.secondaries.emplace_back(SECONDARY);
        config.secondaries.emplace_back(SECONDARY2);
        _replica->set_primary_partition_configuration(config);
    }

    void mock_replica_bulk_load_varieties(bulk_load_status::type status,
                                          int32_t download_progress,
                                          ingestion_status::type istatus,
                                          bool is_ingestion = false)
    {
        _bulk_loader->_status = status;
        _bulk_loader->_download_progress = download_progress;
        _replica->set_is_ingestion(is_ingestion);
        _replica->set_ingestion_status(istatus);
    }

    void mock_secondary_progress(int32_t secondary_progress1, int32_t secondary_progress2)
    {
        mock_primary_states();
        partition_bulk_load_state state1, state2;
        state1.__set_download_status(ERR_OK);
        state1.__set_download_progress(secondary_progress1);
        state2.__set_download_status(ERR_OK);
        state2.__set_download_progress(secondary_progress2);
        _replica->set_secondary_bulk_load_state(SECONDARY, state1);
        _replica->set_secondary_bulk_load_state(SECONDARY2, state2);
    }

    void mock_group_progress(bulk_load_status::type p_status,
                             int32_t p_progress,
                             int32_t s1_progress,
                             int32_t s2_progress)
    {
        if (p_status == bulk_load_status::BLS_INVALID) {
            p_progress = 0;
        } else if (p_status == bulk_load_status::BLS_DOWNLOADED) {
            p_progress = 100;
        }
        mock_replica_bulk_load_varieties(p_status, p_progress, ingestion_status::IS_INVALID);
        mock_secondary_progress(s1_progress, s2_progress);
    }

    void mock_group_progress(bulk_load_status::type p_status)
    {
        if (p_status == bulk_load_status::BLS_INVALID) {
            mock_group_progress(p_status, 0, 0, 0);
        } else if (p_status == bulk_load_status::BLS_DOWNLOADED) {
            mock_group_progress(p_status, 100, 100, 100);
        }
    }

    void mock_secondary_ingestion_states(ingestion_status::type status1,
                                         ingestion_status::type status2,
                                         bool is_empty_prepare_sent = true)
    {
        mock_secondary_progress(100, 100);
        _replica->set_is_empty_prepare_sent(is_empty_prepare_sent);

        partition_bulk_load_state state1, state2;
        state1.__set_ingest_status(status1);
        state2.__set_ingest_status(status2);
        _replica->set_secondary_bulk_load_state(SECONDARY, state1);
        _replica->set_secondary_bulk_load_state(SECONDARY2, state2);
    }

    void mock_group_ingestion_states(ingestion_status::type s1_status,
                                     ingestion_status::type s2_status,
                                     bool is_ingestion_commit = true)
    {
        mock_replica_bulk_load_varieties(
            bulk_load_status::BLS_INGESTING, 100, ingestion_status::IS_SUCCEED);
        mock_secondary_ingestion_states(s1_status, s2_status, is_ingestion_commit);
    }

    void mock_group_cleanup_flag(bulk_load_status::type primary_status,
                                 bool s1_cleanup = true,
                                 bool s2_cleanup = true)
    {
        int32_t primary_progress = primary_status == bulk_load_status::BLS_SUCCEED ? 100 : 0;
        mock_replica_bulk_load_varieties(
            primary_status, primary_progress, ingestion_status::IS_INVALID);
        mock_secondary_ingestion_states(
            ingestion_status::IS_INVALID, ingestion_status::IS_INVALID, true);

        partition_bulk_load_state state1, state2;
        state1.__set_is_cleaned_up(s1_cleanup);
        state2.__set_is_cleaned_up(s2_cleanup);
        _replica->set_secondary_bulk_load_state(SECONDARY, state1);
        _replica->set_secondary_bulk_load_state(SECONDARY2, state2);
    }

    /// helper functions

    int32_t get_download_progress() { return _bulk_loader->_download_progress.load(); }
    bool get_clean_up_flag() { return _bulk_loader->is_cleaned_up(); }
    bulk_load_status::type get_bulk_load_status() { return _bulk_loader->_status; }
    int32_t get_stub_downloading_count()
    {
        return stub->get_bulk_load_recent_downloading_replica_count();
    }

    ingestion_status::type get_ingestion_status() { return _replica->_app->get_ingestion_status(); }

    int32_t is_ingestion() { return _replica->_is_bulk_load_ingestion; }

    bool is_secondary_bulk_load_state_reset()
    {
        const partition_bulk_load_state &state = _replica->get_secondary_bulk_load_state(SECONDARY);
        bool is_download_state_reset =
            (state.__isset.download_progress && state.__isset.download_status &&
             state.download_progress == 0 && state.download_status == ERR_OK);
        bool is_ingestion_status_reset =
            (state.__isset.ingest_status && state.ingest_status == ingestion_status::IS_INVALID);
        bool is_cleanup_flag_reset = (state.__isset.is_cleaned_up && !state.is_cleaned_up);
        bool is_paused_flag_reset = (state.__isset.is_paused && !state.is_paused);
        return is_download_state_reset && is_ingestion_status_reset && is_cleanup_flag_reset &&
               is_paused_flag_reset;
    }

    bool primary_is_bulk_load_states_cleaned()
    {
        if (_replica->status() != partition_status::PS_PRIMARY) {
            return false;
        }
        auto pstates = _replica->_primary_states;
        return (pstates.ingestion_is_empty_prepare_sent == false &&
                pstates.secondary_bulk_load_states.size() == 0);
    }

public:
    std::unique_ptr<mock_replica> _replica;
    std::unique_ptr<replica_bulk_loader> _bulk_loader;
    file_meta _file_meta;
    bulk_load_metadata _metadata;
    bulk_load_request _req;
    group_bulk_load_request _group_req;

    std::string APP_NAME = "replica";
    std::string CLUSTER = "cluster";
    std::string PROVIDER = "local_service";
    std::string LOCAL_DIR = bulk_load_constant::BULK_LOAD_LOCAL_ROOT_DIR;
    std::string METADATA = bulk_load_constant::BULK_LOAD_METADATA;
    std::string FILE_NAME = "test_sst_file";
    gpid PID = gpid(1, 0);
    ballot BALLOT = 3;
    rpc_address PRIMARY = rpc_address("127.0.0.2", 34801);
    rpc_address SECONDARY = rpc_address("127.0.0.3", 34801);
    rpc_address SECONDARY2 = rpc_address("127.0.0.4", 34801);
    int32_t MAX_DOWNLOADING_COUNT = 5;
};

// on_bulk_load unit tests
TEST_F(replica_bulk_loader_test, on_bulk_load_not_primary)
{
    create_bulk_load_request(bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(test_on_bulk_load(), ERR_INVALID_STATE);
}

TEST_F(replica_bulk_loader_test, on_bulk_load_ballot_change)
{
    create_bulk_load_request(bulk_load_status::BLS_DOWNLOADING, BALLOT + 1);
    mock_primary_states();
    ASSERT_EQ(test_on_bulk_load(), ERR_INVALID_STATE);
}

// on_group_bulk_load unit tests
TEST_F(replica_bulk_loader_test, on_group_bulk_load_test)
{
    struct test_struct
    {
        partition_status::type pstatus;
        bulk_load_status::type bstatus;
        ballot b;
        error_code expected_err;
    } tests[] = {
        {partition_status::PS_SECONDARY,
         bulk_load_status::BLS_DOWNLOADING,
         BALLOT - 1,
         ERR_VERSION_OUTDATED},
        {partition_status::PS_SECONDARY,
         bulk_load_status::BLS_DOWNLOADED,
         BALLOT + 1,
         ERR_INVALID_STATE},
        {partition_status::PS_INACTIVE, bulk_load_status::BLS_INGESTING, BALLOT, ERR_INVALID_STATE},
    };

    for (auto test : tests) {
        mock_replica_config(test.pstatus);
        ASSERT_EQ(test_on_group_bulk_load(test.bstatus, test.b), test.expected_err);
    }
}

// parse_bulk_load_metadata unit tests
TEST_F(replica_bulk_loader_test, bulk_load_metadata_not_exist)
{
    error_code ec = test_parse_bulk_load_metadata("path_not_exist");
    ASSERT_EQ(ec, ERR_FILE_OPERATION_FAILED);
}

TEST_F(replica_bulk_loader_test, bulk_load_metadata_corrupt)
{
    utils::filesystem::create_directory(LOCAL_DIR);
    create_local_file(METADATA);
    std::string metadata_file_name = utils::filesystem::path_combine(LOCAL_DIR, METADATA);
    error_code ec = test_parse_bulk_load_metadata(metadata_file_name);
    ASSERT_EQ(ec, ERR_CORRUPTION);
    utils::filesystem::remove_path(LOCAL_DIR);
}

TEST_F(replica_bulk_loader_test, bulk_load_metadata_parse_succeed)
{
    utils::filesystem::create_directory(LOCAL_DIR);
    error_code ec = create_local_metadata_file();
    ASSERT_EQ(ec, ERR_OK);

    std::string metadata_file_name = utils::filesystem::path_combine(LOCAL_DIR, METADATA);
    ec = test_parse_bulk_load_metadata(metadata_file_name);
    ASSERT_EQ(ec, ERR_OK);
    ASSERT_TRUE(validate_metadata());
    utils::filesystem::remove_path(LOCAL_DIR);
}

// start_downloading unit tests
TEST_F(replica_bulk_loader_test, start_downloading_test)
{
    // Test cases:
    // - stub concurrent downloading count excceed
    // - downloading error
    // - downloading succeed
    struct test_struct
    {
        bool mock_function;
        int32_t downloading_count;
        error_code expected_err;
        bulk_load_status::type expected_status;
        int32_t expected_downloading_count;
    } tests[]{{false,
               MAX_DOWNLOADING_COUNT,
               ERR_BUSY,
               bulk_load_status::BLS_INVALID,
               MAX_DOWNLOADING_COUNT},
              {false, 1, ERR_CORRUPTION, bulk_load_status::BLS_DOWNLOADING, 1},
              {true, 1, ERR_OK, bulk_load_status::BLS_DOWNLOADING, 2}};

    for (auto test : tests) {
        if (test.mock_function) {
            fail::cfg("replica_bulk_loader_download_sst_files", "return()");
        }
        mock_group_progress(bulk_load_status::BLS_INVALID);
        create_bulk_load_request(bulk_load_status::BLS_DOWNLOADING, test.downloading_count);

        ASSERT_EQ(test_start_downloading(), test.expected_err);
        ASSERT_EQ(get_bulk_load_status(), test.expected_status);
        ASSERT_EQ(get_stub_downloading_count(), test.expected_downloading_count);
    }
}

// start_downloading unit tests
TEST_F(replica_bulk_loader_test, rollback_to_downloading_test)
{
    fail::cfg("replica_bulk_loader_download_sst_files", "return()");
    struct test_struct
    {
        bulk_load_status::type status;
    } tests[]{
        {bulk_load_status::BLS_DOWNLOADING},
        {bulk_load_status::BLS_DOWNLOADED},
        {bulk_load_status::BLS_INGESTING},
        {bulk_load_status::BLS_SUCCEED},
        {bulk_load_status::BLS_PAUSED},
    };

    for (auto test : tests) {
        test_rollback_to_downloading(test.status);
        ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADING);
        ASSERT_TRUE(primary_is_bulk_load_states_cleaned());
        ASSERT_EQ(get_ingestion_status(), ingestion_status::IS_INVALID);
        ASSERT_FALSE(is_ingestion());
    }
}

// finish download test
TEST_F(replica_bulk_loader_test, finish_download_test)
{
    mock_downloading_progress(100, 50, 50);
    stub->set_bulk_load_downloading_count(3);

    test_update_download_progress(50);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADED);
    ASSERT_EQ(get_stub_downloading_count(), 2);
}

// start ingestion test
TEST_F(replica_bulk_loader_test, start_ingestion_test)
{
    mock_group_progress(bulk_load_status::BLS_DOWNLOADED);
    test_start_ingestion();
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_INGESTING);
}

// handle_bulk_load_finish unit tests
TEST_F(replica_bulk_loader_test, bulk_load_finish_test)
{
    // Test cases
    // - bulk load succeed
    // - double bulk load finish
    // - failed during downloading
    // - failed during ingestion
    // - cancel during downloaded
    // - cancel during ingestion
    // - cancel during succeed
    // Tip: bulk load dir will be removed if bulk load finished, so we should create dir before some
    // cases
    struct test_struct
    {
        bulk_load_status::type local_status;
        int32_t progress;
        ingestion_status::type istatus;
        bool is_ingestion;
        bulk_load_status::type request_status;
        bool create_dir;
    } tests[]{{bulk_load_status::BLS_SUCCEED,
               100,
               ingestion_status::IS_INVALID,
               false,
               bulk_load_status::BLS_SUCCEED,
               false},
              {bulk_load_status::BLS_INVALID,
               0,
               ingestion_status::IS_INVALID,
               false,
               bulk_load_status::BLS_SUCCEED,
               false},
              {bulk_load_status::BLS_DOWNLOADING,
               10,
               ingestion_status::IS_INVALID,
               false,
               bulk_load_status::BLS_FAILED,
               true},
              {bulk_load_status::BLS_INGESTING,
               100,
               ingestion_status::type::IS_FAILED,
               false,
               bulk_load_status::BLS_FAILED,
               true},
              {bulk_load_status::BLS_DOWNLOADED,
               100,
               ingestion_status::IS_INVALID,
               false,
               bulk_load_status::BLS_CANCELED,
               true},
              {bulk_load_status::BLS_INGESTING,
               100,
               ingestion_status::type::IS_RUNNING,
               true,
               bulk_load_status::BLS_CANCELED,
               true},
              {bulk_load_status::BLS_SUCCEED,
               100,
               ingestion_status::IS_INVALID,
               false,
               bulk_load_status::BLS_CANCELED,
               true}};

    for (auto test : tests) {
        if (test.create_dir) {
            utils::filesystem::create_directory(LOCAL_DIR);
        }
        test_handle_bulk_load_finish(
            test.local_status, test.progress, test.istatus, test.is_ingestion, test.request_status);
        ASSERT_EQ(get_ingestion_status(), ingestion_status::IS_INVALID);
        ASSERT_FALSE(is_ingestion());
        ASSERT_TRUE(get_clean_up_flag());
        ASSERT_FALSE(utils::filesystem::directory_exists(LOCAL_DIR));
    }
}

// pause_bulk_load unit tests
TEST_F(replica_bulk_loader_test, pause_bulk_load_test)
{
    // Test cases:
    // pausing while not bulk load
    // pausing during downloading
    // pausing during downloaded
    struct test_struct
    {
        bulk_load_status::type status;
        int32_t progress;
        int32_t expected_progress;
    } tests[]{
        {bulk_load_status::BLS_INVALID, 0, 0},
        {bulk_load_status::BLS_DOWNLOADING, 10, 10},
        {bulk_load_status::BLS_DOWNLOADED, 100, 100},
    };

    for (auto test : tests) {
        test_pause_bulk_load(test.status, test.progress);
        ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_PAUSED);
        ASSERT_EQ(get_download_progress(), test.expected_progress);
    }
}

// report_group_download_progress unit tests
TEST_F(replica_bulk_loader_test, report_group_download_progress_test)
{
    struct test_struct
    {
        bulk_load_status::type primary_status;
        int32_t primary_progress;
        int32_t secondary1_progress;
        int32_t secondary2_progress;
        int32_t total_progress;
    } tests[]{
        {bulk_load_status::BLS_DOWNLOADING, 10, 10, 10, 10},
        {bulk_load_status::BLS_DOWNLOADED, 100, 0, 0, 33},
        {bulk_load_status::BLS_DOWNLOADED, 100, 100, 100, 100},
    };

    for (auto test : tests) {
        ASSERT_EQ(test_report_group_download_progress(test.primary_status,
                                                      test.primary_progress,
                                                      test.secondary1_progress,
                                                      test.secondary2_progress),
                  test.total_progress);
    }
}

// report_group_ingestion_status unit tests
TEST_F(replica_bulk_loader_test, report_group_ingestion_status_test)
{

    struct ingestion_struct
    {
        ingestion_status::type primary;
        ingestion_status::type secondary1;
        ingestion_status::type secondary2;
        bool is_empty_prepare_sent;
        bool replica_is_ingestion;
        bool is_group_ingestion_finished;
    } tests[] = {
        {ingestion_status::IS_INVALID,
         ingestion_status::IS_INVALID,
         ingestion_status::IS_INVALID,
         false,
         false,
         false},
        {ingestion_status::IS_RUNNING,
         ingestion_status::IS_INVALID,
         ingestion_status::IS_INVALID,
         false,
         false,
         false},
        {ingestion_status::IS_SUCCEED,
         ingestion_status::IS_INVALID,
         ingestion_status::IS_INVALID,
         false,
         false,
         false},
        {ingestion_status::IS_FAILED,
         ingestion_status::IS_INVALID,
         ingestion_status::IS_INVALID,
         false,
         false,
         false},
        {ingestion_status::IS_RUNNING,
         ingestion_status::IS_RUNNING,
         ingestion_status::IS_INVALID,
         false,
         false,
         false},
        {ingestion_status::IS_SUCCEED,
         ingestion_status::IS_SUCCEED,
         ingestion_status::IS_RUNNING,
         true,
         false,
         false},
        {ingestion_status::IS_FAILED,
         ingestion_status::IS_FAILED,
         ingestion_status::IS_RUNNING,
         false,
         false,
         false},
        {ingestion_status::IS_SUCCEED,
         ingestion_status::IS_SUCCEED,
         ingestion_status::IS_SUCCEED,
         true,
         true,
         true},
    };

    for (auto test : tests) {
        ASSERT_EQ(test_report_group_ingestion_status(test.primary,
                                                     test.secondary1,
                                                     test.secondary2,
                                                     test.is_empty_prepare_sent,
                                                     test.replica_is_ingestion),
                  test.is_group_ingestion_finished);
        ASSERT_FALSE(_replica->is_ingestion());
    }
}

// report_group_context_clean_flag unit tests
TEST_F(replica_bulk_loader_test, report_group_cleanup_flag_in_unhealthy_state)
{
    // _primary_states.membership.secondaries is empty
    mock_replica_config(partition_status::PS_PRIMARY);
    ASSERT_FALSE(test_report_group_cleaned_up());
}

TEST_F(replica_bulk_loader_test, report_group_cleanup_flag_not_cleaned_up)
{
    mock_group_cleanup_flag(bulk_load_status::BLS_SUCCEED, true, false);
    ASSERT_FALSE(test_report_group_cleaned_up());
}

TEST_F(replica_bulk_loader_test, report_group_cleanup_flag_all_cleaned_up)
{
    mock_group_cleanup_flag(bulk_load_status::BLS_INVALID, true, true);
    ASSERT_TRUE(test_report_group_cleaned_up());
}

// report_group_is_paused unit tests
TEST_F(replica_bulk_loader_test, report_group_is_paused_test)
{
    struct test_struct
    {
        bulk_load_status::type local_status;
        bool expected;
    } tests[]{{bulk_load_status::BLS_DOWNLOADING, false}, {bulk_load_status::BLS_PAUSED, true}};

    for (auto test : tests) {
        ASSERT_EQ(test_report_group_is_paused(test.local_status), test.expected);
    }
}

// on_group_bulk_load_reply unit tests
TEST_F(replica_bulk_loader_test, on_group_bulk_load_reply_downloading_error)
{
    mock_group_progress(bulk_load_status::BLS_DOWNLOADING, 30, 30, 60);
    test_on_group_bulk_load_reply(bulk_load_status::BLS_DOWNLOADING, BALLOT, ERR_BUSY);
    ASSERT_TRUE(is_secondary_bulk_load_state_reset());
}

TEST_F(replica_bulk_loader_test, on_group_bulk_load_reply_downloaded_error)
{
    mock_group_progress(bulk_load_status::BLS_DOWNLOADED);
    test_on_group_bulk_load_reply(bulk_load_status::BLS_DOWNLOADED, BALLOT, ERR_INVALID_STATE);
    ASSERT_TRUE(is_secondary_bulk_load_state_reset());
}

TEST_F(replica_bulk_loader_test, on_group_bulk_load_reply_ingestion_error)
{
    mock_group_ingestion_states(ingestion_status::IS_RUNNING, ingestion_status::IS_SUCCEED);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_INGESTING, BALLOT - 1, ERR_OK, ERR_INVALID_STATE);
    ASSERT_TRUE(is_secondary_bulk_load_state_reset());
}

TEST_F(replica_bulk_loader_test, on_group_bulk_load_reply_succeed_error)
{
    mock_group_cleanup_flag(bulk_load_status::BLS_SUCCEED);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_SUCCEED, BALLOT - 1, ERR_OK, ERR_INVALID_STATE);
    ASSERT_TRUE(is_secondary_bulk_load_state_reset());
}

TEST_F(replica_bulk_loader_test, on_group_bulk_load_reply_failed_error)
{
    mock_group_ingestion_states(ingestion_status::IS_RUNNING, ingestion_status::IS_SUCCEED);
    test_on_group_bulk_load_reply(bulk_load_status::BLS_FAILED, BALLOT, ERR_OK, ERR_TIMEOUT);
    ASSERT_TRUE(is_secondary_bulk_load_state_reset());
}

TEST_F(replica_bulk_loader_test, on_group_bulk_load_reply_pausing_error)
{
    mock_group_progress(bulk_load_status::BLS_PAUSED, 100, 50, 10);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_PAUSING, BALLOT, ERR_OK, ERR_NETWORK_FAILURE);
    ASSERT_TRUE(is_secondary_bulk_load_state_reset());
}

TEST_F(replica_bulk_loader_test, on_group_bulk_load_reply_rpc_error)
{
    mock_group_cleanup_flag(bulk_load_status::BLS_INVALID, true, false);
    test_on_group_bulk_load_reply(bulk_load_status::BLS_CANCELED, BALLOT, ERR_OBJECT_NOT_FOUND);
    ASSERT_TRUE(is_secondary_bulk_load_state_reset());
}

// validate_bulk_load_status unit test
TEST_F(replica_bulk_loader_test, validate_bulk_load_status_test)
{
    struct validate_struct
    {
        bulk_load_status::type meta_status;
        bulk_load_status::type local_status;
        bool expected_flag;
    } tests[] = {{bulk_load_status::BLS_INVALID, bulk_load_status::BLS_INVALID, true},
                 {bulk_load_status::BLS_PAUSED, bulk_load_status::BLS_PAUSED, false},
                 {bulk_load_status::BLS_FAILED, bulk_load_status::BLS_INGESTING, true},
                 {bulk_load_status::BLS_CANCELED, bulk_load_status::BLS_SUCCEED, true},
                 {bulk_load_status::BLS_DOWNLOADING, bulk_load_status::BLS_INVALID, true},
                 {bulk_load_status::BLS_DOWNLOADING, bulk_load_status::BLS_INGESTING, true},
                 {bulk_load_status::BLS_DOWNLOADING, bulk_load_status::BLS_SUCCEED, true},
                 {bulk_load_status::BLS_DOWNLOADING, bulk_load_status::BLS_FAILED, false},
                 {bulk_load_status::BLS_DOWNLOADING, bulk_load_status::BLS_CANCELED, false},
                 {bulk_load_status::BLS_DOWNLOADED, bulk_load_status::BLS_INVALID, false},
                 {bulk_load_status::BLS_DOWNLOADED, bulk_load_status::BLS_DOWNLOADED, true},
                 {bulk_load_status::BLS_INGESTING, bulk_load_status::BLS_DOWNLOADED, true},
                 {bulk_load_status::BLS_INGESTING, bulk_load_status::BLS_SUCCEED, false},
                 {bulk_load_status::BLS_SUCCEED, bulk_load_status::BLS_INVALID, true},
                 {bulk_load_status::BLS_SUCCEED, bulk_load_status::BLS_INGESTING, true},
                 {bulk_load_status::BLS_SUCCEED, bulk_load_status::BLS_DOWNLOADING, false},
                 {bulk_load_status::BLS_PAUSING, bulk_load_status::BLS_INVALID, true},
                 {bulk_load_status::BLS_PAUSING, bulk_load_status::BLS_DOWNLOADING, true},
                 {bulk_load_status::BLS_PAUSING, bulk_load_status::BLS_DOWNLOADED, true},
                 {bulk_load_status::BLS_PAUSING, bulk_load_status::BLS_PAUSED, true},
                 {bulk_load_status::BLS_PAUSING, bulk_load_status::BLS_INGESTING, false}};

    for (auto test : tests) {
        ASSERT_EQ(validate_status(test.meta_status, test.local_status), test.expected_flag);
    }
}

} // namespace replication
} // namespace dsn
