// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/fail_point.h>
#include <fstream>
#include <gtest/gtest.h>

#include "mock_utils.h"

namespace dsn {
namespace replication {

class replica_bulk_load_test : public ::testing::Test
{
    void SetUp()
    {
        _stub = make_unique<mock_replica_stub>();
        _replica = create_mock_replica(_stub.get());
        _fs = _stub->get_block_filesystem();
        utils::filesystem::create_directory(LOCAL_DIR);
    }

    void TearDown() { utils::filesystem::remove_path(LOCAL_DIR); }

public:
    /// test helper functions
    void test_on_bulk_load(bulk_load_response &resp) { _replica->on_bulk_load(_req, resp); }

    void
    test_on_group_bulk_load(bulk_load_status::type status, ballot b, group_bulk_load_response &resp)
    {
        mock_group_bulk_load_request(status, b);
        _replica->on_group_bulk_load(_group_req, resp);
    }

    dsn::error_code test_do_download(const std::string &remote_file_name, bool is_update_progress)
    {
        dsn::error_code err;
        dsn::task_tracker tracker;
        _replica->do_download(
            PROVIDER, LOCAL_DIR, remote_file_name, _fs.get(), is_update_progress, err, tracker);
        return err;
    }

    error_code test_parse_bulk_load_metadata(const std::string &file_path, bulk_load_metadata &meta)
    {
        return _replica->parse_bulk_load_metadata(file_path, meta);
    }

    bool test_verify_sst_files(const file_meta &f_meta)
    {
        return _replica->verify_sst_files(f_meta, LOCAL_DIR);
    }

    dsn::error_code test_start_downloading()
    {
        return _replica->bulk_load_start_download(APP_NAME, CLUSTER, PROVIDER);
    }

    void test_update_download_progress(uint64_t file_size)
    {
        _replica->update_download_progress(file_size);
        _replica->tracker()->wait_outstanding_tasks();
    }

    void test_start_ingestion() { return _replica->bulk_load_start_ingestion(); }

    void test_cleanup_bulk_load_context(bulk_load_status::type status)
    {
        _replica->cleanup_bulk_load_context(status);
    }

    void test_pause_bulk_load() { return _replica->pause_bulk_load(); }

    void test_report_group_download_progress(bulk_load_response &response)
    {
        _replica->report_group_download_progress(response);
    }

    bool test_report_group_ingestion_status(ingestion_status::type primary,
                                            ingestion_status::type secondary1,
                                            ingestion_status::type secondary2,
                                            bool is_ingestion_commit,
                                            bool mock_partition_version)
    {
        if (mock_partition_version) {
            _replica->_partition_version.store(-1);
        }
        mock_ingestion_status(primary);
        mock_ingestion_states(secondary1, secondary2, is_ingestion_commit);
        bulk_load_response response;
        _replica->report_group_ingestion_status(response);
        return response.is_group_ingestion_finished;
    }

    void test_report_group_context_clean_flag(bulk_load_response &response)
    {
        _replica->report_group_context_clean_flag(response);
    }

    void test_report_group_is_paused(bulk_load_response &response)
    {
        _replica->report_group_is_paused(response);
    }

    void
    test_on_group_bulk_load_reply(bulk_load_status::type req_status,
                                  ballot req_ballot,
                                  bulk_load_status::type resp_status,
                                  int32_t progress,
                                  bool is_context_cleaned,
                                  ingestion_status::type istatus = ingestion_status::IS_INVALID,
                                  error_code rpc_error = ERR_OK,
                                  error_code error = ERR_OK)
    {
        mock_group_bulk_load_request(req_status, req_ballot);
        partition_download_progress download_progress;
        download_progress.progress = progress;
        download_progress.status = ERR_OK;
        std::shared_ptr<group_bulk_load_request> req =
            std::make_shared<group_bulk_load_request>(_group_req);
        std::shared_ptr<group_bulk_load_response> resp =
            std::make_shared<group_bulk_load_response>();
        resp->err = error;
        resp->status = resp_status;
        resp->__set_is_bulk_load_context_cleaned(is_context_cleaned);
        resp->__set_download_progress(download_progress);
        resp->__set_istatus(istatus);

        _replica->on_group_bulk_load_reply(rpc_error, req, resp);
    }

    bool validate_status(bulk_load_status::type meta_status, bulk_load_status::type local_status)
    {
        return _replica->validate_bulk_load_status(meta_status, local_status) == ERR_OK;
    }

    /// mock structure functions
    void create_file(std::string file_name)
    {
        std::string whole_file_path = utils::filesystem::path_combine(LOCAL_DIR, file_name);
        utils::filesystem::create_file(whole_file_path);
    }

    void construct_file_meta(file_meta &f, std::string name, int64_t size, std::string md5)
    {
        f.name = name;
        f.size = size;
        f.md5 = md5;
    }

    error_code mock_bulk_load_metadata(std::string file_path)
    {
        file_meta f;
        construct_file_meta(f, "mock", 2333, "this_is_a_mock_md5");
        _metadata.files.emplace_back(f);
        _metadata.file_total_size = 2333;

        std::ofstream os(file_path.c_str(),
                         (std::ofstream::out | std::ios::binary | std::ofstream::trunc));
        if (!os.is_open()) {
            derror("open file %s failed", file_path.c_str());
            return ERR_FILE_OPERATION_FAILED;
        }

        blob bb = json::json_forwarder<bulk_load_metadata>::encode(_metadata);
        os.write((const char *)bb.data(), (std::streamsize)bb.length());
        if (os.bad()) {
            derror("write file %s failed", file_path.c_str());
            return ERR_FILE_OPERATION_FAILED;
        }
        os.close();
        return ERR_OK;
    }

    void create_file_and_get_file_meta(std::string file_name, file_meta &f, bool is_metadata)
    {
        create_file(file_name);
        std::string whole_name = utils::filesystem::path_combine(LOCAL_DIR, file_name);
        if (is_metadata) {
            mock_bulk_load_metadata(whole_name);
        }
        std::string md5;
        int64_t size;
        utils::filesystem::md5sum(whole_name, md5);
        utils::filesystem::file_size(whole_name, size);
        construct_file_meta(f, whole_name, size, md5);
    }

    bool validate_metadata(bulk_load_metadata target)
    {
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

    void mock_bulk_load_request(bulk_load_status::type status, ballot b)
    {
        _req.app_name = APP_NAME;
        _req.ballot = b;
        _req.cluster_name = CLUSTER;
        _req.meta_bulk_load_status = status;
        _req.pid = PID;
        _req.remote_provider_name = PROVIDER;
    }

    void mock_bulk_load_request(bulk_load_status::type status)
    {
        mock_bulk_load_request(status, BALLOT);
    }

    void mock_group_bulk_load_request(bulk_load_status::type status, ballot b)
    {
        _group_req.app_name = APP_NAME;
        _group_req.meta_bulk_load_status = status;
        _group_req.config.status = partition_status::PS_SECONDARY;
        _group_req.config.ballot = b;
        _group_req.target_address = SECONDARY;
    }

    void mock_bulk_load_context(uint64_t file_total_size,
                                uint64_t cur_downloaded_size = 0,
                                int32_t download_progress = 0,
                                bulk_load_status::type status = bulk_load_status::BLS_INVALID)
    {
        _replica->_bulk_load_context._file_total_size = file_total_size;
        _replica->_bulk_load_context._cur_downloaded_size = cur_downloaded_size;
        _replica->_bulk_load_context._download_progress = download_progress;
        _replica->_bulk_load_context._status = status;
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

    void mock_primary_states(bool mock_download_progress = true,
                             bool mock_cleanup_flag = true,
                             bool all_secondary_finish = false,
                             bool mock_pause_flag = false)
    {
        mock_replica_config(partition_status::PS_PRIMARY);
        rpc_address SECONDARY2 = rpc_address("127.0.0.4", 34801);

        partition_configuration config;
        config.max_replica_count = 3;
        config.pid = PID;
        config.ballot = BALLOT;
        config.primary = PRIMARY;
        config.secondaries.emplace_back(SECONDARY);
        config.secondaries.emplace_back(SECONDARY2);
        _replica->_primary_states.membership = config;

        if (mock_download_progress) {
            partition_download_progress mock_progress;
            mock_progress.progress = all_secondary_finish ? 100 : 0;
            _replica->_primary_states.group_download_progress[SECONDARY] = mock_progress;
            _replica->_primary_states.group_download_progress[SECONDARY2] = mock_progress;
        }
        if (mock_cleanup_flag) {
            _replica->_primary_states.group_bulk_load_context_flag[SECONDARY] = true;
            _replica->_primary_states.group_bulk_load_context_flag[SECONDARY2] =
                all_secondary_finish;
        }
        if (mock_pause_flag) {
            _replica->_primary_states.group_bulk_load_paused[SECONDARY] = true;
            _replica->_primary_states.group_bulk_load_paused[SECONDARY2] = all_secondary_finish;
        }
    }

    void mock_ingestion_states(ingestion_status::type status1,
                               ingestion_status::type status2,
                               bool is_ingestion_commit = true)
    {
        // mock finish download
        mock_primary_states(true, false, true);
        rpc_address SECONDARY2 = rpc_address("127.0.0.4", 34801);
        _replica->_primary_states.is_ingestion_commit = is_ingestion_commit;
        _replica->_primary_states.group_ingestion_status[SECONDARY] = status1;
        _replica->_primary_states.group_ingestion_status[SECONDARY2] = status2;
    }

    void mock_cleanup_flag_unhealthy()
    {
        mock_primary_states();
        _replica->_primary_states.membership.secondaries.clear();
    }

    void mock_stub_downloading_count(int32_t count)
    {
        _stub->set_bulk_load_recent_downloading_replica_count(count);
    }

    void mock_ingestion_status(ingestion_status::type status)
    {
        _replica->_app->set_ingestion_status(status);
    }

    /// getter functions
    uint64_t get_cur_downloaded_size()
    {
        return _replica->_bulk_load_context._cur_downloaded_size.load();
    }

    int32_t get_download_progress()
    {
        return _replica->_bulk_load_context._download_progress.load();
    }
    bool get_clean_up_flag() { return _replica->_bulk_load_context.is_cleanup(); }
    bulk_load_status::type get_bulk_load_status() { return _replica->_bulk_load_context._status; }
    int32_t get_stub_downloading_count()
    {
        return _stub->get_bulk_load_recent_downloading_replica_count();
    }

    int32_t get_partition_version() { return _replica->_partition_version.load(); }

    error_code primary_get_node_download_progress(partition_download_progress &download_progress)
    {
        if (_replica->status() != partition_status::PS_PRIMARY) {
            return ERR_INVALID_STATE;
        }
        download_progress = _replica->_primary_states.group_download_progress[SECONDARY];
        return ERR_OK;
    }

    error_code primary_get_node_context_clean_flag(bool &is_context_cleaned)
    {
        if (_replica->status() != partition_status::PS_PRIMARY) {
            return ERR_INVALID_STATE;
        }
        is_context_cleaned = _replica->_primary_states.group_bulk_load_context_flag[SECONDARY];
        return ERR_OK;
    }

    error_code primary_get_node_ingestion_status(ingestion_status::type &istatus)
    {
        if (_replica->status() != partition_status::PS_PRIMARY) {
            return ERR_INVALID_STATE;
        }
        istatus = _replica->_primary_states.group_ingestion_status[SECONDARY];
        return ERR_OK;
    }

public:
    std::unique_ptr<mock_replica_stub> _stub;
    std::unique_ptr<mock_replica> _replica;
    std::unique_ptr<block_service_mock> _fs;
    bulk_load_request _req;
    bulk_load_metadata _metadata;
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
    int32_t MAX_DOWNLOADING_COUNT = 5;
    int32_t PARTITION_VERSION = 3;
};

// on_bulk_load unit tests
TEST_F(replica_bulk_load_test, on_bulk_load_not_primary)
{
    mock_bulk_load_request(bulk_load_status::BLS_DOWNLOADING);

    bulk_load_response resp;
    test_on_bulk_load(resp);
    ASSERT_EQ(resp.err, ERR_INVALID_STATE);
}

TEST_F(replica_bulk_load_test, on_bulk_load_ballot_change)
{
    mock_bulk_load_request(bulk_load_status::BLS_DOWNLOADING, 2);
    mock_primary_states(false, false);

    bulk_load_response resp;
    test_on_bulk_load(resp);
    ASSERT_EQ(resp.err, ERR_INVALID_STATE);
}

// on_group_bulk_load unit tests
TEST_F(replica_bulk_load_test, on_group_bulk_load_request_outdated)
{
    mock_replica_config(partition_status::PS_SECONDARY);

    group_bulk_load_response resp;
    test_on_group_bulk_load(bulk_load_status::BLS_DOWNLOADING, BALLOT - 1, resp);

    ASSERT_EQ(resp.err, ERR_VERSION_OUTDATED);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_ballot_changed)
{
    mock_replica_config(partition_status::PS_SECONDARY);

    group_bulk_load_response resp;
    test_on_group_bulk_load(bulk_load_status::BLS_DOWNLOADED, BALLOT + 1, resp);

    ASSERT_EQ(resp.err, ERR_INVALID_STATE);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_wrong_partition_status)
{
    mock_replica_config(partition_status::PS_INACTIVE);

    group_bulk_load_response resp;
    test_on_group_bulk_load(bulk_load_status::BLS_INGESTING, BALLOT, resp);

    ASSERT_EQ(resp.err, ERR_INVALID_STATE);
}

// download files unit tests
TEST_F(replica_bulk_load_test, do_download_file_not_exist)
{
    ASSERT_EQ(test_do_download(FILE_NAME, false), ERR_CORRUPTION);
}

TEST_F(replica_bulk_load_test, do_download_file_md5_not_match)
{
    create_file(FILE_NAME);
    // mock remote file
    std::string remote_file_name = utils::filesystem::path_combine(PROVIDER, FILE_NAME);
    _fs->files[remote_file_name] = std::make_pair(2333, "md5_not_match");

    ASSERT_EQ(test_do_download(FILE_NAME, false), ERR_OK);
}

TEST_F(replica_bulk_load_test, do_download_file_exist)
{
    file_meta f_meta;
    create_file_and_get_file_meta(METADATA, f_meta, true);

    // mock remote file
    std::string remote_file_name = utils::filesystem::path_combine(PROVIDER, METADATA);
    _fs->files[remote_file_name] = std::make_pair(f_meta.size, f_meta.md5);

    // mock bulk_load context
    mock_bulk_load_context(f_meta.size);

    ASSERT_EQ(test_do_download(METADATA, true), ERR_OK);
    ASSERT_EQ(f_meta.size, get_cur_downloaded_size());
    ASSERT_EQ(100, get_download_progress());
}

TEST_F(replica_bulk_load_test, do_download_succeed)
{
    file_meta f_meta;
    create_file_and_get_file_meta(FILE_NAME, f_meta, false);

    std::string file_name = utils::filesystem::path_combine(LOCAL_DIR, FILE_NAME);
    utils::filesystem::remove_path(file_name);

    // mock remote file
    std::string remote_file_name = utils::filesystem::path_combine(PROVIDER, FILE_NAME);
    _fs->files[remote_file_name] = std::make_pair(f_meta.size, f_meta.md5);

    ASSERT_EQ(test_do_download(FILE_NAME, false), ERR_OK);
}

// parse metadata unit tests
TEST_F(replica_bulk_load_test, bulk_load_metadata_not_exist)
{
    bulk_load_metadata metadata;
    error_code ec = test_parse_bulk_load_metadata("path_not_exist", metadata);
    ASSERT_EQ(ec, ERR_FILE_OPERATION_FAILED);
}

TEST_F(replica_bulk_load_test, bulk_load_metadata_corrupt)
{
    // create an empty metadata file
    create_file(METADATA);

    bulk_load_metadata metadata;
    std::string metadata_file_name = utils::filesystem::path_combine(LOCAL_DIR, METADATA);
    error_code ec = test_parse_bulk_load_metadata(metadata_file_name, metadata);
    ASSERT_EQ(ec, ERR_CORRUPTION);
}

TEST_F(replica_bulk_load_test, bulk_load_metadata_parse_succeed)
{
    create_file(METADATA);
    std::string metadata_file_name = utils::filesystem::path_combine(LOCAL_DIR, METADATA);
    error_code ec = mock_bulk_load_metadata(metadata_file_name);
    ASSERT_EQ(ec, ERR_OK);

    bulk_load_metadata metadata;
    ec = test_parse_bulk_load_metadata(metadata_file_name, metadata);
    ASSERT_EQ(ec, ERR_OK);
    ASSERT_TRUE(validate_metadata(metadata));
}

// verify file unit tests
TEST_F(replica_bulk_load_test, verify_file_failed)
{
    file_meta f_meta, target;
    create_file_and_get_file_meta(FILE_NAME, f_meta, false);
    construct_file_meta(target, FILE_NAME, f_meta.size, "wrong_md5");

    ASSERT_FALSE(test_verify_sst_files(target));
}

TEST_F(replica_bulk_load_test, verify_file_succeed)
{
    file_meta f_meta, target;
    create_file_and_get_file_meta(FILE_NAME, f_meta, false);
    construct_file_meta(target, FILE_NAME, f_meta.size, f_meta.md5);

    ASSERT_TRUE(test_verify_sst_files(target));
}

// start_downloading unit tests
TEST_F(replica_bulk_load_test, start_downloading)
{
    fail::setup();
    fail::cfg("replica_bulk_load_download_sst_files", "return()");

    mock_bulk_load_request(bulk_load_status::BLS_DOWNLOADING);
    mock_primary_states(false, false);
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_INVALID);
    mock_stub_downloading_count(1);

    ASSERT_EQ(test_start_downloading(), ERR_OK);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_stub_downloading_count(), 2);

    fail::teardown();
}

TEST_F(replica_bulk_load_test, start_downloading_error)
{
    fail::setup();
    fail::cfg("replica_bulk_load_download_sst_files_fs_error", "return()");
    fail::cfg("replica_cleanup_bulk_load_context", "return()");

    mock_bulk_load_request(bulk_load_status::BLS_DOWNLOADING);
    mock_primary_states(false, false);
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_INVALID);
    mock_stub_downloading_count(1);

    ASSERT_EQ(test_start_downloading(), ERR_FS_INTERNAL);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_stub_downloading_count(), 1);

    fail::teardown();
}

TEST_F(replica_bulk_load_test, start_downloading_concurrent_excceed)
{
    mock_bulk_load_request(bulk_load_status::BLS_DOWNLOADING);
    mock_primary_states(false, false);
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_INVALID);
    mock_stub_downloading_count(MAX_DOWNLOADING_COUNT);

    ASSERT_EQ(test_start_downloading(), ERR_BUSY);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_INVALID);
    ASSERT_EQ(get_stub_downloading_count(), MAX_DOWNLOADING_COUNT);
}

// finish download test
TEST_F(replica_bulk_load_test, finish_download_concurrent_decrease)
{
    mock_stub_downloading_count(3);
    mock_bulk_load_context(300, 300, 100, bulk_load_status::BLS_DOWNLOADING);
    test_update_download_progress(0);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADED);
    ASSERT_EQ(get_stub_downloading_count(), 2);
}

// start ingestion test
TEST_F(replica_bulk_load_test, start_ingestion)
{
    mock_bulk_load_request(bulk_load_status::BLS_INGESTING);
    mock_bulk_load_context(50, 50, 100, bulk_load_status::BLS_DOWNLOADED);
    test_start_ingestion();
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_INGESTING);
}

// cleanup bulk load context unit tests
TEST_F(replica_bulk_load_test, cleanup_bulk_load_context_with_succeed)
{
    mock_bulk_load_context(200, 200, 100, bulk_load_status::BLS_SUCCEED);
    ASSERT_TRUE(utils::filesystem::directory_exists(LOCAL_DIR));

    test_cleanup_bulk_load_context(bulk_load_status::BLS_SUCCEED);
    ASSERT_TRUE(get_clean_up_flag());
    ASSERT_FALSE(utils::filesystem::directory_exists(LOCAL_DIR));
}

TEST_F(replica_bulk_load_test, double_cleanup_bulk_load_context)
{
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_INVALID);

    test_cleanup_bulk_load_context(bulk_load_status::BLS_SUCCEED);
    ASSERT_TRUE(get_clean_up_flag());
}

TEST_F(replica_bulk_load_test, cleanup_bulk_load_context_with_failed)
{
    mock_bulk_load_context(1000, 100, 10, bulk_load_status::BLS_DOWNLOADING);

    test_cleanup_bulk_load_context(bulk_load_status::BLS_FAILED);
    ASSERT_TRUE(get_clean_up_flag());
    ASSERT_FALSE(utils::filesystem::directory_exists(LOCAL_DIR));
}

TEST_F(replica_bulk_load_test, cleanup_bulk_load_context_with_cancel)
{
    mock_bulk_load_context(50, 50, 100, bulk_load_status::BLS_DOWNLOADED);

    test_cleanup_bulk_load_context(bulk_load_status::BLS_CANCELED);
    ASSERT_TRUE(get_clean_up_flag());
    ASSERT_FALSE(utils::filesystem::directory_exists(LOCAL_DIR));
}

TEST_F(replica_bulk_load_test, pause_bulk_load_with_invalid)
{
    mock_bulk_load_request(bulk_load_status::BLS_PAUSING);
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_INVALID);
    test_pause_bulk_load();
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_PAUSED);
}

TEST_F(replica_bulk_load_test, pause_bulk_load_with_downloading)
{
    mock_bulk_load_request(bulk_load_status::BLS_PAUSING);
    mock_bulk_load_context(1000, 100, 10, bulk_load_status::BLS_DOWNLOADING);
    test_pause_bulk_load();
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_PAUSED);
}

// report_group_download_progress unit tests
TEST_F(replica_bulk_load_test, report_group_download_progress_all_downloading)
{
    mock_primary_states(true, false);
    mock_bulk_load_context(100, 10, 10, bulk_load_status::BLS_DOWNLOADING);

    bulk_load_response resp;
    test_report_group_download_progress(resp);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.download_progresses.size(), 3);
    ASSERT_EQ(resp.total_download_progress, 3);
}

TEST_F(replica_bulk_load_test, report_group_download_progress_primary_downloaded)
{
    mock_primary_states(true, false);
    mock_bulk_load_context(40, 40, 100, bulk_load_status::BLS_DOWNLOADED);

    bulk_load_response resp;
    test_report_group_download_progress(resp);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.total_download_progress, 33);
}

TEST_F(replica_bulk_load_test, report_group_download_progress_all_downloaded)
{
    mock_primary_states(true, false, true);
    mock_bulk_load_context(50, 50, 100, bulk_load_status::BLS_DOWNLOADED);

    bulk_load_response resp;
    test_report_group_download_progress(resp);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.total_download_progress, 100);
}

TEST_F(replica_bulk_load_test, report_group_ingestion_status_test)
{

    struct ingestion_struct
    {
        ingestion_status::type primary;
        ingestion_status::type secondary1;
        ingestion_status::type secondary2;
        bool is_ingestion_commit;
        bool mock_partition_version;
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
                                                     test.is_ingestion_commit,
                                                     test.mock_partition_version),
                  test.is_group_ingestion_finished);
        ASSERT_EQ(get_partition_version(), PARTITION_VERSION);
    }
}

// report_group_context_clean_flag unit tests
TEST_F(replica_bulk_load_test, report_group_context_clean_flag_in_unhealthy_state)
{
    mock_cleanup_flag_unhealthy();

    bulk_load_response response;
    test_report_group_context_clean_flag(response);
    ASSERT_FALSE(response.is_group_bulk_load_context_cleaned);
}

TEST_F(replica_bulk_load_test, report_group_context_clean_flag_not_cleanup)
{
    mock_primary_states(false, true, true);
    mock_bulk_load_context(80, 80, 100, bulk_load_status::BLS_SUCCEED);

    bulk_load_response response;
    test_report_group_context_clean_flag(response);
    ASSERT_FALSE(response.is_group_bulk_load_context_cleaned);
}

TEST_F(replica_bulk_load_test, report_group_context_clean_flag_all_cleanup)
{
    mock_primary_states(false, true, true);
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_INVALID);

    bulk_load_response response;
    test_report_group_context_clean_flag(response);
    ASSERT_TRUE(response.is_group_bulk_load_context_cleaned);
}

TEST_F(replica_bulk_load_test, report_group_is_paused_not_cleanup)
{
    mock_primary_states(false, false, false, true);
    mock_bulk_load_context(100, 10, 10, bulk_load_status::BLS_DOWNLOADING);

    bulk_load_response response;
    test_report_group_is_paused(response);
    ASSERT_FALSE(response.is_group_bulk_load_paused);
}

TEST_F(replica_bulk_load_test, report_group_is_paused_all_cleanup)
{
    mock_primary_states(false, false, true, true);
    mock_bulk_load_context(100, 10, 10, bulk_load_status::BLS_PAUSED);

    bulk_load_response response;
    test_report_group_is_paused(response);
    ASSERT_TRUE(response.is_group_bulk_load_paused);
}

// on_group_bulk_load_reply unit tests
TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_rpc_error)
{
    mock_primary_states(false, true);
    test_on_group_bulk_load_reply(bulk_load_status::BLS_FAILED,
                                  BALLOT,
                                  bulk_load_status::BLS_FAILED,
                                  0,
                                  true,
                                  ingestion_status::IS_INVALID,
                                  ERR_TIMEOUT);
    bool is_context_cleaned = true;
    ASSERT_EQ(primary_get_node_context_clean_flag(is_context_cleaned), ERR_OK);
    ASSERT_FALSE(is_context_cleaned);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_response_error)
{
    mock_primary_states(true, false, true);
    test_on_group_bulk_load_reply(bulk_load_status::BLS_DOWNLOADED,
                                  BALLOT,
                                  bulk_load_status::BLS_DOWNLOADED,
                                  100,
                                  false,
                                  ingestion_status::IS_INVALID,
                                  ERR_OK,
                                  ERR_INVALID_STATE);
    partition_download_progress download_progress;
    ASSERT_EQ(primary_get_node_download_progress(download_progress), ERR_OK);
    ASSERT_EQ(download_progress.progress, 0);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_ballot_change)
{
    mock_primary_states(true, true, true);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_SUCCEED, BALLOT - 1, bulk_load_status::BLS_SUCCEED, 100, false);
    partition_download_progress download_progress;
    ASSERT_EQ(primary_get_node_download_progress(download_progress), ERR_OK);
    ASSERT_EQ(download_progress.progress, 0);
    bool is_context_cleaned = true;
    ASSERT_EQ(primary_get_node_context_clean_flag(is_context_cleaned), ERR_OK);
    ASSERT_FALSE(is_context_cleaned);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_busy)
{
    mock_primary_states(false, false);
    mock_stub_downloading_count(3);
    test_on_group_bulk_load_reply(bulk_load_status::BLS_DOWNLOADING,
                                  BALLOT,
                                  bulk_load_status::BLS_INVALID,
                                  0,
                                  false,
                                  ingestion_status::IS_INVALID,
                                  ERR_OK,
                                  ERR_BUSY);
    ASSERT_EQ(get_stub_downloading_count(), 3);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_downloading)
{
    mock_primary_states(true, false);
    mock_bulk_load_context(100, 0, 0, bulk_load_status::BLS_DOWNLOADING);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_DOWNLOADING, BALLOT, bulk_load_status::BLS_DOWNLOADING, 60, false);
    partition_download_progress download_progress;
    ASSERT_EQ(primary_get_node_download_progress(download_progress), ERR_OK);
    ASSERT_EQ(download_progress.progress, 60);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_downloading_with_one_downloaded)
{
    mock_primary_states(true, false);
    mock_bulk_load_context(100, 0, 0, bulk_load_status::BLS_DOWNLOADING);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_DOWNLOADING, BALLOT, bulk_load_status::BLS_DOWNLOADED, 100, false);
    partition_download_progress download_progress;
    ASSERT_EQ(primary_get_node_download_progress(download_progress), ERR_OK);
    ASSERT_EQ(download_progress.progress, 100);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_downloaded)
{
    mock_primary_states(true, false, true);
    mock_bulk_load_context(100, 100, 100, bulk_load_status::BLS_DOWNLOADED);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_DOWNLOADED, BALLOT, bulk_load_status::BLS_DOWNLOADED, 100, false);
    partition_download_progress download_progress;
    ASSERT_EQ(primary_get_node_download_progress(download_progress), ERR_OK);
    ASSERT_EQ(download_progress.progress, 100);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_downloaded_with_one_succeed)
{
    mock_primary_states(true, false, true);
    mock_bulk_load_context(100, 100, 100, bulk_load_status::BLS_DOWNLOADED);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_SUCCEED, BALLOT, bulk_load_status::BLS_DOWNLOADED, 100, false);
    bool is_context_cleaned = true;
    ASSERT_EQ(primary_get_node_context_clean_flag(is_context_cleaned), ERR_OK);
    ASSERT_FALSE(is_context_cleaned);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_ingestion)
{
    mock_ingestion_states(ingestion_status::IS_RUNNING, ingestion_status::IS_INVALID);
    mock_bulk_load_context(100, 100, 100, bulk_load_status::BLS_INGESTING);
    test_on_group_bulk_load_reply(bulk_load_status::BLS_INGESTING,
                                  BALLOT,
                                  bulk_load_status::BLS_INGESTING,
                                  100,
                                  false,
                                  ingestion_status::IS_SUCCEED);
    ingestion_status::type istatus;
    ASSERT_EQ(primary_get_node_ingestion_status(istatus), ERR_OK);
    ASSERT_EQ(istatus, ingestion_status::IS_SUCCEED);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_succeed)
{
    mock_primary_states(true, false, true);
    mock_bulk_load_context(100, 100, 100, bulk_load_status::BLS_SUCCEED);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_SUCCEED, BALLOT, bulk_load_status::BLS_SUCCEED, 100, false);
    bool is_context_cleaned = true;
    ASSERT_EQ(primary_get_node_context_clean_flag(is_context_cleaned), ERR_OK);
    ASSERT_FALSE(is_context_cleaned);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_failed)
{
    mock_primary_states(false, true, false);
    mock_bulk_load_context(100, 22, 22, bulk_load_status::BLS_FAILED);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_FAILED, BALLOT, bulk_load_status::BLS_FAILED, 22, true);
    bool is_context_cleaned = false;
    ASSERT_EQ(primary_get_node_context_clean_flag(is_context_cleaned), ERR_OK);
    ASSERT_TRUE(is_context_cleaned);
}

// validate_bulk_load_status unit test
TEST_F(replica_bulk_load_test, validate_bulk_load_status_test)
{
    struct validate_struct
    {
        bulk_load_status::type meta_status;
        bulk_load_status::type local_status;
        bool expected_flag;
    } tests[] = {{bulk_load_status::BLS_INVALID, bulk_load_status::BLS_INVALID, true},
                 {bulk_load_status::BLS_DOWNLOADING, bulk_load_status::BLS_INVALID, true},
                 {bulk_load_status::BLS_FAILED, bulk_load_status::BLS_INGESTING, true},
                 {bulk_load_status::BLS_CANCELED, bulk_load_status::BLS_SUCCEED, true},
                 {bulk_load_status::BLS_DOWNLOADED, bulk_load_status::BLS_INVALID, false},
                 {bulk_load_status::BLS_DOWNLOADED, bulk_load_status::BLS_DOWNLOADED, true},
                 {bulk_load_status::BLS_INGESTING, bulk_load_status::BLS_DOWNLOADED, true},
                 {bulk_load_status::BLS_INGESTING, bulk_load_status::BLS_SUCCEED, false},
                 {bulk_load_status::BLS_SUCCEED, bulk_load_status::BLS_INVALID, true},
                 {bulk_load_status::BLS_SUCCEED, bulk_load_status::BLS_INGESTING, true},
                 {bulk_load_status::BLS_SUCCEED, bulk_load_status::BLS_DOWNLOADING, false},
                 {bulk_load_status::BLS_PAUSING, bulk_load_status::BLS_INVALID, true},
                 {bulk_load_status::BLS_PAUSING, bulk_load_status::BLS_PAUSED, true},
                 {bulk_load_status::BLS_PAUSING, bulk_load_status::BLS_INGESTING, false}};

    for (auto test : tests) {
        ASSERT_EQ(validate_status(test.meta_status, test.local_status), test.expected_flag);
    }
}

} // namespace replication
} // namespace dsn
