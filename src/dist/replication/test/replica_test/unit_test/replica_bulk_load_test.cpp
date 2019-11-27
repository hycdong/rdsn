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
    error_code test_read_bulk_load_metadata(const std::string &file_path, bulk_load_metadata &meta)
    {
        return _replica->read_bulk_load_metadata(file_path, meta);
    }

    void test_do_download(const std::string &remote_file_dir,
                          const std::string &local_file_dir,
                          const std::string &remote_file_name,
                          bool is_update_progress,
                          dsn::error_code &err)
    {
        dsn::task_tracker tracker;
        _replica->do_download(remote_file_dir,
                              local_file_dir,
                              remote_file_name,
                              _fs.get(),
                              is_update_progress,
                              err,
                              tracker);
    }

    bool test_verify_sst_files(const file_meta &f_meta, const std::string &dir)
    {
        return _replica->verify_sst_files(f_meta, dir);
    }

    void test_update_group_download_progress(bulk_load_response &response)
    {
        _replica->update_group_download_progress(response);
    }

    void test_update_group_context_clean_flag(bulk_load_response &response)
    {
        _replica->update_group_context_clean_flag(response);
    }

    void test_cleanup_bulk_load_context(bulk_load_status::type status)
    {
        _replica->cleanup_bulk_load_context(status);
    }

    void test_on_bulk_load(bulk_load_response &resp) { _replica->on_bulk_load(_req, resp); }

    void
    test_on_group_bulk_load(bulk_load_status::type status, ballot b, group_bulk_load_response &resp)
    {
        mock_group_bulk_load_request(status, b);
        _replica->on_group_bulk_load(_group_req, resp);
    }

    void test_on_group_bulk_load_reply(bulk_load_status::type req_status,
                                       ballot req_ballot,
                                       bulk_load_status::type resp_status,
                                       int32_t progress,
                                       bool is_context_cleaned,
                                       error_code rpc_error = ERR_OK,
                                       error_code error = ERR_OK)
    {
        mock_group_bulk_load_request(req_status, req_ballot);
        partition_download_progress download_progress;
        download_progress.pid = PID;
        download_progress.progress = progress;
        download_progress.status = ERR_OK;
        std::shared_ptr<group_bulk_load_request> req =
            std::make_shared<group_bulk_load_request>(_group_req);
        std::shared_ptr<group_bulk_load_response> resp =
            std::make_shared<group_bulk_load_response>();
        resp->err = error;
        resp->pid = PID;
        resp->status = resp_status;
        resp->__set_is_bulk_load_context_cleaned(is_context_cleaned);
        resp->__set_download_progress(download_progress);

        _replica->on_group_bulk_load_reply(rpc_error, req, resp);
    }

    void test_update_download_progress() { _replica->update_download_progress(); }

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

    void mock_bulk_load_request(bulk_load_status::type app_status,
                                bulk_load_status::type partition_status,
                                ballot b)
    {
        _req.app_bulk_load_status = app_status;
        _req.app_name = APP_NAME;
        _req.ballot = b;
        _req.cluster_name = CLUSTER;
        _req.partition_bulk_load_status = partition_status;
        _req.pid = PID;
        _req.remote_provider_name = PROVIDER;
    }

    void mock_bulk_load_request(bulk_load_status::type status)
    {
        mock_bulk_load_request(status, status, BALLOT);
    }

    void mock_bulk_load_request(bulk_load_status::type status, ballot b)
    {
        mock_bulk_load_request(status, status, b);
    }

    void mock_group_bulk_load_request(bulk_load_status::type status, ballot b)
    {
        app_info app;
        app.app_name = APP_NAME;
        _group_req.app = app;
        _group_req.meta_app_bulk_load_status = status;
        _group_req.meta_partition_bulk_load_status = status;
        _group_req.config.status = partition_status::PS_SECONDARY;
        _group_req.config.ballot = b;
        _group_req.target_address = SECONDARY;
    }

    void mock_bulk_load_context(uint64_t file_total_size,
                                uint64_t cur_download_size = 0,
                                int32_t download_progress = 0,
                                bulk_load_status::type status = bulk_load_status::BLS_INVALID)
    {
        _replica->_bulk_load_context._file_total_size = file_total_size;
        _replica->_bulk_load_context._cur_download_size = cur_download_size;
        _replica->_bulk_load_context._download_progress = download_progress;
        _replica->_bulk_load_context._status = status;
        _replica->_bulk_load_download_progress.pid = PID;
        _replica->_bulk_load_download_progress.progress = download_progress;
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
                             bool all_secondary_finish = false)
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
            mock_progress.pid = PID;
            mock_progress.progress = all_secondary_finish ? 100 : 0;
            _replica->_primary_states.group_download_progress[SECONDARY] = mock_progress;
            _replica->_primary_states.group_download_progress[SECONDARY2] = mock_progress;
        }
        if (mock_cleanup_flag) {
            _replica->_primary_states.group_bulk_load_context_flag[SECONDARY] = true;
            _replica->_primary_states.group_bulk_load_context_flag[SECONDARY2] =
                all_secondary_finish;
        }
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

    /// getter functions
    uint64_t get_cur_download_size()
    {
        return _replica->_bulk_load_context._cur_download_size.load();
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
};

TEST_F(replica_bulk_load_test, bulk_load_metadata_not_exist)
{
    bulk_load_metadata metadata;
    error_code ec = test_read_bulk_load_metadata("path_not_exist", metadata);
    ASSERT_EQ(ec, ERR_FILE_OPERATION_FAILED);
}

TEST_F(replica_bulk_load_test, bulk_load_metadata_corrupt)
{
    // create an empty metadata file
    create_file(METADATA);

    bulk_load_metadata metadata;
    std::string metadata_file_name = utils::filesystem::path_combine(LOCAL_DIR, METADATA);
    error_code ec = test_read_bulk_load_metadata(metadata_file_name, metadata);
    ASSERT_EQ(ec, ERR_CORRUPTION);
}

TEST_F(replica_bulk_load_test, bulk_load_metadata_parse_succeed)
{
    create_file(METADATA);
    std::string metadata_file_name = utils::filesystem::path_combine(LOCAL_DIR, METADATA);
    error_code ec = mock_bulk_load_metadata(metadata_file_name);
    ASSERT_EQ(ec, ERR_OK);

    bulk_load_metadata metadata;
    ec = test_read_bulk_load_metadata(metadata_file_name, metadata);
    ASSERT_EQ(ec, ERR_OK);
    ASSERT_TRUE(validate_metadata(metadata));
}

TEST_F(replica_bulk_load_test, do_download_file_not_exist)
{
    error_code ec;
    test_do_download(PROVIDER, LOCAL_DIR, FILE_NAME, false, ec);
    ASSERT_EQ(ec, ERR_CORRUPTION);
}

TEST_F(replica_bulk_load_test, do_download_file_md5_not_match)
{
    create_file(FILE_NAME);
    // mock remote file
    std::string remote_whole_file_name = utils::filesystem::path_combine(PROVIDER, FILE_NAME);
    _fs->files[remote_whole_file_name] = std::make_pair(2333, "md5_not_match");

    error_code ec;
    test_do_download(PROVIDER, LOCAL_DIR, FILE_NAME, false, ec);
    ASSERT_EQ(ec, ERR_OK);
}

TEST_F(replica_bulk_load_test, do_download_file_exist)
{
    file_meta f_meta;
    create_file_and_get_file_meta(METADATA, f_meta, true);

    // mock remote file
    std::string remote_whole_file_name = utils::filesystem::path_combine(PROVIDER, METADATA);
    _fs->files[remote_whole_file_name] = std::make_pair(f_meta.size, f_meta.md5);

    // mock bulk_load context
    mock_bulk_load_context(f_meta.size);

    error_code ec;
    test_do_download(PROVIDER, LOCAL_DIR, METADATA, true, ec);
    ASSERT_EQ(ec, ERR_OK);

    ASSERT_EQ(f_meta.size, get_cur_download_size());
    ASSERT_EQ(100, get_download_progress());
}

TEST_F(replica_bulk_load_test, do_download_succeed)
{
    file_meta f_meta;
    create_file_and_get_file_meta(FILE_NAME, f_meta, false);

    std::string file_name = utils::filesystem::path_combine(LOCAL_DIR, FILE_NAME);
    utils::filesystem::remove_path(file_name);

    // mock remote file
    std::string remote_whole_file_name = utils::filesystem::path_combine(PROVIDER, FILE_NAME);
    _fs->files[remote_whole_file_name] = std::make_pair(f_meta.size, f_meta.md5);

    error_code ec;
    test_do_download(PROVIDER, LOCAL_DIR, FILE_NAME, false, ec);
    ASSERT_EQ(ec, ERR_OK);
}

TEST_F(replica_bulk_load_test, verify_file_failed)
{
    file_meta f_meta, target;
    create_file_and_get_file_meta(FILE_NAME, f_meta, false);
    construct_file_meta(target, FILE_NAME, f_meta.size, "wrong_md5");

    bool flag = test_verify_sst_files(target, LOCAL_DIR);
    ASSERT_FALSE(flag);
}

TEST_F(replica_bulk_load_test, verify_file_succeed)
{
    file_meta f_meta, target;
    create_file_and_get_file_meta(FILE_NAME, f_meta, false);
    construct_file_meta(target, FILE_NAME, f_meta.size, f_meta.md5);

    bool flag = test_verify_sst_files(target, LOCAL_DIR);
    ASSERT_TRUE(flag);
}

TEST_F(replica_bulk_load_test, update_group_download_progress)
{
    mock_bulk_load_context(10, 10, 100);
    mock_primary_states(true, false);

    bulk_load_response response;
    response.pid = PID;
    test_update_group_download_progress(response);
    ASSERT_EQ(response.download_progresses.size(), 3);
    ASSERT_EQ(response.total_download_progress, 33);
}

TEST_F(replica_bulk_load_test, update_group_context_clean_flag_in_unhealthy_state)
{
    mock_cleanup_flag_unhealthy();

    bulk_load_response response;
    response.pid = PID;

    test_update_group_context_clean_flag(response);
    ASSERT_FALSE(response.is_group_bulk_load_context_cleaned);
}

TEST_F(replica_bulk_load_test, update_group_context_clean_flag)
{
    mock_primary_states(false, true);

    bulk_load_response response;
    response.pid = PID;

    test_update_group_context_clean_flag(response);
    ASSERT_FALSE(response.is_group_bulk_load_context_cleaned);
}

TEST_F(replica_bulk_load_test, handle_bulk_load_downloading_error)
{
    mock_bulk_load_context(1000, 100, 10, bulk_load_status::BLS_DOWNLOADING);

    test_cleanup_bulk_load_context(bulk_load_status::BLS_FAILED);
    ASSERT_TRUE(get_clean_up_flag());
    ASSERT_FALSE(utils::filesystem::directory_exists(LOCAL_DIR));
}

TEST_F(replica_bulk_load_test, handle_bulk_load_finish)
{
    mock_bulk_load_context(200, 200, 100, bulk_load_status::BLS_DOWNLOADED);

    test_cleanup_bulk_load_context(bulk_load_status::BLS_FINISH);
    ASSERT_TRUE(get_clean_up_flag());
    ASSERT_FALSE(utils::filesystem::directory_exists(LOCAL_DIR));
}

TEST_F(replica_bulk_load_test, double_cleanup_bulk_load_context)
{
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_INVALID);

    test_cleanup_bulk_load_context(bulk_load_status::BLS_FINISH);
    ASSERT_TRUE(get_clean_up_flag());
}

/// on_bulk_load unit tests
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

// request: downoading; local:invalid
TEST_F(replica_bulk_load_test, on_bulk_load_start_downloading)
{
    fail::setup();
    fail::cfg("replica_broadcast_group_bulk_load", "return()");
    fail::cfg("replica_bulk_load_download_sst_files", "return()");

    mock_bulk_load_request(bulk_load_status::BLS_DOWNLOADING);
    mock_primary_states(false, false);
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_INVALID);
    mock_stub_downloading_count(1);

    bulk_load_response resp;
    test_on_bulk_load(resp);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.primary_bulk_load_status, bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(resp.total_download_progress, 0);
    ASSERT_FALSE(resp.__isset.is_group_bulk_load_context_cleaned);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_stub_downloading_count(), 2);

    fail::teardown();
}

// request: downoading; local:invalid
TEST_F(replica_bulk_load_test, on_bulk_load_downloading_error)
{
    fail::setup();
    fail::cfg("replica_broadcast_group_bulk_load", "return()");
    fail::cfg("replica_bulk_load_download_sst_files_fs_error", "return()");
    fail::cfg("replica_cleanup_bulk_load_context", "return()");

    mock_bulk_load_request(bulk_load_status::BLS_DOWNLOADING);
    mock_primary_states(false, false);
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_INVALID);
    mock_stub_downloading_count(1);

    bulk_load_response resp;
    test_on_bulk_load(resp);

    ASSERT_EQ(resp.err, ERR_FS_INTERNAL);
    ASSERT_EQ(resp.primary_bulk_load_status, bulk_load_status::BLS_FAILED);
    ASSERT_FALSE(resp.__isset.is_group_bulk_load_context_cleaned);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_FAILED);
    ASSERT_EQ(get_stub_downloading_count(), 1);

    fail::teardown();
}

// request: downoading; local:invalid
TEST_F(replica_bulk_load_test, on_bulk_load_start_downloading_concurrent_excceed)
{
    mock_bulk_load_request(bulk_load_status::BLS_DOWNLOADING);
    mock_primary_states(false, false);
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_INVALID);
    mock_stub_downloading_count(MAX_DOWNLOADING_COUNT);

    bulk_load_response resp;
    test_on_bulk_load(resp);

    ASSERT_EQ(resp.err, ERR_BUSY);
    ASSERT_EQ(resp.primary_bulk_load_status, bulk_load_status::BLS_INVALID);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_INVALID);
    ASSERT_EQ(get_stub_downloading_count(), MAX_DOWNLOADING_COUNT);
}

// request: downoading; local:downloading
TEST_F(replica_bulk_load_test, on_bulk_load_downloading)
{
    fail::setup();
    fail::cfg("replica_broadcast_group_bulk_load", "return()");

    mock_bulk_load_request(bulk_load_status::BLS_DOWNLOADING);
    mock_primary_states(true, false);
    mock_bulk_load_context(30, 9, 30, bulk_load_status::BLS_DOWNLOADING);
    mock_stub_downloading_count(2);

    bulk_load_response resp;
    test_on_bulk_load(resp);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.primary_bulk_load_status, bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(resp.total_download_progress, 10);
    ASSERT_FALSE(resp.__isset.is_group_bulk_load_context_cleaned);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_stub_downloading_count(), 2);

    fail::teardown();
}

// request: downoading; local:downloaded
TEST_F(replica_bulk_load_test, on_bulk_load_downloading_with_primary_downloaded)
{
    fail::setup();
    fail::cfg("replica_broadcast_group_bulk_load", "return()");

    mock_bulk_load_request(bulk_load_status::BLS_DOWNLOADING);
    mock_primary_states(true, false);
    mock_bulk_load_context(40, 40, 100, bulk_load_status::BLS_DOWNLOADED);

    bulk_load_response resp;
    test_on_bulk_load(resp);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.primary_bulk_load_status, bulk_load_status::BLS_DOWNLOADED);
    ASSERT_EQ(resp.total_download_progress, 33);
    ASSERT_FALSE(resp.__isset.is_group_bulk_load_context_cleaned);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADED);

    fail::teardown();
}

// request: downoaded; local:downloaded
TEST_F(replica_bulk_load_test, on_bulk_load_downloaded)
{
    fail::setup();
    fail::cfg("replica_broadcast_group_bulk_load", "return()");

    mock_bulk_load_request(bulk_load_status::BLS_DOWNLOADED);
    mock_primary_states(true, false, true);
    mock_bulk_load_context(50, 50, 100, bulk_load_status::BLS_DOWNLOADED);

    bulk_load_response resp;
    test_on_bulk_load(resp);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.primary_bulk_load_status, bulk_load_status::BLS_DOWNLOADED);
    ASSERT_EQ(resp.total_download_progress, 100);
    ASSERT_FALSE(resp.__isset.is_group_bulk_load_context_cleaned);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADED);

    fail::teardown();
}

// request: finish; local:downloaded
TEST_F(replica_bulk_load_test, on_bulk_load_finish)
{
    fail::setup();
    fail::cfg("replica_broadcast_group_bulk_load", "return()");
    fail::cfg("replica_handle_bulk_load_succeed", "return()");

    mock_bulk_load_request(bulk_load_status::BLS_FINISH);
    mock_primary_states(true, false, true);
    mock_bulk_load_context(60, 60, 100, bulk_load_status::BLS_DOWNLOADED);

    bulk_load_response resp;
    test_on_bulk_load(resp);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.primary_bulk_load_status, bulk_load_status::BLS_FINISH);
    ASSERT_EQ(resp.total_download_progress, 100);
    ASSERT_FALSE(resp.__isset.is_group_bulk_load_context_cleaned);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_FINISH);

    fail::teardown();
}

// request: finish; local:finish
TEST_F(replica_bulk_load_test, on_bulk_load_finish_primary_cleanup)
{
    fail::setup();
    fail::cfg("replica_broadcast_group_bulk_load", "return()");

    mock_bulk_load_request(bulk_load_status::BLS_FINISH);
    mock_primary_states(false, true);
    mock_bulk_load_context(70, 70, 100, bulk_load_status::BLS_FINISH);

    bulk_load_response resp;
    test_on_bulk_load(resp);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.primary_bulk_load_status, bulk_load_status::BLS_INVALID);
    ASSERT_FALSE(resp.__isset.total_download_progress);
    ASSERT_FALSE(resp.is_group_bulk_load_context_cleaned);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_INVALID);

    fail::teardown();
}

// request: finish; local:finish
TEST_F(replica_bulk_load_test, on_bulk_load_finish_all_cleanup)
{
    fail::setup();
    fail::cfg("replica_broadcast_group_bulk_load", "return()");

    mock_bulk_load_request(bulk_load_status::BLS_FINISH);
    mock_primary_states(false, true, true);
    mock_bulk_load_context(80, 80, 100, bulk_load_status::BLS_INVALID);

    bulk_load_response resp;
    test_on_bulk_load(resp);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.primary_bulk_load_status, bulk_load_status::BLS_INVALID);
    ASSERT_FALSE(resp.__isset.total_download_progress);
    ASSERT_TRUE(resp.is_group_bulk_load_context_cleaned);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_INVALID);

    fail::teardown();
}

// request: downloading; local:failed
TEST_F(replica_bulk_load_test, on_bulk_load_failed_with_app_not_failed)
{
    fail::setup();
    fail::cfg("replica_broadcast_group_bulk_load", "return()");

    mock_bulk_load_request(bulk_load_status::BLS_DOWNLOADING, bulk_load_status::BLS_FAILED, BALLOT);
    mock_primary_states(false, false);
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_FAILED);

    bulk_load_response resp;
    test_on_bulk_load(resp);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.primary_bulk_load_status, bulk_load_status::BLS_FAILED);
    ASSERT_FALSE(resp.__isset.total_download_progress);
    ASSERT_FALSE(resp.__isset.is_group_bulk_load_context_cleaned);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_FAILED);

    fail::teardown();
}

// request: failed; local:failed
TEST_F(replica_bulk_load_test, on_bulk_load_failed_all_cleanup)
{
    fail::setup();
    fail::cfg("replica_broadcast_group_bulk_load", "return()");

    mock_bulk_load_request(bulk_load_status::BLS_FAILED);
    mock_primary_states(false, true, true);
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_FAILED);

    bulk_load_response resp;
    test_on_bulk_load(resp);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.primary_bulk_load_status, bulk_load_status::BLS_INVALID);
    ASSERT_FALSE(resp.__isset.total_download_progress);
    ASSERT_TRUE(resp.is_group_bulk_load_context_cleaned);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_INVALID);

    fail::teardown();
}
/// on_group_bulk_load unit tests

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
    test_on_group_bulk_load(bulk_load_status::BLS_DOWNLOADING, BALLOT + 1, resp);

    ASSERT_EQ(resp.err, ERR_INVALID_STATE);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_wrong_partition_status)
{
    mock_replica_config(partition_status::PS_INACTIVE);

    group_bulk_load_response resp;
    test_on_group_bulk_load(bulk_load_status::BLS_DOWNLOADING, BALLOT, resp);

    ASSERT_EQ(resp.err, ERR_INVALID_STATE);
}

// request: downoading; local:invalid
TEST_F(replica_bulk_load_test, on_group_bulk_load_start_downloading)
{
    fail::setup();
    fail::cfg("replica_bulk_load_download_sst_files", "return()");

    mock_replica_config(partition_status::PS_SECONDARY);
    mock_bulk_load_context(0, 0, 0);
    mock_stub_downloading_count(0);

    group_bulk_load_response resp;
    test_on_group_bulk_load(bulk_load_status::BLS_DOWNLOADING, BALLOT, resp);

    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.status, bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(resp.download_progress.progress, 0);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_stub_downloading_count(), 1);

    fail::teardown();
}

// request: downoading; local:invalid
TEST_F(replica_bulk_load_test, on_group_bulk_load_start_downloading_failed)
{
    fail::setup();
    fail::cfg("replica_bulk_load_download_sst_files_fs_error", "return()");
    fail::cfg("replica_cleanup_bulk_load_context", "return()");

    mock_replica_config(partition_status::PS_SECONDARY);
    mock_bulk_load_context(0, 0, 0);
    mock_stub_downloading_count(0);

    group_bulk_load_response resp;
    test_on_group_bulk_load(bulk_load_status::BLS_DOWNLOADING, BALLOT, resp);

    ASSERT_EQ(resp.err, ERR_FS_INTERNAL);
    ASSERT_EQ(resp.status, bulk_load_status::BLS_FAILED);
    ASSERT_EQ(resp.download_progress.progress, 0);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_FAILED);
    ASSERT_EQ(get_stub_downloading_count(), 0);

    fail::teardown();
}

// request: downoading; local:invalid
TEST_F(replica_bulk_load_test, on_group_bulk_load_start_downloading_concurrent_excceed)
{
    fail::setup();
    fail::cfg("replica_bulk_load_download_sst_files", "return()");

    mock_replica_config(partition_status::PS_SECONDARY);
    mock_stub_downloading_count(MAX_DOWNLOADING_COUNT);
    mock_bulk_load_context(0, 0, 0);

    group_bulk_load_response resp;
    test_on_group_bulk_load(bulk_load_status::BLS_DOWNLOADING, BALLOT, resp);

    ASSERT_EQ(resp.err, ERR_BUSY);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_INVALID);
    ASSERT_EQ(get_stub_downloading_count(), MAX_DOWNLOADING_COUNT);

    fail::teardown();
}

// request: downoading; local:downloading
TEST_F(replica_bulk_load_test, on_group_bulk_load_downloading)
{
    mock_replica_config(partition_status::PS_SECONDARY);
    mock_bulk_load_context(100, 80, 80, bulk_load_status::BLS_DOWNLOADING);
    mock_stub_downloading_count(1);

    group_bulk_load_response resp;
    test_on_group_bulk_load(bulk_load_status::BLS_DOWNLOADING, BALLOT, resp);

    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.status, bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(resp.download_progress.progress, 80);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_stub_downloading_count(), 1);
}

// request: downoading; local:downloaded
TEST_F(replica_bulk_load_test, on_group_bulk_load_downloaded)
{
    mock_replica_config(partition_status::PS_SECONDARY);
    mock_bulk_load_context(200, 200, 100, bulk_load_status::BLS_DOWNLOADED);

    group_bulk_load_response resp;
    test_on_group_bulk_load(bulk_load_status::BLS_DOWNLOADING, BALLOT, resp);

    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.status, bulk_load_status::BLS_DOWNLOADED);
    ASSERT_EQ(resp.download_progress.progress, 100);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADED);
}

// request: finish; local:downloaded
TEST_F(replica_bulk_load_test, on_group_bulk_load_finish)
{
    mock_replica_config(partition_status::PS_SECONDARY);
    mock_bulk_load_context(200, 200, 100, bulk_load_status::BLS_DOWNLOADED);

    group_bulk_load_response resp;
    test_on_group_bulk_load(bulk_load_status::BLS_FINISH, BALLOT, resp);

    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.status, bulk_load_status::BLS_FINISH);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_FINISH);
}

// request: finish; local:finish
TEST_F(replica_bulk_load_test, on_group_bulk_load_finish_with_cleanup)
{
    mock_replica_config(partition_status::PS_SECONDARY);
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_FINISH);

    group_bulk_load_response resp;
    test_on_group_bulk_load(bulk_load_status::BLS_FINISH, BALLOT, resp);

    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.status, bulk_load_status::BLS_INVALID);
    ASSERT_TRUE(resp.is_bulk_load_context_cleaned);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_INVALID);
}

// request: failed; local:failed
TEST_F(replica_bulk_load_test, on_group_bulk_load_failed)
{
    mock_replica_config(partition_status::PS_SECONDARY);
    mock_bulk_load_context(0, 0, 0, bulk_load_status::BLS_FAILED);

    group_bulk_load_response resp;
    test_on_group_bulk_load(bulk_load_status::BLS_FAILED, BALLOT, resp);

    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.status, bulk_load_status::BLS_INVALID);
    ASSERT_TRUE(resp.is_bulk_load_context_cleaned);
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_INVALID);
}

/// on_group_bulk_load_reply unit tests
TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_rpc_error)
{
    mock_primary_states(false, true);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_FAILED, BALLOT, bulk_load_status::BLS_FAILED, 0, true, ERR_TIMEOUT);
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
        bulk_load_status::BLS_FINISH, BALLOT - 1, bulk_load_status::BLS_FINISH, 100, false);
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
                                  ERR_OK,
                                  ERR_BUSY);
    ASSERT_EQ(get_stub_downloading_count(), 3);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_downloading)
{
    mock_primary_states(true, false);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_DOWNLOADING, BALLOT, bulk_load_status::BLS_DOWNLOADING, 60, false);
    partition_download_progress download_progress;
    ASSERT_EQ(primary_get_node_download_progress(download_progress), ERR_OK);
    ASSERT_EQ(download_progress.progress, 60);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_downloading_with_one_downloaded)
{
    mock_primary_states(true, false);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_DOWNLOADING, BALLOT, bulk_load_status::BLS_DOWNLOADED, 100, false);
    partition_download_progress download_progress;
    ASSERT_EQ(primary_get_node_download_progress(download_progress), ERR_OK);
    ASSERT_EQ(download_progress.progress, 100);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_downloaded)
{
    mock_primary_states(true, false, true);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_DOWNLOADED, BALLOT, bulk_load_status::BLS_DOWNLOADED, 100, false);
    partition_download_progress download_progress;
    ASSERT_EQ(primary_get_node_download_progress(download_progress), ERR_OK);
    ASSERT_EQ(download_progress.progress, 100);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_downloaded_with_one_finish)
{
    mock_primary_states(true, false, true);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_FINISH, BALLOT, bulk_load_status::BLS_DOWNLOADED, 100, false);
    bool is_context_cleaned = true;
    ASSERT_EQ(primary_get_node_context_clean_flag(is_context_cleaned), ERR_OK);
    ASSERT_FALSE(is_context_cleaned);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_finish)
{
    mock_primary_states(true, false, true);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_FINISH, BALLOT, bulk_load_status::BLS_FINISH, 100, false);
    bool is_context_cleaned = true;
    ASSERT_EQ(primary_get_node_context_clean_flag(is_context_cleaned), ERR_OK);
    ASSERT_FALSE(is_context_cleaned);
}

TEST_F(replica_bulk_load_test, on_group_bulk_load_reply_failed)
{
    mock_primary_states(false, true, false);
    test_on_group_bulk_load_reply(
        bulk_load_status::BLS_FAILED, BALLOT, bulk_load_status::BLS_FAILED, 0, true);
    bool is_context_cleaned = false;
    ASSERT_EQ(primary_get_node_context_clean_flag(is_context_cleaned), ERR_OK);
    ASSERT_TRUE(is_context_cleaned);
}

TEST_F(replica_bulk_load_test, concurrent_count_decrease_test)
{
    mock_stub_downloading_count(3);
    mock_bulk_load_context(300, 300, 100, bulk_load_status::BLS_DOWNLOADING);
    test_update_download_progress();
    ASSERT_EQ(get_bulk_load_status(), bulk_load_status::BLS_DOWNLOADED);
    ASSERT_EQ(get_stub_downloading_count(), 2);
}

} // namespace replication
} // namespace dsn
