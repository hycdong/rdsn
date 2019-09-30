// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

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

    void test_handle_bulk_load_error() { _replica->handle_bulk_load_error(); }

    /// mock structure functions
    void mock_bulk_load_request()
    {
        partition_bulk_load_info pinfo;
        pinfo.status = bulk_load_status::BLS_INVALID;

        _req.app_bl_status = bulk_load_status::BLS_INVALID;
        _req.app_name = APP_NAME;
        _req.cluster_name = CLUSTER;
        _req.partition_bl_info = pinfo;
        _req.pid = PID;
        _req.remote_provider_name = PROVIDER;
    }

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

    void set_bulk_load_context(uint64_t file_total_size,
                               uint64_t cur_download_size = 0,
                               int32_t download_progress = 0,
                               bulk_load_status::type status = bulk_load_status::BLS_INVALID,
                               bool clean_up = false)
    {
        _replica->_bulk_load_context._file_total_size = file_total_size;
        _replica->_bulk_load_context._cur_download_size = cur_download_size;
        _replica->_bulk_load_context._download_progress = download_progress;
        _replica->_bulk_load_context._status = status;
        _replica->_bulk_load_context._clean_up = clean_up;
    }

    void mock_primary_states(bool mock_download_progress = true, bool mock_cleanup_flag = true)
    {
        _replica->as_primary();

        partition_configuration config;
        config.max_replica_count = 3;
        config.pid = PID;
        config.primary = rpc_address("127.0.0.2", 34801);
        config.secondaries.emplace_back(rpc_address("127.0.0.3", 34801));
        config.secondaries.emplace_back(rpc_address("127.0.0.4", 34801));
        _replica->_primary_states.membership = config;

        if (mock_download_progress) {
            partition_download_progress mock_progress;
            mock_progress.pid = PID;
            mock_progress.progress = 0;
            _replica->_primary_states.group_download_progress[rpc_address("127.0.0.3", 34801)] =
                mock_progress;
            _replica->_primary_states.group_download_progress[rpc_address("127.0.0.4", 34801)] =
                mock_progress;
            _replica->_bld_progress.pid = PID;
            _replica->_bld_progress.progress = 100;
        }
        if (mock_cleanup_flag) {
            _replica->_primary_states
                .group_bulk_load_context_flag[rpc_address("127.0.0.3", 34801)] = true;
            _replica->_primary_states
                .group_bulk_load_context_flag[rpc_address("127.0.0.4", 34801)] = false;
        }
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
    bool get_clean_up_flag() { return _replica->_bulk_load_context._clean_up; }

public:
    std::unique_ptr<mock_replica_stub> _stub;
    std::unique_ptr<mock_replica> _replica;
    std::unique_ptr<block_service_mock> _fs;
    bulk_load_request _req;
    bulk_load_metadata _metadata;

    std::string APP_NAME = "replica";
    std::string CLUSTER = "cluster";
    std::string PROVIDER = "local_service";
    std::string LOCAL_DIR = ".bulk_load";
    std::string METADATA = "bulk_load_metadata";
    std::string FILE_NAME = "test_sst_file";
    gpid PID = gpid(1, 0);
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
    set_bulk_load_context(f_meta.size);

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
    mock_primary_states(true, false);

    bulk_load_response response;
    response.pid = PID;
    test_update_group_download_progress(response);
    ASSERT_EQ(response.download_progresses.size(), 3);
    ASSERT_EQ(response.total_download_progress, 33);
}

TEST_F(replica_bulk_load_test, update_group_context_clean_flag)
{
    mock_primary_states(false, true);

    bulk_load_response response;
    response.pid = PID;

    test_update_group_context_clean_flag(response);
    ASSERT_FALSE(response.is_group_bulk_load_context_cleaned);
}

TEST_F(replica_bulk_load_test, handle_bulk_load_error)
{
    set_bulk_load_context(1000, 100, 10, bulk_load_status::BLS_DOWNLOADING, false);

    test_handle_bulk_load_error();
    ASSERT_TRUE(get_clean_up_flag());
    ASSERT_FALSE(utils::filesystem::directory_exists(LOCAL_DIR));
}

} // namespace replication
} // namespace dsn
