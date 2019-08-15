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
        create_bulk_load_dir();
    }

    void TearDown() { remove_bulk_load_dir(); }

public:
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

    void create_bulk_load_dir() { utils::filesystem::create_directory(LOCAL_DIR); }

    void remove_bulk_load_dir() { utils::filesystem::remove_path(LOCAL_DIR); }

    error_code mock_bulk_load_metadata(std::string file_path)
    {
        // mock bulk_load_metadata file
        file_meta f;
        f.name = "mock";
        f.size = 2333;
        f.md5 = "thisisamockmd5";
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

    void set_bulk_load_context(uint64_t file_total_size,
                               uint64_t cur_download_size = 0,
                               int32_t download_progress = 0,
                               bool clean_up = false)
    {
        _replica->_bulk_load_context._file_total_size = file_total_size;
        _replica->_bulk_load_context._cur_download_size = cur_download_size;
        _replica->_bulk_load_context._download_progress = download_progress;
        _replica->_bulk_load_context._clean_up = clean_up;
    }

    uint64_t get_cur_download_size()
    {
        return _replica->_bulk_load_context._cur_download_size.load();
    }

    int32_t get_download_progress()
    {
        return _replica->_bulk_load_context._download_progress.load();
    }

public:
    std::unique_ptr<mock_replica_stub> _stub;
    std::unique_ptr<mock_replica> _replica;
    std::unique_ptr<block_service_mock> _fs;
    bulk_load_request _req;
    bulk_load_metadata _metadata;

    std::string APP_NAME = "replica";
    std::string CLUSTER = "cluster";
    std::string PROVIDER = "local_service";
    std::string LOCAL_DIR = "bulk_load";
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
    // create metadata
    std::string metadata_file_name = utils::filesystem::path_combine(LOCAL_DIR, METADATA);
    utils::filesystem::create_file(metadata_file_name);

    bulk_load_metadata metadata;
    error_code ec = test_read_bulk_load_metadata(metadata_file_name, metadata);
    ASSERT_EQ(ec, ERR_CORRUPTION);
}

TEST_F(replica_bulk_load_test, bulk_load_metadata_parse_succeed)
{
    std::string metadata_file_name = utils::filesystem::path_combine(LOCAL_DIR, METADATA);
    utils::filesystem::create_file(metadata_file_name);
    // mock metadata
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
    std::string file_name = utils::filesystem::path_combine(LOCAL_DIR, FILE_NAME);
    utils::filesystem::create_file(file_name);

    // mock remote file
    std::string remote_whole_file_name = utils::filesystem::path_combine(PROVIDER, FILE_NAME);
    _fs->files[remote_whole_file_name] = std::make_pair(2333, "md5notmatch");

    error_code ec;
    test_do_download(PROVIDER, LOCAL_DIR, FILE_NAME, false, ec);
    ASSERT_EQ(ec, ERR_OK);
}

TEST_F(replica_bulk_load_test, do_download_file_exist)
{
    std::string file_name = utils::filesystem::path_combine(LOCAL_DIR, METADATA);
    utils::filesystem::create_file(file_name);
    // mock metadata
    error_code ec = mock_bulk_load_metadata(file_name);
    ASSERT_EQ(ec, ERR_OK);

    std::string md5;
    utils::filesystem::md5sum(file_name, md5);
    int64_t size;
    utils::filesystem::file_size(file_name, size);

    // mock remote file
    std::string remote_whole_file_name = utils::filesystem::path_combine(PROVIDER, METADATA);
    _fs->files[remote_whole_file_name] = std::make_pair(size, md5);

    // mock bulk_load context
    set_bulk_load_context(size);

    test_do_download(PROVIDER, LOCAL_DIR, METADATA, true, ec);
    ASSERT_EQ(ec, ERR_OK);

    ASSERT_EQ(size, get_cur_download_size());
    ASSERT_EQ(100, get_download_progress());
}

TEST_F(replica_bulk_load_test, do_download_succeed)
{
    std::string file_name = utils::filesystem::path_combine(LOCAL_DIR, FILE_NAME);
    utils::filesystem::create_file(file_name);
    std::string md5;
    utils::filesystem::md5sum(file_name, md5);
    int64_t size;
    utils::filesystem::file_size(file_name, size);

    utils::filesystem::remove_path(file_name);

    // mock remote file
    std::string remote_whole_file_name = utils::filesystem::path_combine(PROVIDER, FILE_NAME);
    _fs->files[remote_whole_file_name] = std::make_pair(size, md5);

    error_code ec;
    test_do_download(PROVIDER, LOCAL_DIR, FILE_NAME, false, ec);
    ASSERT_EQ(ec, ERR_OK);
}

} // namespace replication
} // namespace dsn
