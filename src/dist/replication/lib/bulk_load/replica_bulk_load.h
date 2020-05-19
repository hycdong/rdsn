// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dist/replication/lib/replica.h>
#include <dist/replication/lib/replica_stub.h>

namespace dsn {
namespace replication {

class replica_bulk_load : replica_base
{
public:
    explicit replica_bulk_load(replica *r);
    ~replica_bulk_load();

private:
    //
    // bulk load core functions
    //
    void on_bulk_load(const bulk_load_request &request, /*out*/ bulk_load_response &response);
    void broadcast_group_bulk_load(const bulk_load_request &meta_req);
    void on_group_bulk_load(const group_bulk_load_request &request,
                            /*out*/ group_bulk_load_response &response);
    void on_group_bulk_load_reply(error_code err,
                                  const group_bulk_load_request &req,
                                  const group_bulk_load_response &resp);

    error_code do_bulk_load(const std::string &app_name,
                            bulk_load_status::type meta_status,
                            const std::string &cluster_name,
                            const std::string &provider_name);

    // compare meta bulk load status and local bulk load status
    // \return ERR_INVALID_STATE if local bulk load status is invalid
    error_code validate_bulk_load_status(bulk_load_status::type meta_status,
                                         bulk_load_status::type local_status);

    // replica start or restart download sst files from remote provider
    // \return ERR_BUSY if node has already had enought replica executing downloading
    // \return download errors by function `download_sst_files`
    error_code bulk_load_start_download(const std::string &app_name,
                                        const std::string &cluster_name,
                                        const std::string &provider_name);

    // \return ERR_FILE_OPERATION_FAILED: create local bulk load dir failed
    // \return download metadata file error, see function `do_download`
    // \return parse metadata file error, see function `parse_bulk_load_metadata`
    error_code download_sst_files(const std::string &app_name,
                                  const std::string &cluster_name,
                                  const std::string &provider_name);

    // download file from remote file system
    // download_err = ERR_FILE_OPERATION_FAILED: local file system errors
    // download_err = ERR_FS_INTERNAL: remote file system error
    // download_err = ERR_CORRUPTION: file not exist or damaged or not pass verify
    // if download file succeed, download_err = ERR_OK and set download_file_size
    void do_download(const std::string &remote_dir,
                     const std::string &local_dir,
                     const std::string &file_name,
                     dist::block_service::block_filesystem *fs,
                     /*out*/ error_code &download_err,
                     /*out*/ uint64_t &download_file_size);

    // \return ERR_FILE_OPERATION_FAILED: file not exist, get size failed, open file failed
    // \return ERR_CORRUPTION: parse failed
    error_code parse_bulk_load_metadata(const std::string &fname, /*out*/ bulk_load_metadata &meta);

    bool verify_sst_files(const file_meta &f_meta, const std::string &local_dir);
    void update_bulk_load_download_progress(uint64_t file_size, const std::string &file_name);

    void try_decrease_bulk_load_download_count();
    void bulk_load_check_download_finish();
    void bulk_load_start_ingestion();
    void bulk_load_check_ingestion_finish();
    void handle_bulk_load_succeed();
    void handle_bulk_load_finish(bulk_load_status::type new_status);
    error_code remove_local_bulk_load_dir(const std::string &bulk_load_dir);
    void clear_bulk_load_states();

    void pause_bulk_load();

    // only called by primary
    void report_bulk_load_states_to_meta(bulk_load_status::type remote_status,
                                         bool report_metadata,
                                         /*out*/ bulk_load_response &response);
    void report_group_download_progress(/*out*/ bulk_load_response &response);
    void report_group_ingestion_status(/*out*/ bulk_load_response &response);
    void report_group_context_clean_flag(/*out*/ bulk_load_response &response);
    void report_group_is_paused(/*out*/ bulk_load_response &response);

    // only called by secondary
    void report_bulk_load_states_to_primary(bulk_load_status::type remote_status,
                                            /*out*/ group_bulk_load_response &response);

    //
    // bulk load helper functions
    //
    inline std::string get_remote_bulk_load_dir(const std::string &app_name,
                                                const std::string &cluster_name,
                                                uint32_t pidx) const
    {
        std::ostringstream oss;
        oss << _replica->_options->bulk_load_provider_root << "/" << cluster_name << "/" << app_name
            << "/" << pidx;
        return oss.str();
    }
    bulk_load_status::type get_bulk_load_status() { return _replica->_bulk_load_context._status; }
    void set_bulk_load_status(bulk_load_status::type status)
    {
        _replica->_bulk_load_context._status = status;
    }

    //
    // helper functions
    //
    partition_status::type status() const { return _replica->status(); }
    ballot get_ballot() const { return _replica->get_ballot(); }
    task_tracker *tracker() { return _replica->tracker(); }

private:
    replica *_replica;

    friend class replica;
    friend class replica_stub;
    friend class replica_bulk_load_test;
};

} // namespace replication
} // namespace dsn
