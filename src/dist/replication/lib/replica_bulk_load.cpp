// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "replica.h"
#include "replica_stub.h"

#include <fstream>

#include <dsn/dist/block_service.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/utility/fail_point.h>
#include <dsn/utility/filesystem.h>

namespace dsn {
namespace replication {

typedef rpc_holder<group_bulk_load_request, group_bulk_load_response> group_bulk_load_rpc;

// ThreadPool: THREAD_POOL_REPLICATION
void replica::on_bulk_load(const bulk_load_request &request, /*out*/ bulk_load_response &response)
{
    _checker.only_one_thread_access();

    response.pid = request.pid;
    response.app_name = request.app_name;
    response.err = ERR_OK;

    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("receive bulk load request with wrong status {}", enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.ballot != get_ballot()) {
        dwarn_replica(
            "receive bulk load request with wrong version, remote ballot={}, local ballot={}",
            request.ballot,
            get_ballot());
        response.err = ERR_INVALID_STATE;
        return;
    }

    ddebug_replica(
        "receive bulk load request, remote provider = {}, cluster_name = {}, app_name = {}, "
        "meta_bulk_load_status = {}, local bulk_load_status = {}",
        request.remote_provider_name,
        request.cluster_name,
        request.app_name,
        enum_to_string(request.meta_bulk_load_status),
        enum_to_string(get_bulk_load_status()));

    error_code ec = do_bulk_load(request.app_name,
                                 request.meta_bulk_load_status,
                                 request.cluster_name,
                                 request.remote_provider_name);
    if (ec != ERR_OK) {
        response.err = ec;
        response.primary_bulk_load_status = get_bulk_load_status();
        return;
    }

    report_bulk_load_states_to_meta(
        request.meta_bulk_load_status, request.query_bulk_load_metadata, response);
    if (response.err != ERR_OK) {
        return;
    }

    broadcast_group_bulk_load(request);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::broadcast_group_bulk_load(const bulk_load_request &meta_req)
{
    if (!_primary_states.learners.empty()) {
        dwarn_replica("has learners, skip broadcast group bulk load request");
        return;
    }

    if (_primary_states.group_bulk_load_pending_replies.size() > 0) {
        dwarn_replica("{} group bulk_load replies are still pending, cancel it firstly",
                      static_cast<int>(_primary_states.group_bulk_load_pending_replies.size()));
        for (const auto &it = _primary_states.group_bulk_load_pending_replies.begin();
             it != _primary_states.group_bulk_load_pending_replies.end();
             ++it) {
            it->second->cancel(true);
        }
        _primary_states.group_bulk_load_pending_replies.clear();
    }

    ddebug_replica("start to broadcast group bulk load");

    for (const auto &addr : _primary_states.membership.secondaries) {
        if (addr == _stub->_primary_address)
            continue;

        auto request = make_unique<group_bulk_load_request>();
        request->app_name = _app_info.app_name;
        request->target_address = addr;
        _primary_states.get_replica_config(partition_status::PS_SECONDARY, request->config);
        request->cluster_name = meta_req.cluster_name;
        request->provider_name = meta_req.remote_provider_name;
        request->meta_bulk_load_status = meta_req.meta_bulk_load_status;

        ddebug_replica("send group_bulk_load_request to {}", addr.to_string());

        group_bulk_load_rpc rpc(
            std::move(request), RPC_GROUP_BULK_LOAD, 0_ms, 0, get_gpid().thread_hash());
        auto callback_task = rpc.call(addr, tracker(), [this, rpc](error_code err) mutable {
            on_group_bulk_load_reply(err, rpc.request(), rpc.response());
        });
        _primary_states.group_bulk_load_pending_replies[addr] = callback_task;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::on_group_bulk_load(const group_bulk_load_request &request,
                                 /*out*/ group_bulk_load_response &response)
{
    _checker.only_one_thread_access();

    response.err = ERR_OK;

    if (request.config.ballot < get_ballot()) {
        response.err = ERR_VERSION_OUTDATED;
        dwarn_replica(
            "receive outdated group_bulk_load request, request ballot({}) VS loca ballot({})",
            request.config.ballot,
            get_ballot());
        return;
    }
    if (request.config.ballot > get_ballot()) {
        response.err = ERR_INVALID_STATE;
        dwarn_replica("receive group_bulk_load request, local ballot is outdated, request "
                      "ballot({}) VS loca ballot({})",
                      request.config.ballot,
                      get_ballot());
        return;
    }
    if (status() != request.config.status) {
        response.err = ERR_INVALID_STATE;
        dwarn_replica("status changed, status should be {}, but {}",
                      enum_to_string(request.config.status),
                      enum_to_string(status()));
        return;
    }

    ddebug_replica("receive group_bulk_load request, primary address = {}, ballot = {}, "
                   "meta_bulk_load_status = {}, local bulk_load_status = {}",
                   request.config.primary.to_string(),
                   request.config.ballot,
                   enum_to_string(request.meta_bulk_load_status),
                   enum_to_string(get_bulk_load_status()));

    error_code ec = do_bulk_load(request.app_name,
                                 request.meta_bulk_load_status,
                                 request.cluster_name,
                                 request.provider_name);
    if (ec != ERR_OK) {
        response.err = ec;
        response.status = get_bulk_load_status();
        return;
    }

    report_bulk_load_states_to_primary(request.meta_bulk_load_status, response);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::on_group_bulk_load_reply(error_code err,
                                       const group_bulk_load_request &req,
                                       const group_bulk_load_response &resp)
{
    _checker.only_one_thread_access();

    if (partition_status::PS_PRIMARY != status()) {
        derror_replica("replica status={}, should be {}",
                       enum_to_string(status()),
                       enum_to_string(partition_status::PS_PRIMARY));
        return;
    }

    _primary_states.group_bulk_load_pending_replies.erase(req.target_address);

    if (err != ERR_OK) {
        derror_replica("failed to receive group_bulk_load_reply from {}, error = {}",
                       req.target_address.to_string(),
                       err.to_string());
        _primary_states.reset_node_bulk_load_states(req.target_address, req.meta_bulk_load_status);
        return;
    }

    if (resp.err != ERR_OK) {
        derror_replica("receive group_bulk_load response from {} failed, error = {}",
                       req.target_address.to_string(),
                       resp.err.to_string());
        _primary_states.reset_node_bulk_load_states(req.target_address, req.meta_bulk_load_status);
        return;
    }

    if (req.config.ballot != get_ballot()) {
        derror_replica("recevied wrong group_bulk_load response from {}, request ballot = {}, "
                       "current ballot = {}",
                       req.target_address.to_string(),
                       req.config.ballot,
                       get_ballot());
        _primary_states.reset_node_bulk_load_states(req.target_address, req.meta_bulk_load_status);
    }

    _primary_states.secondary_bulk_load_states[req.target_address] = resp.bulk_load_state;
}

error_code replica::do_bulk_load(const std::string &app_name,
                                 bulk_load_status::type meta_status,
                                 const std::string &cluster_name,
                                 const std::string &provider_name)
{
    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        return ERR_INVALID_STATE;
    }

    bulk_load_status::type local_status = get_bulk_load_status();
    error_code ec = validate_bulk_load_status(meta_status, local_status);
    if (ec != ERR_OK) {
        derror_replica("invalid bulk load status, remote={}, local={}",
                       enum_to_string(meta_status),
                       enum_to_string(local_status));
        return ec;
    }

    switch (meta_status) {
    case bulk_load_status::BLS_DOWNLOADING:
        // start or restart downloading
        if (local_status == bulk_load_status::BLS_INVALID ||
            local_status == bulk_load_status::BLS_INGESTING ||
            local_status == bulk_load_status::BLS_SUCCEED ||
            local_status == bulk_load_status::BLS_PAUSED) {
            ec = bulk_load_start_download(app_name, cluster_name, provider_name);
        }
        break;
    case bulk_load_status::BLS_INGESTING:
        if (local_status == bulk_load_status::BLS_DOWNLOADED) {
            bulk_load_start_ingestion();
        } else if (local_status == bulk_load_status::BLS_INGESTING &&
                   status() == partition_status::PS_PRIMARY) {
            bulk_load_check_ingestion_finish();
        }
        break;
    case bulk_load_status::BLS_SUCCEED:
        if (local_status == bulk_load_status::BLS_INGESTING) {
            handle_bulk_load_succeed();
        } else if (local_status == bulk_load_status::BLS_SUCCEED) {
            handle_bulk_load_finish(meta_status);
        }
        break;
    case bulk_load_status::BLS_FAILED:
        handle_bulk_load_finish(bulk_load_status::BLS_FAILED);
        _stub->_counter_bulk_load_failed_count->increment();
        break;
    case bulk_load_status::BLS_PAUSING:
        pause_bulk_load();
        break;
    case bulk_load_status::BLS_CANCELED: {
        handle_bulk_load_finish(bulk_load_status::BLS_CANCELED);
    } break;
    default:
        break;
    }
    return ec;
}

error_code replica::validate_bulk_load_status(bulk_load_status::type meta_status,
                                              bulk_load_status::type local_status)
{
    error_code err = ERR_OK;
    switch (meta_status) {
    case bulk_load_status::BLS_DOWNLOADED:
        if (local_status != bulk_load_status::BLS_DOWNLOADED) {
            err = ERR_INVALID_STATE;
        }
        break;
    case bulk_load_status::BLS_INGESTING:
        if (local_status != bulk_load_status::BLS_DOWNLOADED &&
            local_status != bulk_load_status::BLS_INGESTING) {
            err = ERR_INVALID_STATE;
        }
        break;
    case bulk_load_status::BLS_SUCCEED:
        if (local_status != bulk_load_status::BLS_INGESTING &&
            local_status != bulk_load_status::BLS_SUCCEED &&
            local_status != bulk_load_status::BLS_INVALID) {
            err = ERR_INVALID_STATE;
        }
        break;
    case bulk_load_status::BLS_PAUSING:
        if (local_status != bulk_load_status::BLS_INVALID &&
            local_status != bulk_load_status::BLS_DOWNLOADING &&
            local_status != bulk_load_status::BLS_DOWNLOADED &&
            local_status != bulk_load_status::BLS_PAUSING &&
            local_status != bulk_load_status::BLS_PAUSED) {
            err = ERR_INVALID_STATE;
        }
        break;
    default:
        break;
    }
    return err;
}

error_code replica::bulk_load_start_download(const std::string &app_name,
                                             const std::string &cluster_name,
                                             const std::string &provider_name)
{
    if (_stub->_bulk_load_recent_downloading_replica_count >=
        _stub->_max_concurrent_bulk_load_downloading_count) {
        dwarn_replica("node[{}] already has {} replica downloading, wait for next round",
                      _stub->_primary_address_str,
                      _stub->_bulk_load_recent_downloading_replica_count);
        return ERR_BUSY;
    }

    if (status() == partition_status::PS_PRIMARY) {
        _primary_states.cleanup_bulk_load_states();
    }
    _bulk_load_context.cleanup_download_prgress();
    _app->set_ingestion_status(ingestion_status::IS_INVALID);
    _is_bulk_load_ingestion = false;
    _bulk_load_context._bulk_load_start_time_ns = dsn_now_ns();

    set_bulk_load_status(bulk_load_status::BLS_DOWNLOADING);
    _stub->_counter_bulk_load_downloading_count->increment();
    _stub->_bulk_load_recent_downloading_replica_count++;

    // TODO(heyuchen): delete this debug log
    ddebug_replica("concurrent: node[{}] recent_bulk_load_downloading_replica_count={}",
                   _stub->_primary_address_str,
                   _stub->_bulk_load_recent_downloading_replica_count);

    // start download
    ddebug_replica("start to download sst files");

    error_code err = download_sst_files(app_name, cluster_name, provider_name);
    if (err != ERR_OK) {
        try_decrease_bulk_load_download_count();
    }
    return err;
}

error_code replica::download_sst_files(const std::string &app_name,
                                       const std::string &cluster_name,
                                       const std::string &provider_name)
{
    FAIL_POINT_INJECT_F("replica_bulk_load_download_sst_files",
                        [](string_view) -> error_code { return ERR_OK; });

    std::string remote_dir =
        get_remote_bulk_load_dir(app_name, cluster_name, get_gpid().get_partition_index());
    std::string local_dir =
        utils::filesystem::path_combine(_dir, bulk_load_constant::BULK_LOAD_LOCAL_ROOT_DIR);

    // create local bulk load dir
    if (!utils::filesystem::directory_exists(_dir)) {
        derror_replica("_dir({}) is not existed", _dir);
        return ERR_FILE_OPERATION_FAILED;
    }
    if (!utils::filesystem::directory_exists(local_dir) &&
        !utils::filesystem::create_directory(local_dir)) {
        derror_replica("create bulk_load_dir({}) failed", local_dir);
        return ERR_FILE_OPERATION_FAILED;
    }

    dist::block_service::block_filesystem *fs =
        _stub->_block_service_manager.get_block_filesystem(provider_name);
    error_code err = ERR_OK;
    task_tracker tracker;

    // download metadata file synchronously
    std::string meta_name = bulk_load_constant::BULK_LOAD_METADATA;
    do_download(remote_dir, local_dir, meta_name, fs, false, err, tracker);
    if (err != ERR_OK) {
        derror_replica("download bulk load metadata file failed, error = {}", err.to_string());
        return err;
    }

    // parse metadata
    std::string local_metadata_file_name = utils::filesystem::path_combine(local_dir, meta_name);
    bulk_load_metadata metadata;
    err = parse_bulk_load_metadata(local_metadata_file_name, metadata);
    if (err != ERR_OK) {
        derror_replica("parse bulk load metadata failed, error = {}", err.to_string());
        return err;
    }
    _bulk_load_context._metadata = metadata;
    _bulk_load_context._file_total_size = metadata.file_total_size;

    // download sst files asynchronously
    for (const auto &f_meta : metadata.files) {
        auto bulk_load_download_task = tasking::enqueue(
            LPC_BACKGROUND_BULK_LOAD, &_tracker, [this, remote_dir, local_dir, f_meta, fs]() {
                error_code ec;
                task_tracker tracker;
                do_download(remote_dir, local_dir, f_meta.name, fs, true, ec, tracker);
                if (ec == ERR_OK && !verify_sst_files(f_meta, local_dir)) {
                    ec = ERR_CORRUPTION;
                }
                if (ec != ERR_OK) {
                    try_decrease_bulk_load_download_count();
                    _bulk_load_context._download_status = ec;
                    derror_replica(
                        "failed to download file({}), error = {}", f_meta.name, ec.to_string());
                    return;
                }
            });
        _bulk_load_context._bulk_load_download_task[f_meta.name] = bulk_load_download_task;
    }
    return err;
}

void replica::do_download(const std::string &remote_dir,
                          const std::string &local_dir,
                          const std::string &file_name,
                          dist::block_service::block_filesystem *fs,
                          bool is_update_progress,
                          error_code &err,
                          task_tracker &tracker)
{
    std::string remote_file_name = utils::filesystem::path_combine(remote_dir, file_name);

    // ThreadPool: THREAD_POOL_FDS_SERVICE
    auto download_file_callback_func = [this,
                                        &err](const dist::block_service::download_response &resp,
                                              dist::block_service::block_file_ptr bf,
                                              const std::string &local_file_name,
                                              bool update_progress) {
        if (resp.err != ERR_OK) {
            if (resp.err == ERR_OBJECT_NOT_FOUND) {
                derror_replica("download file({}) failed, data on bulk load provider is damaged",
                               local_file_name);
                err = ERR_CORRUPTION;
            } else {
                err = resp.err;
            }
            _stub->_counter_bulk_load_recent_download_file_fail_count->increment();
            return;
        }

        if (resp.downloaded_size != bf->get_size()) {
            derror_replica(
                "size not match while download file({}), total_size({}) vs downloaded_size({})",
                bf->file_name().c_str(),
                bf->get_size(),
                resp.downloaded_size);
            err = ERR_CORRUPTION;
            _stub->_counter_bulk_load_recent_download_file_fail_count->increment();
            return;
        }

        std::string current_md5;
        error_code e = utils::filesystem::md5sum(local_file_name, current_md5);
        if (e != ERR_OK) {
            derror_replica("calculate file({}) md5 failed", local_file_name);
            err = e;
            _stub->_counter_bulk_load_recent_download_file_fail_count->increment();
        } else if (current_md5 != bf->get_md5sum()) {
            derror_replica("local file({}) is not same with remote file({}), download failed, md5: "
                           "local({}) VS remote({})",
                           local_file_name.c_str(),
                           bf->file_name().c_str(),
                           current_md5.c_str(),
                           bf->get_md5sum().c_str());
            err = ERR_CORRUPTION;
            _stub->_counter_bulk_load_recent_download_file_fail_count->increment();
        } else {
            uint64_t file_size = bf->get_size();
            if (update_progress) {
                update_download_progress(file_size);
            }
            ddebug_replica("download file({}) succeed, file_size = {}, download_progress = {}%",
                           local_file_name.c_str(),
                           resp.downloaded_size,
                           _bulk_load_context._download_progress.load());

            _stub->_counter_bulk_load_recent_download_file_succ_count->increment();
            _stub->_counter_bulk_load_recent_download_file_size->add(file_size);
            try_set_bulk_load_max_download_size(file_size);
        }

    };

    // ThreadPool: THREAD_POOL_FDS_SERVICE
    auto create_file_cb = [this, &err, &local_dir, &download_file_callback_func, &tracker](
        const dist::block_service::create_file_response &resp,
        const std::string fname,
        bool update_progress) {
        if (resp.err != ERR_OK) {
            derror_replica("create file({}) failed with error({})", fname, resp.err.to_string());
            err = resp.err;
            return;
        }

        dist::block_service::block_file *bf = resp.file_handle.get();
        if (bf->get_md5sum().empty()) {
            derror_replica("file({}) doesn't exist on bulk load provider", bf->file_name());
            err = ERR_CORRUPTION;
            return;
        }

        std::string local_file_name = utils::filesystem::path_combine(local_dir, fname);
        bool download_file = false;
        if (!utils::filesystem::file_exists(local_file_name)) {
            dinfo_replica("local file({}) not exist, download it from remote path({})",
                          local_file_name.c_str(),
                          bf->file_name().c_str());
            download_file = true;
        } else {
            std::string current_md5;
            error_code e = utils::filesystem::md5sum(local_file_name, current_md5);
            if (e != ERR_OK) {
                dwarn_replica("calculate file({}) md5 failed, redownload it", local_file_name);
                if (!utils::filesystem::remove_path(local_file_name)) {
                    derror_replica("failed to remove file({})", local_file_name);
                    err = e;
                    return;
                }
                download_file = true;
            } else if (current_md5 != bf->get_md5sum()) {
                dwarn_replica("local file({}) is not same with remote file({}), md5: local({}) VS "
                              "remote({}), redownload it",
                              local_file_name.c_str(),
                              bf->file_name().c_str(),
                              current_md5.c_str(),
                              bf->get_md5sum().c_str());
                if (!utils::filesystem::remove_path(local_file_name)) {
                    derror_replica("failed to remove file({})", local_file_name);
                    err = e;
                    return;
                }
                download_file = true;
            } else {
                ddebug_replica("local file({}) has been downloaded", local_file_name);
                if (update_progress) {
                    update_download_progress(bf->get_size());
                }
            }
        }

        if (download_file) {
            bf->download(dist::block_service::download_request{local_file_name, 0, -1},
                         TASK_CODE_EXEC_INLINED,
                         std::bind(download_file_callback_func,
                                   std::placeholders::_1,
                                   resp.file_handle,
                                   local_file_name,
                                   update_progress),
                         &tracker);
        }
    };

    fs->create_file(dist::block_service::create_file_request{remote_file_name, false},
                    TASK_CODE_EXEC_INLINED,
                    std::bind(create_file_cb, std::placeholders::_1, file_name, is_update_progress),
                    &tracker);
    tracker.wait_outstanding_tasks();
}

error_code replica::parse_bulk_load_metadata(const std::string &fname, bulk_load_metadata &meta)
{
    if (!utils::filesystem::file_exists(fname)) {
        derror_replica("file({}) doesn't exist", fname);
        return ERR_FILE_OPERATION_FAILED;
    }

    int64_t file_sz = 0;
    if (!utils::filesystem::file_size(fname, file_sz)) {
        derror_replica("get file({}) size failed", fname);
        return ERR_FILE_OPERATION_FAILED;
    }
    std::shared_ptr<char> buf = utils::make_shared_array<char>(file_sz + 1);

    std::ifstream fin(fname, std::ifstream::in);
    if (!fin.is_open()) {
        derror_replica("open file({}) failed", fname);
        return ERR_FILE_OPERATION_FAILED;
    }
    fin.read(buf.get(), file_sz);
    dassert_replica(file_sz == fin.gcount(),
                    "read file({}) failed, file_size = {} but read size = {}",
                    fname.c_str(),
                    file_sz,
                    fin.gcount());
    fin.close();

    buf.get()[fin.gcount()] = '\0';
    blob bb;
    bb.assign(std::move(buf), 0, file_sz);
    if (!json::json_forwarder<bulk_load_metadata>::decode(bb, meta)) {
        derror_replica("file({}) is damaged", fname);
        return ERR_CORRUPTION;
    }
    return ERR_OK;
}

// ThreadPool: THREAD_POOL_FDS_SERVICE
void replica::update_download_progress(uint64_t file_size)
{
    if (_bulk_load_context._file_total_size <= 0) {
        return;
    }

    _bulk_load_context._cur_downloaded_size.fetch_add(file_size);
    // TODO(heyuchen): dinfo
    ddebug_replica("total_size = {}, cur_downloaded_size = {}",
                   _bulk_load_context._file_total_size,
                   _bulk_load_context._cur_downloaded_size.load());

    auto total_size = static_cast<double>(_bulk_load_context._file_total_size);
    auto cur_downloaded_size = static_cast<double>(_bulk_load_context._cur_downloaded_size.load());
    auto cur_progress = static_cast<int32_t>((cur_downloaded_size / total_size) * 100);
    _bulk_load_context._download_progress.store(cur_progress);

    tasking::enqueue(LPC_REPLICATION_COMMON,
                     &_tracker,
                     std::bind(&replica::bulk_load_check_download_finish, this),
                     get_gpid().thread_hash());
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
bool replica::verify_sst_files(const file_meta &f_meta, const std::string &local_dir)
{
    std::string local_file = utils::filesystem::path_combine(local_dir, f_meta.name);
    int64_t file_sz = 0;
    std::string md5;
    if (!utils::filesystem::file_size(local_file, file_sz)) {
        derror_replica("get file({}) size failed", local_file.c_str());
        return false;
    }
    if (utils::filesystem::md5sum(local_file, md5) != ERR_OK) {
        derror_replica("get file({}) md5 failed", local_file.c_str());
        return false;
    }
    if (file_sz != f_meta.size || md5 != f_meta.md5) {
        derror_replica("file({}) is damaged", local_file.c_str());
        return false;
    }
    return true;
}

void replica::try_decrease_bulk_load_download_count()
{
    if (_stub->_bulk_load_recent_downloading_replica_count > 0) {
        _stub->_bulk_load_recent_downloading_replica_count--;
        // TODO(heyuchen): delete this debug log
        ddebug_replica("concurrent: node[{}] recent_bulk_load_downloading_replica_count={}",
                       _stub->_primary_address_str,
                       _stub->_bulk_load_recent_downloading_replica_count);
    }
}

void replica::bulk_load_check_download_finish()
{
    if (_bulk_load_context._download_progress.load() == bulk_load_constant::PROGRESS_FINISHED &&
        get_bulk_load_status() == bulk_load_status::BLS_DOWNLOADING) {
        set_bulk_load_status(bulk_load_status::BLS_DOWNLOADED);
        _bulk_load_context.cleanup_download_task();
        try_decrease_bulk_load_download_count();
    }
}

void replica::bulk_load_start_ingestion()
{
    set_bulk_load_status(bulk_load_status::BLS_INGESTING);
    _stub->_counter_bulk_load_ingestion_count->increment();
    if (status() == partition_status::PS_PRIMARY) {
        _primary_states.is_ingestion_commit = false;
    }
}

void replica::bulk_load_check_ingestion_finish()
{
    if (_app->get_ingestion_status() == ingestion_status::IS_SUCCEED &&
        !_primary_states.is_ingestion_commit) {
        // gurantee secondary commit ingestion request
        mutation_ptr mu = new_mutation(invalid_decree);
        mu->add_client_request(RPC_REPLICATION_WRITE_EMPTY, nullptr);
        init_prepare(mu, false, true);
        _primary_states.is_ingestion_commit = true;
    }
}

void replica::handle_bulk_load_succeed()
{
    // generate checkpoint
    init_checkpoint(true);

    // TODO(heyuchen): ingestion - consider when reset
    _app->set_ingestion_status(ingestion_status::IS_INVALID);

    set_bulk_load_status(bulk_load_status::BLS_SUCCEED);
    _stub->_counter_bulk_load_finish_count->increment();
}

void replica::handle_bulk_load_finish(bulk_load_status::type new_status)
{
    if (_bulk_load_context.is_cleanup()) {
        ddebug_replica("bulk load context has been cleaned up");
        return;
    }

    if (status() == partition_status::PS_PRIMARY) {
        for (const auto &target_address : _primary_states.membership.secondaries) {
            _primary_states.reset_node_bulk_load_states(target_address, new_status);
        }
    }

    ddebug_replica("bulk load finished, old_status={}, new_status={}",
                   enum_to_string(get_bulk_load_status()),
                   enum_to_string(new_status));

    // remove local bulk load dir
    std::string bulk_load_dir =
        utils::filesystem::path_combine(_dir, bulk_load_constant::BULK_LOAD_LOCAL_ROOT_DIR);
    error_code err = remove_local_bulk_load_dir(bulk_load_dir);
    if (err != ERR_OK) {
        tasking::enqueue(LPC_REPLICATION_COMMON,
                         &_tracker,
                         std::bind(&replica::remove_local_bulk_load_dir, this, bulk_load_dir),
                         get_gpid().thread_hash());
    }

    // clean up bulk load states
    clear_bulk_load_states();
}

error_code replica::remove_local_bulk_load_dir(const std::string &bulk_load_dir)
{
    if (!utils::filesystem::directory_exists(bulk_load_dir) ||
        !utils::filesystem::remove_path(bulk_load_dir)) {
        derror_replica("remove bulk_load dir({}) failed", bulk_load_dir);
        return ERR_FILE_OPERATION_FAILED;
    }

    return ERR_OK;
}

void replica::pause_bulk_load()
{
    bulk_load_status::type cur_status = get_bulk_load_status();
    if (cur_status == bulk_load_status::BLS_PAUSED) {
        ddebug_replica("bulk load has been paused");
        return;
    }

    if (cur_status == bulk_load_status::BLS_DOWNLOADING) {
        try_decrease_bulk_load_download_count();
    }

    set_bulk_load_status(bulk_load_status::BLS_PAUSED);
    ddebug_replica("paused bulk load");
}

void replica::clear_bulk_load_states()
{
    if (get_bulk_load_status() == bulk_load_status::BLS_DOWNLOADING) {
        try_decrease_bulk_load_download_count();
    }
    _is_bulk_load_ingestion = false;
    _app->set_ingestion_status(ingestion_status::IS_INVALID);
    _bulk_load_context.cleanup();
}

// TODO(heyuchen): refactor it
replica::bulk_load_report_flag replica::get_report_flag(bulk_load_status::type meta_status,
                                                        bulk_load_status::type local_status)
{
    if (local_status == bulk_load_status::BLS_DOWNLOADING ||
        (local_status == bulk_load_status::BLS_DOWNLOADED &&
         meta_status != bulk_load_status::BLS_INGESTING)) {
        return ReportDownloadProgress;
    }

    if (local_status == bulk_load_status::BLS_INGESTING &&
        meta_status == bulk_load_status::BLS_INGESTING) {
        return ReportIngestionStatus;
    }

    if (((local_status == bulk_load_status::BLS_SUCCEED ||
          local_status == bulk_load_status::BLS_INVALID) &&
         meta_status == bulk_load_status::BLS_SUCCEED) ||
        meta_status == bulk_load_status::BLS_FAILED ||
        meta_status == bulk_load_status::BLS_CANCELED) {
        return ReportCleanupFlag;
    }

    if ((local_status == bulk_load_status::BLS_PAUSING ||
         local_status == bulk_load_status::BLS_PAUSED) &&
        meta_status == bulk_load_status::BLS_PAUSING) {
        return ReportIsPaused;
    }

    return ReportNothing;
}

void replica::report_bulk_load_states_to_meta(bulk_load_status::type remote_status,
                                              bool report_metadata,
                                              bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (report_metadata && _bulk_load_context._metadata.files.size() > 0) {
        response.__set_metadata(_bulk_load_context._metadata);
    }

    bulk_load_report_flag flag = get_report_flag(remote_status, get_bulk_load_status());
    switch (flag) {
    case ReportDownloadProgress:
        report_group_download_progress(response);
        break;
    case ReportIngestionStatus:
        report_group_ingestion_status(response);
        break;
    case ReportCleanupFlag:
        report_group_context_clean_flag(response);
        break;
    case ReportIsPaused:
        report_group_is_paused(response);
        break;
    case ReportNothing:
        break;
    default:
        break;
    }

    response.primary_bulk_load_status = get_bulk_load_status();
}

void replica::report_bulk_load_states_to_primary(bulk_load_status::type remote_status,
                                                 group_bulk_load_response &response)
{
    if (status() != partition_status::PS_SECONDARY) {
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state bulk_load_state;
    auto local_status = get_bulk_load_status();
    auto flag = get_report_flag(remote_status, local_status);
    switch (flag) {
    case ReportDownloadProgress: {
        bulk_load_state.__set_download_progress(_bulk_load_context._download_progress.load());
        bulk_load_state.__set_download_status(_bulk_load_context._download_status);
    } break;
    case ReportIngestionStatus:
        bulk_load_state.__set_ingest_status(_app->get_ingestion_status());
        break;
    case ReportCleanupFlag:
        bulk_load_state.__set_is_cleanuped(_bulk_load_context.is_cleanup());
        break;
    case ReportIsPaused:
        bulk_load_state.__set_is_paused(local_status == bulk_load_status::BLS_PAUSED);
    case ReportNothing:
    default:
        break;
    }
    response.status = local_status;
    response.bulk_load_state = bulk_load_state;
}

void replica::report_group_download_progress(bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("receive request with wrong status {}", enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state p_state;
    p_state.__set_download_progress(_bulk_load_context._download_progress.load());
    p_state.__set_download_status(_bulk_load_context._download_status);
    response.group_bulk_load_state[_primary_states.membership.primary] = p_state;
    ddebug_replica("primary = {}, download progress = {}%, status={}",
                   _primary_states.membership.primary.to_string(),
                   p_state.download_progress,
                   p_state.download_status.to_string());

    int32_t total_progress = p_state.download_progress;
    for (const auto &target_address : _primary_states.membership.secondaries) {
        partition_bulk_load_state s_state =
            _primary_states.secondary_bulk_load_states[target_address];
        int32_t s_progress = s_state.__isset.download_progress ? s_state.download_progress : 0;
        error_code s_status = s_state.__isset.download_status ? s_state.download_status : ERR_OK;
        ddebug_replica("secondary = {}, download progress = {}%, status={}",
                       target_address.to_string(),
                       s_progress,
                       s_status);
        response.group_bulk_load_state[target_address] = s_state;
        total_progress += s_progress;
    }

    total_progress /= _primary_states.membership.max_replica_count;
    ddebug_replica("total download progress = {}%", total_progress);

    response.__set_total_download_progress(total_progress);
}

void replica::report_group_ingestion_status(bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("receive request with wrong status {}", enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state p_state;
    p_state.__set_ingest_status(_app->get_ingestion_status());
    response.group_bulk_load_state[_primary_states.membership.primary] = p_state;
    ddebug_replica("primary = {}, ingestion status = {}",
                   _primary_states.membership.primary.to_string(),
                   p_state.ingest_status);

    bool is_group_ingestion_finish = p_state.ingest_status == ingestion_status::IS_SUCCEED;
    // TODO(heyuchen): remove this log
    ddebug_replica("hyc: is_group_ingestion_finish = {}", is_group_ingestion_finish);
    for (const auto &target_address : _primary_states.membership.secondaries) {
        partition_bulk_load_state s_state =
            _primary_states.secondary_bulk_load_states[target_address];
        ingestion_status::type i_status =
            s_state.__isset.ingest_status ? s_state.ingest_status : ingestion_status::IS_INVALID;
        ddebug_replica("secondary = {}, ingestion status={}", target_address.to_string(), i_status);
        response.group_bulk_load_state[target_address] = s_state;
        is_group_ingestion_finish =
            is_group_ingestion_finish && (i_status == ingestion_status::IS_SUCCEED);
        // TODO(heyuchen): remove this log
        ddebug_replica("hyc: is_group_ingestion_finish = {}", is_group_ingestion_finish);
    }
    response.__set_is_group_ingestion_finished(is_group_ingestion_finish &&
                                               (_primary_states.membership.secondaries.size() + 1 ==
                                                _primary_states.membership.max_replica_count));

    if (is_group_ingestion_finish) {
        // group ingestion finish will recover wirte
        ddebug_replica("finish ingestion, recover write");
        _is_bulk_load_ingestion = false;
        _bulk_load_context._bulk_load_start_time_ns = 0;
    }
}

void replica::report_group_context_clean_flag(bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("receive request with wrong status {}", enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state p_state;
    p_state.__set_is_cleanuped(_bulk_load_context.is_cleanup());
    response.group_bulk_load_state[_primary_states.membership.primary] = p_state;
    ddebug_replica("primary = {}, bulk_load_context cleanup = {}",
                   _primary_states.membership.primary.to_string(),
                   p_state.is_cleanuped);

    bool group_flag = p_state.is_cleanuped;
    for (const auto &target_address : _primary_states.membership.secondaries) {
        partition_bulk_load_state s_state =
            _primary_states.secondary_bulk_load_states[target_address];
        bool is_cleanup = s_state.__isset.is_cleanuped ? s_state.is_cleanuped : false;
        ddebug_replica("secondary = {}, bulk_load_context cleanup = {}",
                       target_address.to_string(),
                       is_cleanup);
        response.group_bulk_load_state[target_address] = s_state;
        group_flag = group_flag && is_cleanup;
    }
    response.__set_is_group_bulk_load_context_cleaned(
        group_flag && (_primary_states.membership.secondaries.size() + 1 ==
                       _primary_states.membership.max_replica_count));
}

void replica::report_group_is_paused(bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("receive request with wrong status {}", enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    partition_bulk_load_state p_state;
    p_state.__set_is_paused(get_bulk_load_status() == bulk_load_status::BLS_PAUSED);
    response.group_bulk_load_state[_primary_states.membership.primary] = p_state;
    ddebug_replica("primary = {}, bulk_load is_paused = {}",
                   _primary_states.membership.primary.to_string(),
                   p_state.is_paused);

    bool group_is_paused = p_state.is_paused;
    for (const auto &target_address : _primary_states.membership.secondaries) {
        partition_bulk_load_state s_state =
            _primary_states.secondary_bulk_load_states[target_address];
        bool is_paused = s_state.__isset.is_paused ? s_state.is_paused : false;
        ddebug_replica(
            "secondary = {}, bulk_load is_paused = {}", target_address.to_string(), is_paused);
        response.group_bulk_load_state[target_address] = s_state;
        group_is_paused = group_is_paused && is_paused;
    }
    response.__set_is_group_bulk_load_paused(group_is_paused &&
                                             (_primary_states.membership.secondaries.size() + 1 ==
                                              _primary_states.membership.max_replica_count));
}

} // namespace replication
} // namespace dsn
