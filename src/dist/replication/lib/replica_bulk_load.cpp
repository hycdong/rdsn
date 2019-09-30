// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <fstream>
#include <dsn/dist/block_service.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/filesystem.h>

#include "replica.h"
#include "replica_stub.h"

namespace dsn {
namespace replication {

void replica::on_bulk_load(const bulk_load_request &request, bulk_load_response &response)
{
    _checker.only_one_thread_access();

    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("receive bulk load request with wrong status {}", enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    response.err = ERR_OK;
    response.pid = request.pid;
    response.app_name = request.app_name;

    ddebug_replica(
        "receive bulk load request, remote provider = {}, cluster_name = {}, app_name = {}, "
        "app_bulk_load_status = {}, meta partition_bulk_load_status = {}, local bulk_load_status = "
        "{}",
        request.remote_provider_name,
        request.cluster_name,
        request.app_name,
        enum_to_string(request.app_bulk_load_status),
        enum_to_string(request.partition_bulk_load_status),
        enum_to_string(_bulk_load_context.get_status()));

    broadcast_group_bulk_load(request);

    if (_bulk_load_context.get_status() == bulk_load_status::BLS_INVALID &&
        request.partition_bulk_load_status == bulk_load_status::BLS_DOWNLOADING) {

        // update bulk load status
        _bulk_load_context.set_status(bulk_load_status::BLS_DOWNLOADING);

        // start download
        ddebug_replica("start to download sst files");
        _bld_progress.pid = get_gpid();
        response.err = download_sst_files(
            request.app_name, request.cluster_name, request.remote_provider_name);
        if (response.err != ERR_OK) {
            handle_bulk_load_error();
        }
    }

    if (_bulk_load_context.get_status() == bulk_load_status::BLS_DOWNLOADING ||
        _bulk_load_context.get_status() == bulk_load_status::BLS_DOWNLOADED) {
        // primary set download status in response
        update_group_download_progress(response);
    }

    if (request.partition_bulk_load_status == bulk_load_status::BLS_FINISH) {
        if (_bulk_load_context.get_status() == bulk_load_status::BLS_DOWNLOADED) {
            // primary and secondary update bulk load status to finish
            handle_bulk_load_succeed();
        } else {
            // handle cleanup
            // set clean flags
            cleanup_bulk_load_context(bulk_load_status::BLS_FINISH);
            update_group_context_clean_flag(response);
        }
    }

    if (request.app_bulk_load_status == bulk_load_status::BLS_FAILED &&
        request.partition_bulk_load_status == bulk_load_status::BLS_FAILED) {
        // handle cleanup
        // set clean flags
        handle_bulk_load_error();
        update_group_context_clean_flag(response);
    }

    response.primary_bulk_load_status = _bulk_load_context.get_status();
}

// TODO(heyuchen): refactor group_bulk_load functions

void replica::broadcast_group_bulk_load(const bulk_load_request &meta_req)
{
    if (!_primary_states.learners.empty()) {
        // replica has learners, do not send group_bulk_load request, because response status is
        // useless
        dwarn_replica("has learners, skip broadcast group bulk load request");
        return;
    }

    if (_primary_states.group_bulk_load_pending_replies.size() > 0) {
        dwarn_replica(
            "{} group bulk_load replies are still pending when doing next round, cancel it first",
            static_cast<int>(_primary_states.group_bulk_load_pending_replies.size()));

        for (auto it = _primary_states.group_bulk_load_pending_replies.begin();
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

        std::shared_ptr<group_bulk_load_request> request(new group_bulk_load_request);
        request->app = _app_info;
        request->target_address = addr;
        _primary_states.get_replica_config(partition_status::PS_SECONDARY, request->config);
        request->meta_app_bulk_load_status = meta_req.app_bulk_load_status;
        request->meta_partition_bulk_load_status = meta_req.partition_bulk_load_status;
        if (_bulk_load_context.get_status() == bulk_load_status::BLS_DOWNLOADING ||
            bulk_load_status::BLS_DOWNLOADED) {
            request->__isset.cluster_name = true;
            request->cluster_name = meta_req.cluster_name;
            request->__isset.provider_name = true;
            request->provider_name = meta_req.remote_provider_name;
        }

        ddebug_replica("send group_bulk_load_request to {}", addr.to_string());

        dsn::task_ptr callback_task =
            rpc::call(addr,
                      RPC_GROUP_BULK_LOAD,
                      *request,
                      &_tracker,
                      [=](error_code err, group_bulk_load_response &&resp) {
                          auto response =
                              std::make_shared<group_bulk_load_response>(std::move(resp));
                          on_group_bulk_load_reply(err, request, response);
                      },
                      std::chrono::milliseconds(0),
                      get_gpid().thread_hash());
        _primary_states.group_bulk_load_pending_replies[addr] = callback_task;
    }
}

void replica::on_group_bulk_load(const group_bulk_load_request &request,
                                 /*out*/ group_bulk_load_response &response)
{
    _checker.only_one_thread_access();

    ddebug_replica(
        "process group bulk load request, primary = {}, ballot = {}, meta app "
        "bulk_load_status = {}, meta partition bulk_load_status= {}, local bulk_load_status = {}",
        request.config.primary.to_string(),
        request.config.ballot,
        enum_to_string(request.meta_app_bulk_load_status),
        enum_to_string(request.meta_partition_bulk_load_status),
        enum_to_string(_bulk_load_context.get_status()));

    if (request.config.ballot < get_ballot()) {
        response.err = ERR_VERSION_OUTDATED;
        dwarn_replica(
            "receive outdated group_bulk_load request, request ballot = {} VS loca ballot = {}",
            request.config.ballot,
            get_ballot());
        return;
    } else if (request.config.ballot > get_ballot()) {
        response.err = ERR_INVALID_STATE;
        dwarn_replica("receive group_bulk_load request, local ballot is outdated, request "
                      "ballot = {} VS loca ballot = {}",
                      request.config.ballot,
                      get_ballot());
        return;
    } else if (status() != request.config.status) {
        response.err = ERR_INVALID_STATE;
        dwarn_replica("status changed, status should be {}, but {}",
                      enum_to_string(request.config.status),
                      enum_to_string(status()));
        return;
    }

    response.err = ERR_OK;
    response.pid = get_gpid();
    response.target_address = _stub->_primary_address;

    // TODO(heyuchen):
    // do bulk load things
    if (_bulk_load_context.get_status() == bulk_load_status::BLS_INVALID &&
        request.meta_partition_bulk_load_status == bulk_load_status::BLS_DOWNLOADING) {

        // update bulk load status
        _bulk_load_context.set_status(bulk_load_status::BLS_DOWNLOADING);

        // start download
        ddebug_replica("try to download sst files");
        _bld_progress.pid = get_gpid();
        response.err =
            download_sst_files(request.app.app_name, request.cluster_name, request.provider_name);

        // primary report to meta
        if (response.err == ERR_OK) {
            response.__isset.download_progress = true;
            response.download_progress = _bld_progress;
        } else {
            handle_bulk_load_error();
        }
    }

    if (_bulk_load_context.get_status() == bulk_load_status::BLS_DOWNLOADING ||
        _bulk_load_context.get_status() == bulk_load_status::BLS_DOWNLOADED) {
        // secondary report to primary
        response.__isset.download_progress = true;
        response.download_progress = _bld_progress;
    }

    if (request.meta_partition_bulk_load_status == bulk_load_status::BLS_FINISH) {
        if (_bulk_load_context.get_status() == bulk_load_status::BLS_DOWNLOADED) {
            // primary and secondary update bulk load status to finish
            handle_bulk_load_succeed();
        } else {
            // handle cleanup
            // set clean flags
            cleanup_bulk_load_context(bulk_load_status::BLS_FINISH);
            response.__isset.is_bulk_load_context_cleaned = true;
            response.is_bulk_load_context_cleaned = _bulk_load_context.is_cleanup();
        }
    }

    if (request.meta_app_bulk_load_status == bulk_load_status::BLS_FAILED &&
        request.meta_partition_bulk_load_status == bulk_load_status::BLS_FAILED) {
        // handle cleanup
        // set clean flags
        handle_bulk_load_error();
        response.__isset.is_bulk_load_context_cleaned = true;
        response.is_bulk_load_context_cleaned = _bulk_load_context.is_cleanup();
    }

    response.status = _bulk_load_context.get_status();
}

void replica::on_group_bulk_load_reply(error_code err,
                                       const std::shared_ptr<group_bulk_load_request> &req,
                                       const std::shared_ptr<group_bulk_load_response> &resp)
{
    _checker.only_one_thread_access();

    if (partition_status::PS_PRIMARY != status() || req->config.ballot < get_ballot()) {
        return;
    }

    _primary_states.group_bulk_load_pending_replies.erase(req->target_address);

    if (err != ERR_OK) {
        dwarn_replica("get group_bulk_load_reply failed, error = {}", err);
        return;
    }

    if (resp->err != ERR_OK) {
        derror_replica("on_group_bulk_load failed, error = {}", resp->err);
        if (resp->__isset.download_progress) {
            _primary_states.group_download_progress[req->target_address] = resp->download_progress;
        }
        if (resp->__isset.is_bulk_load_context_cleaned) {
            _primary_states.group_bulk_load_context_flag[req->target_address] = false;
        }
    } else {
        if (resp->__isset.download_progress) {
            _primary_states.group_download_progress[req->target_address] = resp->download_progress;
        }
        if (resp->__isset.is_bulk_load_context_cleaned) {
            _primary_states.group_bulk_load_context_flag[req->target_address] =
                resp->is_bulk_load_context_cleaned;
        }
    }
}

// return value:
// - ERR_FILE_OPERATION_FAILED: local file system errors
// - ERR_FS_INTERNAL - remote fs error
// - ERR_CORRUPTION: file not exist or damaged
//                   verify failed, fize not match, md5 not match, meta file not exist or damaged
dsn::error_code replica::download_sst_files(const std::string &app_name,
                                            const std::string &cluster_name,
                                            const std::string &provider_name)
{
    std::string remote_dir =
        get_bulk_load_remote_dir(app_name, cluster_name, get_gpid().get_partition_index());

    // TODO(heyuchen): move '.bulk_load' to common const var
    std::string local_dir = utils::filesystem::path_combine(_dir, ".bulk_load");
    dsn::error_code err = create_local_bulk_load_dir(local_dir);
    if (err != ERR_OK) {
        derror_replica("failed to download sst files because create local dir failed");
        return err;
    }
    return do_download_sst_files(provider_name, remote_dir, local_dir);
}

std::string replica::get_bulk_load_remote_dir(const std::string &app_name,
                                              const std::string &cluster_name,
                                              uint32_t pidx)
{
    // TODO(heyuchen): change "bulk_load_test" from value in config
    std::ostringstream oss;
    oss << "bulk_load_test/" << cluster_name << "/" << app_name << "/" << pidx;
    return oss.str();
}

// - ERR_FILE_OPERATION_FAILED: create folder failed
dsn::error_code replica::create_local_bulk_load_dir(const std::string &bulk_load_dir)
{
    if (!utils::filesystem::directory_exists(_dir) && !utils::filesystem::create_directory(_dir)) {
        derror_replica("_dir({}) is not existed and create bulk load directory failed", _dir);
        return ERR_FILE_OPERATION_FAILED;
    }

    if (!utils::filesystem::directory_exists(bulk_load_dir) &&
        !utils::filesystem::create_directory(bulk_load_dir)) {
        derror_replica("create bulk_load_dir({}) failed", bulk_load_dir);
        return ERR_FILE_OPERATION_FAILED;
    }

    return ERR_OK;
}

// download errors
// parse metadata errors
// verify md5, size failed: ERR_CORRUPTION
dsn::error_code replica::do_download_sst_files(const std::string &remote_provider,
                                               const std::string &remote_file_dir,
                                               const std::string &local_file_dir)
{
    dsn::dist::block_service::block_filesystem *fs =
        _stub->_block_service_manager.get_block_filesystem(remote_provider);

    dsn::error_code err = ERR_OK;
    dsn::task_tracker tracker;
    // TODO(heyuchen): change metadata file name in config or const
    std::string meta_name = "bulk_load_metadata";

    // sync download metadata file
    do_download(remote_file_dir, local_file_dir, meta_name, fs, false, err, tracker);
    if (err != ERR_OK) {
        derror_replica("download bulk load metadata file failed, error = {}", err.to_string());
        return err;
    }

    // parse metadata
    std::string local_metadata_file_name =
        utils::filesystem::path_combine(local_file_dir, meta_name);
    bulk_load_metadata metadata;
    err = read_bulk_load_metadata(local_metadata_file_name, metadata);
    if (err != ERR_OK) {
        derror_replica("parse bulk load metadata failed, error = {}", err.to_string());
        return err;
    }
    _bulk_load_context._file_total_size = metadata.file_total_size;

    // async download sst files
    for (const auto &f_meta : metadata.files) {
        auto bulk_load_download_task = tasking::enqueue(
            LPC_BACKGROUND_BULK_LOAD,
            &_tracker,
            [this, remote_file_dir, local_file_dir, f_meta, fs]() {
                dsn::error_code ec;
                dsn::task_tracker tracker;
                do_download(remote_file_dir, local_file_dir, f_meta.name, fs, true, ec, tracker);
                if (ec == ERR_OK) {
                    if (!verify_sst_files(f_meta, local_file_dir)) {
                        derror_replica("verify failed, because sst file({}) is damaged",
                                       f_meta.name);
                        ec = ERR_CORRUPTION;
                    } else {
                        ddebug_replica("sst file({}) is verified", f_meta.name);
                    }
                }
                _bld_progress.status = ec;
                _bulk_load_download_task.erase(f_meta.name);
                if (ec != ERR_OK) {
                    handle_bulk_load_error();
                }
            });
        _bulk_load_download_task[f_meta.name] = bulk_load_download_task;
    }
    return err;
}

// - ERR_FS_INTERNAL - remote fs error
// - ERR_CORRUPTION - file not exist, file size not match, md5 not match
// - ERR_FILE_OPERATION_FAILED: - calculate file md5 error, download fs error(local file error)
void replica::do_download(const std::string &remote_file_dir,
                          const std::string &local_file_dir,
                          const std::string &remote_file_name,
                          dsn::dist::block_service::block_filesystem *fs,
                          bool is_update_progress,
                          dsn::error_code &err,
                          dsn::task_tracker &tracker)
{
    std::string remote_whole_file_name =
        utils::filesystem::path_combine(remote_file_dir, remote_file_name);

    auto download_file_callback_func = [this, &err](
        const dsn::dist::block_service::download_response &resp,
        dsn::dist::block_service::block_file_ptr bf,
        const std::string &local_file,
        bool update_progress) {
        if (resp.err != dsn::ERR_OK) {
            if (resp.err == ERR_OBJECT_NOT_FOUND) {
                derror_replica("download file({}) failed, data on bulk load provider is damaged",
                               local_file);
                err = ERR_CORRUPTION;
            } else {
                err = resp.err;
            }
        } else {
            if (resp.downloaded_size != bf->get_size()) {
                derror_replica(
                    "size not match while download file({}), total_size({}) vs downloaded_size({})",
                    bf->file_name().c_str(),
                    bf->get_size(),
                    resp.downloaded_size);
                err = ERR_CORRUPTION;
                return;
            }

            std::string current_md5;
            dsn::error_code e = utils::filesystem::md5sum(local_file, current_md5);
            if (e != dsn::ERR_OK) {
                derror_replica("calculate file({}) md5 failed", local_file);
                err = e;
            } else if (current_md5 != bf->get_md5sum()) {
                derror_replica(
                    "local file({}) is not same with remote file({}), download failed, md5: "
                    "local({}) VS remote({})",
                    local_file.c_str(),
                    bf->file_name().c_str(),
                    current_md5.c_str(),
                    bf->get_md5sum().c_str());
                err = ERR_CORRUPTION;
            } else {
                if (update_progress) {
                    _bulk_load_context._cur_download_size.fetch_add(bf->get_size());
                    update_download_progress();
                }
                ddebug_replica("download file({}) succeed, file_size = {}, download_progress = {}%",
                               local_file.c_str(),
                               resp.downloaded_size,
                               _bld_progress.progress);
            }
        }
    };

    auto create_file_cb = [this, &err, &local_file_dir, &download_file_callback_func, &tracker](
        const dsn::dist::block_service::create_file_response &resp,
        const std::string fname,
        bool update_progress) {
        if (resp.err != dsn::ERR_OK) {
            derror_replica("create file({}) failed with error({})", fname, resp.err.to_string());
            err = resp.err;
        } else {
            dsn::dist::block_service::block_file *bf = resp.file_handle.get();
            if (bf->get_md5sum().empty()) {
                derror_replica("file({}) doesn't exist on bulk load provider", bf->file_name());
                err = ERR_CORRUPTION;
                _bld_progress.status = err;
                return;
            }

            std::string local_file = utils::filesystem::path_combine(local_file_dir, fname);
            bool download_file = false;
            if (!utils::filesystem::file_exists(local_file)) {
                ddebug_replica("local file({}) not exist, download it from remote path({})",
                               local_file.c_str(),
                               bf->file_name().c_str());
                download_file = true;
            } else {
                std::string current_md5;
                dsn::error_code e = utils::filesystem::md5sum(local_file, current_md5);
                if (e != dsn::ERR_OK) {
                    derror_replica("calculate file({}) md5 failed", local_file);
                    // here we just retry and download it
                    if (!utils::filesystem::remove_path(local_file)) {
                        err = e;
                        return;
                    }
                    download_file = true;
                } else if (current_md5 != bf->get_md5sum()) {
                    ddebug_replica(
                        "local file({}) is not same with remote file({}), md5: local({}) VS "
                        "remote({}), redownload it",
                        local_file.c_str(),
                        bf->file_name().c_str(),
                        current_md5.c_str(),
                        bf->get_md5sum().c_str());
                    download_file = true;
                } else {
                    ddebug_replica("local file({}) has been downloaded", local_file);
                    if (update_progress) {
                        _bulk_load_context._cur_download_size.fetch_add(bf->get_size());
                        update_download_progress();
                    }
                }
            }

            if (download_file) {
                bf->download(dsn::dist::block_service::download_request{local_file, 0, -1},
                             TASK_CODE_EXEC_INLINED,
                             std::bind(download_file_callback_func,
                                       std::placeholders::_1,
                                       resp.file_handle,
                                       local_file,
                                       update_progress),
                             &tracker);
            }
        }
    };

    fs->create_file(
        dsn::dist::block_service::create_file_request{remote_whole_file_name, false},
        TASK_CODE_EXEC_INLINED,
        std::bind(create_file_cb, std::placeholders::_1, remote_file_name, is_update_progress),
        &tracker);
    tracker.wait_outstanding_tasks();
}

// - ERR_FILE_OPERATION_FAILED
//   file not exist, get size failed, read file failed(local file error)
// - ERR_CORRUPTION: parse failed
dsn::error_code replica::read_bulk_load_metadata(const std::string &file_path,
                                                 bulk_load_metadata &meta)
{
    if (!::dsn::utils::filesystem::file_exists(file_path)) {
        derror_replica("file({}) doesn't exist", file_path);
        return ERR_FILE_OPERATION_FAILED;
    }

    int64_t file_sz = 0;
    if (!::dsn::utils::filesystem::file_size(file_path, file_sz)) {
        derror_replica("get file({}) size failed", file_path);
        return ERR_FILE_OPERATION_FAILED;
    }
    std::shared_ptr<char> buf = utils::make_shared_array<char>(file_sz + 1);

    std::ifstream fin(file_path, std::ifstream::in);
    if (!fin.is_open()) {
        derror_replica("open file({}) failed", file_path);
        return ERR_FILE_OPERATION_FAILED;
    }
    fin.read(buf.get(), file_sz);
    dassert_replica(file_sz == fin.gcount(),
                    "read file({}) failed, file_size = {} but read size = {}",
                    file_path.c_str(),
                    file_sz,
                    fin.gcount());
    fin.close();

    buf.get()[fin.gcount()] = '\0';
    blob bb;
    bb.assign(std::move(buf), 0, file_sz);
    if (!::dsn::json::json_forwarder<bulk_load_metadata>::decode(bb, meta)) {
        derror_replica("file({}) is damaged", file_path);
        return ERR_CORRUPTION;
    }
    return ERR_OK;
}

// verify whether the checkpoint directory is damaged base on backup_metadata under the chkpt
bool replica::verify_sst_files(const file_meta &f_meta, const std::string &dir)
{
    std::string local_file = ::dsn::utils::filesystem::path_combine(dir, f_meta.name);
    int64_t file_sz = 0;
    std::string md5;
    if (!::dsn::utils::filesystem::file_size(local_file, file_sz)) {
        derror_replica("get file({}) size failed", local_file.c_str());
        return false;
    }
    if (::dsn::utils::filesystem::md5sum(local_file, md5) != ERR_OK) {
        derror_replica("get file({}) md5 failed", local_file.c_str());
        return false;
    }
    if (file_sz != f_meta.size || md5 != f_meta.md5) {
        derror_replica("file({}) is damaged", local_file.c_str());
        return false;
    }
    return true;
}

// TODO(heyuchen): move to context.cpp
void replica::update_download_progress()
{
    // TODO(heyuchen):delete
    ddebug_replica("total_size = {}, cur_download_size = {}",
                   _bulk_load_context._file_total_size,
                   _bulk_load_context._cur_download_size.load());

    if (_bulk_load_context._file_total_size <= 0) {
        // have not be initialized, just return 0
        return;
    }
    auto total_size = static_cast<double>(_bulk_load_context._file_total_size);
    auto cur_download_size = static_cast<double>(_bulk_load_context._cur_download_size.load());
    _bulk_load_context._download_progress.store(
        static_cast<int32_t>((cur_download_size / total_size) * 100));
    _bld_progress.progress = _bulk_load_context._download_progress.load();
    if (_bulk_load_context._download_progress.load() == 100 &&
        _bulk_load_context.get_status() == bulk_load_status::BLS_DOWNLOADING) {
        // update bulk load status to downloaded
        _bulk_load_context.set_status(bulk_load_status::BLS_DOWNLOADED);
    }
}

void replica::send_download_request_to_secondaries(const bulk_load_request &request)
{
    for (const auto &target_address : _primary_states.membership.secondaries) {
        rpc::call_one_way_typed(target_address, RPC_BULK_LOAD, request, get_gpid().thread_hash());
    }
}

void replica::update_group_download_progress(bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("receive request with wrong status {}", enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    response.__isset.download_progresses = true;
    response.download_progresses[_primary_states.membership.primary] = _bld_progress;
    ddebug_replica("primary = {}, download progress = {}%",
                   _primary_states.membership.primary.to_string(),
                   _bld_progress.progress);

    int32_t total_progress = _bld_progress.progress;
    for (const auto &target_address : _primary_states.membership.secondaries) {
        partition_download_progress sprogress =
            _primary_states.group_download_progress[target_address];
        response.download_progresses[target_address] = sprogress;
        total_progress += sprogress.progress;
        ddebug_replica("secondary = {}, download progress = {}%",
                       target_address.to_string(),
                       sprogress.progress);
    }
    total_progress /= _primary_states.membership.max_replica_count;
    ddebug_replica("total download progress = {}%", total_progress);

    response.__isset.total_download_progress = true;
    response.total_download_progress = total_progress;
}

void replica::cleanup_bulk_load_context(bulk_load_status::type new_status)
{
    if (_bulk_load_context.is_cleanup()) {
        ddebug_replica("bulk load context has been cleaned up");
        return;
    }

    // set bulk load status
    auto old_status = _bulk_load_context.get_status();
    if (new_status == bulk_load_status::BLS_FAILED) {
        _bulk_load_context.set_status(new_status);
        dwarn_replica("bulk load failed, original status = {}", enum_to_string(old_status));
    } else {
        ddebug_replica("bulk load succeed, original status = {}", enum_to_string(old_status));
    }

    if (old_status == bulk_load_status::BLS_DOWNLOADING ||
        old_status == bulk_load_status::BLS_DOWNLOADED) {
        // stop bulk load download tasks
        for (auto iter = _bulk_load_download_task.begin(); iter != _bulk_load_download_task.end();
             iter++) {
            auto download_task = iter->second;
            if (download_task != nullptr) {
                download_task->cancel(false);
            }
            _bulk_load_download_task.erase(iter);
        }
        // reset bulk load download progress
        _bld_progress.progress = 0;
        _bld_progress.status = ERR_OK;
    }

    // remove local bulk load dir
    std::string local_dir = utils::filesystem::path_combine(_dir, ".bulk_load");
    dsn::error_code err = remove_local_bulk_load_dir(local_dir);
    if (err != ERR_OK) {
        tasking::enqueue(LPC_BACKGROUND_BULK_LOAD, &_tracker, [this, local_dir]() {
            remove_local_bulk_load_dir(local_dir);
        });
    } else {
        ddebug_replica("remove bulk load dir({}) succeed", local_dir);
    }

    // clean up bulk_load_context
    _bulk_load_context.cleanup();
}

// void replica::handle_bulk_load_succeed(const bulk_load_request &request)
void replica::handle_bulk_load_succeed()
{
    auto old_status = _bulk_load_context.get_status();
    if (old_status == bulk_load_status::BLS_DOWNLOADING ||
        old_status == bulk_load_status::BLS_DOWNLOADED) {
        // stop bulk load download tasks
        for (auto iter = _bulk_load_download_task.begin(); iter != _bulk_load_download_task.end();
             iter++) {
            auto download_task = iter->second;
            if (download_task != nullptr) {
                download_task->cancel(false);
            }
            _bulk_load_download_task.erase(iter);
        }
        // reset bulk load download progress
        _bld_progress.progress = 0;
        _bld_progress.status = ERR_OK;
    }

    if (status() == partition_status::PS_PRIMARY) {
        // gurantee secondary commit ingestion request
        mutation_ptr mu = new_mutation(invalid_decree);
        mu->add_client_request(RPC_REPLICATION_WRITE_EMPTY, nullptr);
        init_prepare(mu, false);
        // send_download_request_to_secondaries(request);
    }

    _bulk_load_context.set_status(bulk_load_status::BLS_FINISH);
}

void replica::handle_bulk_load_error() { cleanup_bulk_load_context(bulk_load_status::BLS_FAILED); }

// - ERR_FILE_OPERATION_FAILED: remove folder failed
dsn::error_code replica::remove_local_bulk_load_dir(const std::string &bulk_load_dir)
{
    if (!utils::filesystem::directory_exists(bulk_load_dir) ||
        !utils::filesystem::remove_path(bulk_load_dir)) {
        derror_replica("remove bulk_load dir({}) failed", bulk_load_dir);
        return ERR_FILE_OPERATION_FAILED;
    }

    return ERR_OK;
}

void replica::update_group_context_clean_flag(bulk_load_response &response)
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("receive request with wrong status {}", enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    ddebug_replica("primary = {}, bulk_load_context cleanup = {}",
                   _primary_states.membership.primary.to_string(),
                   _bulk_load_context.is_cleanup());

    bool group_flag = _bulk_load_context.is_cleanup();
    for (const auto &target_address : _primary_states.membership.secondaries) {
        bool is_clean_up = _primary_states.group_bulk_load_context_flag[target_address];
        ddebug_replica("secondary = {}, bulk_load_context cleanup = {}",
                       target_address.to_string(),
                       is_clean_up);
        group_flag = group_flag && is_clean_up;
    }

    response.__isset.is_group_bulk_load_context_cleaned = true;
    response.is_group_bulk_load_context_cleaned = group_flag;
}

} // namespace replication
} // namespace dsn
