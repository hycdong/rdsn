/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <fstream>
#include <dsn/dist/block_service.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/filesystem.h>

#include "replica.h"
#include "replica_stub.h"

namespace dsn {
namespace replication {

// TODO(heyuchen): refactor code, move some functions to replication_common & replica_context
void replica::on_bulk_load(const bulk_load_request &request, bulk_load_response &response)
{
    _checker.only_one_thread_access();

    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        dwarn_f("{} receive request with wrong status {}", name(), enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    response.pid = request.pid;
    response.app_name = request.app_name;

    ddebug_f("{}: receive bulk load request, remote provider={}, cluster_name={}, app_name={}, "
             "app_bulk_load_status={}, partition_bulk_load_status: remote={} VS local={}",
             name(),
             request.remote_provider_name,
             request.cluster_name,
             request.app_name,
             enum_to_string(request.app_bl_status),
             enum_to_string(request.partition_bl_info.status),
             enum_to_string(_bulk_load_context.get_status()));

    if (_bulk_load_context.get_status() == bulk_load_status::BLS_INVALID &&
        request.partition_bl_info.status == bulk_load_status::BLS_DOWNLOADING) {
        ddebug_f("{}: try to download sst files", name());
        _bld_progress.pid = get_gpid();
        _bulk_load_context.set_status(request.partition_bl_info.status);
        response.err = download_sst_files(request);
    }

    if ((_bulk_load_context.get_status() == bulk_load_status::BLS_DOWNLOADING ||
         _bulk_load_context.get_status() == bulk_load_status::BLS_DOWNLOADED) &&
        status() == partition_status::PS_PRIMARY && response.err == ERR_OK) {
        response.partition_bl_status = _bulk_load_context.get_status();
        send_download_request_to_secondaries(request);
        update_group_download_progress(response);
    }

    if (response.err != ERR_OK) {
        handle_bulk_load_error();
    }
}

// return value:
// - ERR_FILE_OPERATION_FAILED: local file system errors
// - ERR_FS_INTERNAL - remote fs error
// - ERR_CORRUPTION: file not exist or damaged
//                   verify failed, fize not match, md5 not match, meta file not exist or damaged
dsn::error_code replica::download_sst_files(const bulk_load_request &request)
{
    std::string remote_dir = get_bulk_load_remote_dir(
        request.app_name, request.cluster_name, request.pid.get_partition_index());

    std::string local_dir = utils::filesystem::path_combine(_dir, ".bulk_load");
    dsn::error_code err = create_local_bulk_load_dir(local_dir);
    if (err != ERR_OK) {
        derror_f("{} failed to download sst files coz create local dir failed", name());
        return err;
    }
    return do_download_sst_files(request.remote_provider_name, remote_dir, local_dir);
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
        derror_f("{}: _dir {} not exist and create directory failed", name(), _dir);
        return ERR_FILE_OPERATION_FAILED;
    }

    if (!utils::filesystem::directory_exists(bulk_load_dir) &&
        !utils::filesystem::create_directory(bulk_load_dir)) {
        derror_f(
            "{}: bulk_load_dir {} not exist and create directory failed", name(), bulk_load_dir);
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
        derror_f(
            "{}: download bulk load metadata file failed, error is {}", name(), err.to_string());
        return err;
    }

    // parse metadata
    std::string local_metadata_file_name =
        utils::filesystem::path_combine(local_file_dir, meta_name);
    bulk_load_metadata metadata;
    err = read_bulk_load_metadata(local_metadata_file_name, metadata);
    if (err != ERR_OK) {
        derror_f("{}: parse bulk load metadata failed, error is {}", name(), err.to_string());
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
                        derror_f("{}: sst file({}) is damaged", name(), f_meta.name);
                        ec = ERR_CORRUPTION;
                    } else {
                        ddebug_f("{}: sst file({}) is verified", name(), f_meta.name);
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
                derror_f("{}: download file({}) failed, data on bulk load provider is damaged",
                         name());
                err = ERR_CORRUPTION;
            } else {
                err = resp.err;
            }
        } else {
            if (resp.downloaded_size != bf->get_size()) {
                derror_f("{}: size not match when download file({}), total({}) vs downloaded({})",
                         name(),
                         bf->file_name().c_str(),
                         bf->get_size(),
                         resp.downloaded_size);
                err = ERR_CORRUPTION;
                return;
            }

            std::string current_md5;
            dsn::error_code e = utils::filesystem::md5sum(local_file, current_md5);
            if (e != dsn::ERR_OK) {
                derror_f("{}: calculate file({}) md5 failed", name(), local_file.c_str());
                err = e;
            } else if (current_md5 != bf->get_md5sum()) {
                derror_f("{}: local file({}) not same with remote file({}), download failed, md5: "
                         "local({}) VS remote({})",
                         name(),
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
                ddebug_f("{}: download file({}) succeed, file_size={}, progress={}%",
                         name(),
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
            derror_f("{}: create file({}) failed with err({})",
                     name(),
                     fname.c_str(),
                     resp.err.to_string());
            err = resp.err;
        } else {
            dsn::dist::block_service::block_file *bf = resp.file_handle.get();
            if (bf->get_md5sum().empty()) {
                derror_f(
                    "{}: file({}) doesn't on bulk load provider", name(), bf->file_name().c_str());
                err = ERR_CORRUPTION;
                _bld_progress.status = err;
                return;
            }

            std::string local_file = utils::filesystem::path_combine(local_file_dir, fname);
            bool download_file = false;
            if (!utils::filesystem::file_exists(local_file)) {
                ddebug_f("{}: local file({}) not exist, download it from remote file({})",
                         name(),
                         local_file.c_str(),
                         bf->file_name().c_str());
                download_file = true;
            } else {
                std::string current_md5;
                dsn::error_code e = utils::filesystem::md5sum(local_file, current_md5);
                if (e != dsn::ERR_OK) {
                    derror_f("{}: calculate file({}) md5 failed", name(), local_file.c_str());
                    // here we just retry and download it
                    if (!utils::filesystem::remove_path(local_file)) {
                        err = e;
                        return;
                    }
                    download_file = true;
                } else if (current_md5 != bf->get_md5sum()) {
                    ddebug_f("{}: local file({}) is not same with remote file({}), md5: local{} VS "
                             "remote{}, redownload it",
                             name(),
                             local_file.c_str(),
                             bf->file_name().c_str(),
                             current_md5.c_str(),
                             bf->get_md5sum().c_str());
                    download_file = true;
                } else {
                    ddebug_f("{}: local file({}) has been downloaded", name(), local_file.c_str());
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
        derror_f("file({}) doesn't exist", file_path.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    int64_t file_sz = 0;
    if (!::dsn::utils::filesystem::file_size(file_path, file_sz)) {
        derror_f("get file({}) size failed", file_path.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }
    std::shared_ptr<char> buf = utils::make_shared_array<char>(file_sz + 1);

    std::ifstream fin(file_path, std::ifstream::in);
    if (!fin.is_open()) {
        derror_f("open file({}) failed", file_path.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }
    fin.read(buf.get(), file_sz);
    dassert(file_sz == fin.gcount(),
            "read file(%s) failed, need %" PRId64 ", but read %" PRId64 "",
            file_path.c_str(),
            file_sz,
            fin.gcount());
    fin.close();

    buf.get()[fin.gcount()] = '\0';
    blob bb;
    bb.assign(std::move(buf), 0, file_sz);
    if (!::dsn::json::json_forwarder<bulk_load_metadata>::decode(bb, meta)) {
        derror_f("file({}) is damaged", file_path.c_str());
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
        derror_f("{}: get file({}) size failed", name(), local_file.c_str());
        return false;
    }
    if (::dsn::utils::filesystem::md5sum(local_file, md5) != ERR_OK) {
        derror_f("{}: get file({}) md5 failed", name(), local_file.c_str());
        return false;
    }
    if (file_sz != f_meta.size || md5 != f_meta.md5) {
        derror_f("{}: file({}) is damaged", name(), local_file.c_str());
        return false;
    }
    // return remove_useless_file_under_chkpt(chkpt_dir, backup_metadata);
    return true;
}

// TODO(heyuchen): move to context.cpp
void replica::update_download_progress()
{
    if (_bulk_load_context._file_total_size <= 0) {
        // have not be initialized, just return 0
        return;
    }
    auto total_size = static_cast<double>(_bulk_load_context._file_total_size);
    auto cur_download_size = static_cast<double>(_bulk_load_context._cur_download_size.load());
    _bulk_load_context._download_progress.store(
        static_cast<int32_t>((cur_download_size / total_size) * 100));
    _bld_progress.progress = _bulk_load_context._download_progress.load();
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
        dwarn_f("{} receive request with wrong status {}", name(), enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    response.__isset.download_progresses = true;
    response.download_progresses[_primary_states.membership.primary] = _bld_progress;
    ddebug_f("{}: pid({}.{}) primary={}, download progress={}%",
             name(),
             response.pid.get_app_id(),
             response.pid.get_partition_index(),
             _primary_states.membership.primary.to_string(),
             _bld_progress.progress);

    int32_t total_progress = _bld_progress.progress;
    for (const auto &target_address : _primary_states.membership.secondaries) {
        partition_download_progress sprogress =
            _primary_states.group_download_progress[target_address];
        response.download_progresses[target_address] = sprogress;
        total_progress += sprogress.progress;
        ddebug_f("{}: pid({}.{}) secondary={}, download progress={}%",
                 name(),
                 response.pid.get_app_id(),
                 response.pid.get_partition_index(),
                 target_address.to_string(),
                 sprogress.progress);
    }
    total_progress /= (_primary_states.membership.secondaries.size() + 1);
    ddebug_f("{}: pid({}.{}) total download progress={}%",
             name(),
             response.pid.get_app_id(),
             response.pid.get_partition_index(),
             total_progress);

    response.__isset.total_download_progress = true;
    response.total_download_progress = total_progress;
}

void replica::handle_bulk_load_error()
{
    // set bulk load status to failure
    auto old_status = _bulk_load_context.get_status();
    auto new_status = bulk_load_status::BLS_FAILED;
    _bulk_load_context.set_status(new_status);
    dwarn_f("{}: bulk load failed, old status={}", name(), enum_to_string(old_status));

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
        // TODO(heyuchen): delete this debug log
        ddebug_f(
            "{}: _bulk_load_download_task is_empty({})", name(), _bulk_load_download_task.empty());

        // reset bulk load download progress
        _bld_progress.progress = 0;
    }

    // remove local bulk load dir
    std::string local_dir = utils::filesystem::path_combine(_dir, ".bulk_load");
    dsn::error_code err = remove_local_bulk_load_dir(local_dir);
    if (err != ERR_OK) {
        tasking::enqueue(LPC_BACKGROUND_BULK_LOAD, &_tracker, [this, local_dir]() {
            remove_local_bulk_load_dir(local_dir);
        });
    } else {
        ddebug_f("{} remove dir({}) succeed", name(), local_dir);
    }
}

// - ERR_FILE_OPERATION_FAILED: remove folder failed
dsn::error_code replica::remove_local_bulk_load_dir(const std::string &bulk_load_dir)
{
    if (!utils::filesystem::directory_exists(bulk_load_dir) ||
        !utils::filesystem::remove_path(bulk_load_dir)) {
        derror_f(
            "{}: bulk_load_dir {} not exist and remove directory failed", name(), bulk_load_dir);
        return ERR_FILE_OPERATION_FAILED;
    }

    return ERR_OK;
}

} // namespace replication
} // namespace dsn
