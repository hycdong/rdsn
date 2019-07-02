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
        ddebug_f("{} receive request with wrong status {}", name(), enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    // TODO(heyuchen): 3. set response field

    ddebug_f("{} receive bulk load request, remote provider:{}, app bulk load status:{}, partition "
             "bulk load status: remote {} VS local {}",
             name(),
             request.remote_provider_name,
             enum_to_string(request.app_bl_status),
             enum_to_string(request.partition_bl_info.status),
             enum_to_string(_bulk_load_context.get_status()));

    if (_bulk_load_context.get_status() == bulk_load_status::BLS_INVALID &&
        request.partition_bl_info.status == bulk_load_status::BLS_DOWNLOADING) {
        ddebug_f("{} try to download sst files", name());
        _bulk_load_context.set_status(request.partition_bl_info.status);
        response.err = download_sst_files(request);
    }

    if ((_bulk_load_context.get_status() == bulk_load_status::BLS_DOWNLOADING ||
         _bulk_load_context.get_status() == bulk_load_status::BLS_DOWNLOADED)) {
        if (status() == partition_status::PS_PRIMARY) {
            send_download_request_to_secondaries(request);
        } else {
            // TODO(heyuchen): 1. report download
        }
    }
}

void replica::send_download_request_to_secondaries(const bulk_load_request &request)
{
    for (const auto &target_address : _primary_states.membership.secondaries) {
        rpc::call_one_way_typed(target_address, RPC_BULK_LOAD, request, get_gpid().thread_hash());
    }
}

dsn::error_code replica::download_sst_files(const bulk_load_request &request)
{
    std::string remote_dir =
        get_bulk_load_remote_dir(request.app_name, request.pid.get_partition_index());

    std::string bulk_load_dir = utils::filesystem::path_combine(_dir, ".bulk_load");
    dsn::error_code err = create_local_bulk_load_dir(bulk_load_dir);
    if (err != ERR_OK) {
        derror_f("{} failed to download sst files coz create local dir failed", name());
        return err;
    }
    return do_download_sst_files(request.remote_provider_name, remote_dir, bulk_load_dir);
}

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

std::string replica::get_bulk_load_remote_dir(const std::string &app_name, uint32_t pidx)
{
    // TODO(heyuchen): change dir path
    std::ostringstream oss;
    oss << "bulk_load_test/cluster/" << app_name << "/" << pidx;
    return oss.str();
}

dsn::error_code replica::do_download_sst_files(const std::string &remote_provider,
                                               const std::string &remote_file_dir,
                                               const std::string &local_file_dir)
{
    dsn::dist::block_service::block_filesystem *fs =
        _stub->_block_service_manager.get_block_filesystem(remote_provider);
    dsn::task_tracker tracker;
    dsn::error_code err;

    auto download_file_callback_func = [this, &err](
        const dsn::dist::block_service::download_response &resp,
        dsn::dist::block_service::block_file_ptr bf,
        const std::string &local_file) {
        if (resp.err != dsn::ERR_OK) {
            if (resp.err == ERR_OBJECT_NOT_FOUND) {
                derror_f("{}: data on bulk load provider is damaged", name());
            }
            err = resp.err;
        } else {
            ddebug_f("{} start to download file {}", name(), local_file.c_str());
            // TODO: find a better way to replace dassert
            if (resp.downloaded_size != bf->get_size()) {
                derror_f("{}: size not match when download file({}), total({}) vs downloaded({})",
                         name(),
                         bf->file_name().c_str(),
                         bf->get_size(),
                         resp.downloaded_size);
            }

            std::string current_md5;
            dsn::error_code e = utils::filesystem::md5sum(local_file, current_md5);
            if (e != dsn::ERR_OK) {
                derror("%s: calc md5sum(%s) failed", name(), local_file.c_str());
                err = e;
            } else if (current_md5 != bf->get_md5sum()) {
                ddebug(
                    "%s: local file(%s) not same with remote file(%s), download failed, %s VS %s",
                    name(),
                    local_file.c_str(),
                    bf->file_name().c_str(),
                    current_md5.c_str(),
                    bf->get_md5sum().c_str());
                err = ERR_FILE_OPERATION_FAILED;
            } else {
                ddebug("%s: download file(%s) succeed, size(%" PRId64 ")",
                       name(),
                       local_file.c_str(),
                       resp.downloaded_size);
            }
        }
    };

    auto create_file_cb = [this, &err, &local_file_dir, &download_file_callback_func, &tracker](
        const dsn::dist::block_service::create_file_response &resp, const std::string fname) {
        if (resp.err != dsn::ERR_OK) {
            derror_f("{}: create file({}) failed with err({})",
                     name(),
                     fname.c_str(),
                     resp.err.to_string());
            err = resp.err;
        } else {
            ddebug_f("{}: create file({}) succeed", name(), fname.c_str());

            dsn::dist::block_service::block_file *bf = resp.file_handle.get();
            if (bf->get_md5sum().empty()) {
                derror_f(
                    "{}: file({}) doesn't on bulk load provider", name(), bf->file_name().c_str());
                err = ERR_CORRUPTION;
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
                    derror_f("{}: calc md5sum({}) failed", name(), local_file.c_str());
                    // here we just retry and download it
                    if (!utils::filesystem::remove_path(local_file)) {
                        err = e;
                        return;
                    }
                    download_file = true;
                } else if (current_md5 != bf->get_md5sum()) {
                    ddebug_f("{}: local file({}) not same with remote file({}), redownload, "
                             "{} VS {}",
                             name(),
                             local_file.c_str(),
                             bf->file_name().c_str(),
                             current_md5.c_str(),
                             bf->get_md5sum().c_str());
                    download_file = true;
                } else {
                    ddebug_f("{}: local file({}) has been downloaded, just ignore",
                             name(),
                             local_file.c_str());
                }
            }

            if (download_file) {
                bf->download(dsn::dist::block_service::download_request{local_file, 0, -1},
                             TASK_CODE_EXEC_INLINED,
                             std::bind(download_file_callback_func,
                                       std::placeholders::_1,
                                       resp.file_handle,
                                       local_file),
                             &tracker);
            }
        }
    };

    std::string remote_file_name = "bulk_load_metadata";
    std::string remote_whole_file_name =
        utils::filesystem::path_combine(remote_file_dir, remote_file_name);

    // TODO(heyuchen): check TASK_CODE_EXEC_INLINED usage
    fs->create_file(dsn::dist::block_service::create_file_request{remote_whole_file_name, false},
                    TASK_CODE_EXEC_INLINED,
                    std::bind(create_file_cb, std::placeholders::_1, remote_file_name),
                    &tracker);
    tracker.wait_outstanding_tasks();

    std::string local_whole_file_name =
        utils::filesystem::path_combine(local_file_dir, remote_file_name);
    bulk_load_metadata metadata;
    if (!_bulk_load_context.read_bulk_load_metadata(local_whole_file_name, metadata)) {
        derror_f("{}: parse bulk load metadata failed", name());
        return ERR_FILE_OPERATION_FAILED;
    }

    _bulk_load_context._file_total_size = metadata.file_total_size;
    for (const auto &f_meta : metadata.files) {
        std::string rname = utils::filesystem::path_combine(remote_file_dir, f_meta.name);
        fs->create_file(dsn::dist::block_service::create_file_request{rname, false},
                        TASK_CODE_EXEC_INLINED,
                        std::bind(create_file_cb, std::placeholders::_1, f_meta.name),
                        &tracker);
    }
    tracker.wait_outstanding_tasks();

    // TODO(heyuchen): 0. validate file with md5, size

    // TODO(heyuchen): 2. download files process

    return err;
}

} // namespace replication
} // namespace dsn
