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

void replica::on_bulk_load(const bulk_load_request &request, bulk_load_response &response)
{
    _checker.only_one_thread_access();

    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        ddebug_f("{} receive request with wrong status {}", name(), enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    ddebug_f("{} receive bulk load request", name());

    // TODO(heyuchen): change remote to temp folder, local to actual dir
    std::ostringstream oss;
    oss << "bulk_load_test/cluster/temp/" << request.pid.get_partition_index();
    std::string remote_dir = oss.str();

    std::ostringstream os;
    os << _dir << "/bulk_load";
    std::string bulk_load_dir = os.str();
    if (!utils::filesystem::directory_exists(bulk_load_dir) &&
        !utils::filesystem::create_directory(bulk_load_dir)) {
        derror_f("{} failed to create directory {}", name(), bulk_load_dir);
        response.err = ERR_FILE_OPERATION_FAILED;
        return;
    }
    response.err = download_sst_files(request.remote_provider_name, remote_dir, bulk_load_dir);

    // TODO(heyuchen): 2. send group check to secondaries
    if (status() == partition_status::PS_PRIMARY) {
        ddebug_f("{} send bulk load request to secondary", name());
    }
}

dsn::error_code replica::download_sst_files(const std::string &remote_provider,
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
    std::string whole_file_name =
        utils::filesystem::path_combine(remote_file_dir, remote_file_name);

    // TODO(heyuchen): check TASK_CODE_EXEC_INLINED usage
    fs->create_file(dsn::dist::block_service::create_file_request{whole_file_name, false},
                    TASK_CODE_EXEC_INLINED,
                    std::bind(create_file_cb, std::placeholders::_1, remote_file_name),
                    &tracker);
    tracker.wait_outstanding_tasks();

    // TODO(heyuchen): 1. download sst files, not only metadata

    return err;
}

} // namespace replication
} // namespace dsn
