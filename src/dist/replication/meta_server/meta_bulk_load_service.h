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

#pragma once

#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/server_state.h"

namespace dsn {
namespace replication {

// TODO(heyuchen): initialize it
struct bulk_load_progress
{
    // std::map<gpid, int32_t> partition_progress;
    std::map<gpid, dsn::task_ptr> bulk_load_requests;
    std::map<app_id, uint32_t> unfinished_partitions_per_app;
};

class bulk_load_service
{
public:
    explicit bulk_load_service(meta_service *meta_svc);

    // client -> meta server to start bulk load
    void on_start_bulk_load(start_bulk_load_rpc rpc);

    void update_blstatus_downloading_on_remote_storage(std::shared_ptr<app_state> app,
                                                       start_bulk_load_rpc rpc);
    void create_bulk_load_folder_on_remote_storage(std::shared_ptr<app_state> app,
                                                   start_bulk_load_rpc rpc);
    void update_partition_blstatus_downloading(std::shared_ptr<app_state> app,
                                               uint32_t pidx,
                                               const std::string &bulk_load_path,
                                               start_bulk_load_rpc rpc);

    void partition_bulk_load(gpid pid, const std::string &remote_provider_name);

    void on_partition_bulk_load_reply(dsn::error_code err,
                                      bulk_load_response &&response,
                                      gpid pid,
                                      const dsn::rpc_address &primary_addr);

    // app bulk load path is {app_path}/bulk_load
    std::string get_app_bulk_load_path(std::shared_ptr<app_state> app) const
    {
        std::stringstream oss;
        oss << _state->get_app_path(*app) << "/"
            << "bulk_load";
        return oss.str();
    }

    // partition bulk load path is {app_path}/bulk_load/partition_index/
    std::string get_partition_bulk_load_path(const std::string &app_bulk_load_path,
                                             int partition_id) const
    {
        std::stringstream oss;
        oss << app_bulk_load_path << "/" << partition_id;
        return oss.str();
    }

private:
    meta_service *_meta_svc;
    server_state *_state;

    bulk_load_progress _progress;

    // TODO(heyuchen): lock difference???
    // app lock
    zrwlock_nr &app_lock() const { return _state->_lock; }
    // bulk load lock
    zrwlock_nr _lock;
};

} // namespace replication
} // namespace dsn
