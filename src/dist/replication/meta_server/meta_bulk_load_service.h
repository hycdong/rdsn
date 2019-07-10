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

struct app_bulk_load_info
{
    uint32_t app_id;
    std::string cluster_name;
    std::string file_provider_type;
    // TODO(heyuchen): consider add bulk load status
    // bulk_load_status::type status;
    DEFINE_JSON_SERIALIZATION(app_id, cluster_name, file_provider_type)
};

// struct partition_bulk_load_info
//{
//    bulk_load_status::type status;
//    DEFINE_JSON_SERIALIZATION(status)
//};

struct bulk_load_info
{
    std::string app_name;
    std::string remote_root;
    app_bulk_load_info info;
};

// TODO(heyuchen): initialize it
struct bulk_load_context
{
    std::map<app_id, uint32_t> apps_in_progress_count;
    std::map<gpid, dsn::task_ptr> partitions_request;
    std::map<gpid, partition_bulk_load_info> partitions_info;
    std::map<gpid, int32_t> partitions_download_progress;
};

class bulk_load_service
{
public:
    explicit bulk_load_service(meta_service *meta_svc);

    // client -> meta server to start bulk load
    void on_start_bulk_load(start_bulk_load_rpc rpc);
    // client -> meta server to query bulk load status
    void on_query_bulk_load_status(query_bulk_load_rpc rpc);

    void update_app_bulk_load_status_with_rpc(std::shared_ptr<app_state> app,
                                              bulk_load_status::type new_status,
                                              start_bulk_load_rpc rpc);

    void create_app_bulk_load_info_with_rpc(std::shared_ptr<app_state> app,
                                            start_bulk_load_rpc rpc);

    void create_partition_bulk_load_info_with_rpc(const std::string &app_name,
                                                  gpid pid,
                                                  uint32_t partition_count,
                                                  const std::string &bulk_load_path,
                                                  start_bulk_load_rpc rpc);

    void partition_bulk_load(gpid pid);

    void on_partition_bulk_load_reply(dsn::error_code err,
                                      bulk_load_response &&response,
                                      gpid pid,
                                      const dsn::rpc_address &primary_addr);

    void update_partition_bulk_load_status(std::shared_ptr<app_state> app,
                                           dsn::gpid pid,
                                           std::string &path,
                                           bulk_load_status::type status);

    void update_app_bulk_load_status(std::shared_ptr<app_state> app, bulk_load_status::type status);

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

    // app_id -> bulk_load_context
    std::map<uint32_t, bulk_load_info> _apps_bulk_load_info;

    bulk_load_context _bulk_load_states;

    // TODO(heyuchen): lock difference???
    // app lock
    zrwlock_nr &app_lock() const { return _state->_lock; }
    // _bulk_load_states lock
    zrwlock_nr _lock;
};

} // namespace replication
} // namespace dsn
