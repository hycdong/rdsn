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
    uint32_t partition_count;
    std::string app_name;
    std::string cluster_name;
    std::string file_provider_type;
    // TODO(heyuchen): consider add bulk load status
    bulk_load_status::type status;
    DEFINE_JSON_SERIALIZATION(
        app_id, partition_count, app_name, cluster_name, file_provider_type, status)
};

// struct partition_bulk_load_info
//{
//    bulk_load_status::type status;
//    DEFINE_JSON_SERIALIZATION(status)
//};

// struct bulk_load_info
//{
//    std::map<app_id, std::string> app_name;
//    std::map<app_id, std::string> cluster_name;
//    std::map<app_id, std::string> file_provider_type;
//};

// TODO(heyuchen): initialize it
struct bulk_load_context
{
    std::map<app_id, uint32_t> apps_in_progress_count;
    std::map<gpid, dsn::task_ptr> partitions_request;
    std::map<gpid, partition_bulk_load_info> partitions_info;
    std::map<gpid, std::map<dsn::rpc_address, partition_download_progress>>
        partitions_download_progress;
    std::map<gpid, int32_t> partitions_total_download_progress;
    std::map<app_id, bool> apps_cleaning_up;
};

class bulk_load_service
{
public:
    explicit bulk_load_service(meta_service *meta_svc, const std::string &bulk_load_dir);
    void create_bulk_load_dir_on_remote_stroage();

    // client -> meta server to start bulk load
    void on_start_bulk_load(start_bulk_load_rpc rpc);
    // client -> meta server to query bulk load status
    void on_query_bulk_load_status(query_bulk_load_rpc rpc);

    void start_bulk_load_on_remote_storage(std::shared_ptr<app_state> app, start_bulk_load_rpc rpc);

    void create_app_bulk_load_info_with_rpc(std::shared_ptr<app_state> app,
                                            start_bulk_load_rpc rpc);

    void create_partition_bulk_load_info_with_rpc(const std::string &app_name,
                                                  gpid pid,
                                                  uint32_t partition_count,
                                                  const std::string &bulk_load_path,
                                                  start_bulk_load_rpc rpc);

    virtual void partition_bulk_load(gpid pid);

    void on_partition_bulk_load_reply(dsn::error_code err,
                                      bulk_load_response &&response,
                                      gpid pid,
                                      const dsn::rpc_address &primary_addr);

    void update_partition_bulk_load_status(const std::string &app_name,
                                           dsn::gpid pid,
                                           std::string &path,
                                           bulk_load_status::type status);

    void update_app_bulk_load_status_unlock(uint32_t app_id, bulk_load_status::type new_status);

    dsn::error_code check_download_status(bulk_load_response &response);

    // need_remove is only used when trying to set is_bulk_loading to false
    // need_remove = true: remove app bulk load dir on remote storage, otherwise not remove
    void update_app_bulk_load_flag(std::shared_ptr<app_state> app,
                                   bool is_bulk_loading,
                                   bool need_remove);

    void remove_app_bulk_load_dir(uint32_t app_id);

    void clear_app_bulk_load_context(uint32_t app_id);

    void start_sync_apps_bulk_load();

    void do_sync_app_bulk_load(uint32_t app_id, std::string app_path);

    void do_sync_partitions_bulk_load(std::string app_path,
                                      uint32_t app_id,
                                      std::string app_name,
                                      uint32_t partition_count);

    void create_partition_bulk_load_info(const std::string &app_name,
                                         gpid pid,
                                         uint32_t partition_count,
                                         const std::string &bulk_load_path);

    void check_app_bulk_load_dir_exist(std::shared_ptr<app_state> app, bool is_app_bulk_loading);

    template <typename T>
    void erase_map_elem_by_id(uint32_t app_id, std::map<gpid, T> &mymap);

    // app bulk load path is {_bulk_load_root}/{app_id}
    std::string get_app_bulk_load_path(uint32_t app_id) const
    {
        std::stringstream oss;
        oss << _bulk_load_root << "/" << app_id;
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

    bulk_load_status::type get_app_bulk_load_status(uint32_t app_id)
    {
        auto ainfo = _bulk_load_info[app_id];
        return ainfo.status;
    }

    bool is_app_bulk_loading(uint32_t app_id)
    {
        return (_bulk_load_app_id.find(app_id) == _bulk_load_app_id.end() ? false : true);
    }

private:
    meta_service *_meta_svc;
    server_state *_state;

    // bulk load root on remote stroage
    std::string _bulk_load_root;

    // app_id -> bulk_load_context
    std::map<app_id, app_bulk_load_info> _bulk_load_info;

    bulk_load_context _bulk_load_states;

    std::set<uint32_t> _bulk_load_app_id;

    // TODO(heyuchen): lock difference???
    // app lock
    zrwlock_nr &app_lock() const { return _state->_lock; }
    // _bulk_load_states lock
    zrwlock_nr _lock;
};

} // namespace replication
} // namespace dsn
