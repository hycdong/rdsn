// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

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

    // meta sent bulk_load_request to each partition's primary
    virtual void partition_bulk_load(gpid pid);

    void on_partition_bulk_load_reply(dsn::error_code err,
                                      bulk_load_response &&response,
                                      gpid pid,
                                      const dsn::rpc_address &primary_addr);

    // Called when service initialize and meta service leader switch
    // sync app's bulk load status from remote stroage
    // if bulk load dir exist, restart bulk load
    void start_sync_apps_bulk_load();

    // Called when sync_apps_from_remote_stroage, check bulk load state consistency
    // If app->is_bulk_loading = true, app_bulk_load_info should be existed on remote stroage
    // otherwise, app_bulk_load_info shouldn't be existed. This function will handle inconsistency.
    // If app is_bulk_loading = true, and app_bulk_load_info not existed, set is_bulk_loading=false
    // If app is_bulk_loading = false, and app_bulk_load_info existed, remove useless app bulk load
    // on remote stroage
    void check_app_bulk_load_consistency(std::shared_ptr<app_state> app, bool is_app_bulk_loading);

private:
    // check download status in progress, if all partitions download_status is ERR_OK, return
    // ERR_OK,
    // otherwise return the error_code NOT ERR_OK
    dsn::error_code check_download_status(bulk_load_response &response); // private

    // helper function for on_partition_bulk_load_reply
    // handle situation when response.error is NOT ERR_OK during downloading status
    void handle_partition_download_error(gpid pid); // private

    // helper function for on_partition_bulk_load_reply
    // hanlde situation when response.error is ERR_OK during downloading status
    bool handle_partition_bulk_load_downloading(bulk_load_response &response,
                                                const rpc_address &primary_addr); // private

    // helper function for on_partition_bulk_load_reply
    // hanlde situation when response.error is ERR_OK during failed status
    void handle_partition_bulk_load_failed(bulk_load_response &response,
                                           const rpc_address &primary_addr); // private

    // clear bulk load service local variety
    void clear_app_bulk_load_context(uint32_t app_id); // private

    /// remote stroage functions
    // update app's is_bulk_loading = true
    void start_bulk_load_on_remote_storage(std::shared_ptr<app_state> app,
                                           start_bulk_load_rpc rpc); // private + zk
    // create app bulk load dir on remote stroage
    // path: {_bulk_load_root}/{app_id}, value type: app_bulk_load_info
    void create_app_bulk_load_dir_with_rpc(std::shared_ptr<app_state> app,
                                           start_bulk_load_rpc rpc); // private + zk
    // create partition bulk load dir on remote stroage
    // path: {_bulk_load_root}/{app_id}/{<partition_index>}, value type: partition_bulk_load_info
    void create_partition_bulk_load_dir_with_rpc(const std::string &app_name,
                                                 gpid pid,
                                                 uint32_t partition_count,
                                                 const std::string &bulk_load_path,
                                                 start_bulk_load_rpc rpc); // private + zk

    // sync app bulk load info from remote storage
    void do_sync_app_bulk_load(uint32_t app_id, std::string app_path); // private + zk

    // sync partition bulk load info from remote stroage
    // if partition bulk load info exist, send bulk load request to primary
    // otherwise, create partition bulk load info first, then send request to primary
    void do_sync_partitions_bulk_load(std::string app_path,
                                      uint32_t app_id,
                                      std::string app_name,
                                      uint32_t partition_count); // private + zk

    void create_partition_bulk_load_info(const std::string &app_name,
                                         gpid pid,
                                         uint32_t partition_count,
                                         const std::string &bulk_load_path); // private + zk

    // update partition's bulk load status to {status} on remote storage
    void update_partition_bulk_load_status(const std::string &app_name,
                                           dsn::gpid pid,
                                           std::string &path,
                                           bulk_load_status::type status); // private + zk

    // update app's bulk load status to {new_status} on remote storage
    void update_app_bulk_load_status_unlock(uint32_t app_id,
                                            bulk_load_status::type new_status); // private + zk

    // remove app bulk load dir on remote stroage
    void remove_app_bulk_load_dir(uint32_t app_id); // private + zk

    // update app's is_bulk_loading to {is_bulk_loading} on remote_storage
    // need_remove is only used when trying to set is_bulk_loading to false
    // need_remove = true: remove app bulk load dir on remote storage, otherwise not remove
    void update_app_bulk_load_flag(std::shared_ptr<app_state> app,
                                   bool is_bulk_loading,
                                   bool need_remove); // private + zk

    /// helper functions
    template <typename T>
    void erase_map_elem_by_id(uint32_t app_id, std::map<gpid, T> &mymap);

    std::string get_app_bulk_load_path(uint32_t app_id) const
    {
        std::stringstream oss;
        oss << _bulk_load_root << "/" << app_id;
        return oss.str();
    }

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
    friend class meta_bulk_load_service_test;

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
