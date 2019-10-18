// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/server_state.h"

namespace dsn {
namespace replication {

// bulk_load_info is used for remote file provider
struct bulk_load_info
{
    int32_t app_id;
    std::string app_name;
    int32_t partition_count;
    DEFINE_JSON_SERIALIZATION(app_id, app_name, partition_count)
};

// TODO(heyuchen): move it into thrift file
struct app_bulk_load_info
{
    int32_t app_id;
    int32_t partition_count;
    std::string app_name;
    std::string cluster_name;
    std::string file_provider_type;
    bulk_load_status::type status;
    DEFINE_JSON_SERIALIZATION(
        app_id, partition_count, app_name, cluster_name, file_provider_type, status)
};

// TODO(heyuchen): add comments for functions
class bulk_load_service
{
public:
    explicit bulk_load_service(meta_service *meta_svc, const std::string &bulk_load_dir);

    void initialize_bulk_load_service();

    // Called when sync_apps_from_remote_stroage, check bulk load state consistency
    // If app->is_bulk_loading = true, app_bulk_load_info should be existed on remote stroage
    // otherwise, app_bulk_load_info shouldn't be existed. This function will handle inconsistency.
    // If app is_bulk_loading = true, and app_bulk_load_info not existed, set is_bulk_loading=false
    // If app is_bulk_loading = false, and app_bulk_load_info existed, remove useless app bulk load
    // on remote stroage
    void check_app_bulk_load_consistency(std::shared_ptr<app_state> app, bool is_app_bulk_loading);

    // client -> meta server to start bulk load
    void on_start_bulk_load(start_bulk_load_rpc rpc);
    // client -> meta server to query bulk load status
    void on_query_bulk_load_status(query_bulk_load_rpc rpc);

private:
    //
    error_code check_bulk_load_request_params(const std::string &app_name,
                                              const std::string &cluster_name,
                                              const std::string &file_provider,
                                              const int32_t app_id,
                                              const int32_t partition_count);

    // check download status in progress, if all partitions download_status is ERR_OK, return
    // ERR_OK,
    // otherwise return the error_code NOT ERR_OK
    error_code check_download_status(const bulk_load_response &response); // private

    // helper function for on_partition_bulk_load_reply
    // handle situation when response.error is NOT ERR_OK during downloading status
    void handle_partition_download_error(const gpid &pid); // private

    // helper function for on_partition_bulk_load_reply
    // hanlde situation when response.error is ERR_OK during downloading status
    void handle_app_bulk_load_downloading(const bulk_load_response &response,
                                          const rpc_address &primary_addr); // private

    void handle_app_bulk_load_downloaded(const bulk_load_response &response,
                                         const rpc_address &primary_addr);

    // helper function for on_partition_bulk_load_reply
    // hanlde situation when response.error is ERR_OK during failed status or finish status
    void handle_app_bulk_load_cleanup(const bulk_load_response &response,
                                      const rpc_address &primary_addr); // private

    // create ingestion request and send it to primary
    void partition_ingestion(const gpid &pid);

    // receive ingestion response from primary
    void on_partition_ingestion_reply(error_code err,
                                      ingestion_response &&resp,
                                      const std::string &app_name,
                                      const gpid &pid);

    // meta sent bulk_load_request to each partition's primary
    void partition_bulk_load(const gpid &pid);

    void on_partition_bulk_load_reply(error_code err,
                                      ballot req_ballot,
                                      bulk_load_response &&response,
                                      const gpid &pid,
                                      const rpc_address &primary_addr);

    void rollback_to_downloading(int32_t app_id);

    // clear bulk load service local variety
    void clear_app_bulk_load_states(int32_t app_id, const std::string &app_name); // private

    // continue bulk load
    void try_to_continue_bulk_load();

    void get_partition_bulk_load_info_by_app_id(
        int32_t app_id,
        std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map);

    void try_to_continue_app_bulk_load(
        const app_bulk_load_info &ainfo,
        std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map);

    bool check_continue_bulk_load(
        int32_t app_id,
        int32_t partition_count,
        const app_bulk_load_info &ainfo,
        std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map,
        std::unordered_set<int32_t> &different_status_pidx_set);

    bool validate_partition_bulk_load_status(
        const app_bulk_load_info &ainfo,
        std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map,
        std::unordered_set<int32_t> &different_status_pidx_set);

    bulk_load_status::type get_valid_partition_status(bulk_load_status::type app_status);

    void continue_app_bulk_load(
        const app_bulk_load_info &ainfo,
        std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map,
        std::unordered_set<int32_t> different_status_pidx_set);

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
                                                 const gpid &pid,
                                                 int32_t partition_count,
                                                 const std::string &app_path,
                                                 start_bulk_load_rpc rpc); // private + zk

    void create_bulk_load_root_dir(error_code &err, task_tracker &tracker);

    // Called when service initialize and meta service leader switch
    // sync app's bulk load status from remote stroage
    // if bulk load dir exist, restart bulk load
    void sync_apps_bulk_load(error_code &err, task_tracker &tracker);

    // sync app bulk load info from remote storage
    void do_sync_app_bulk_load(int32_t app_id,
                               error_code &err,
                               task_tracker &tracker); // private + zk

    // sync partition bulk load info from remote stroage
    // if partition bulk load info exist, send bulk load request to primary
    // otherwise, create partition bulk load info first, then send request to primary
    void sync_partitions_bulk_load(int32_t app_id,
                                   const std::string &app_name,
                                   error_code &err,
                                   task_tracker &tracker); // private + zk

    void do_sync_partition_bulk_load(const gpid &pid,
                                     const std::string &app_name,
                                     const std::string &partition_path,
                                     error_code &err,
                                     task_tracker &tracker);

    void create_partition_bulk_load_info(const std::string &app_name,
                                         const gpid &pid,
                                         int32_t partition_count); // private + zk

    // update partition's bulk load status to {status} on remote storage
    void update_partition_bulk_load_status(const std::string &app_name,
                                           const gpid &pid,
                                           std::string &path,
                                           bulk_load_status::type status); // private + zk

    // update app's bulk load status to {new_status} on remote storage
    void update_app_bulk_load_status_unlock(int32_t app_id,
                                            bulk_load_status::type new_status); // private + zk

    // remove app bulk load dir on remote stroage
    void remove_app_bulk_load_dir(int32_t app_id, const std::string &app_name);

    // remove app bulk load dir on remote stroage
    // need_set_app_flag = true: update app's is_bulk_loading to false on remote_storage
    void remove_app_bulk_load_dir(std::shared_ptr<app_state> app,
                                  bool need_set_app_flag); // private + zk

    // update app's is_bulk_loading to {is_bulk_loading} on remote_storage
    // if is_bulk_loading = false: clear meta bulk load states
    void update_app_bulk_load_flag(std::shared_ptr<app_state> app,
                                   bool is_bulk_loading); // private + zk

    /// helper functions
    template <typename T>
    void erase_map_elem_by_id(int32_t app_id, std::unordered_map<gpid, T> &mymap);

    std::string get_app_bulk_load_path(int32_t app_id) const
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

    bulk_load_status::type get_app_bulk_load_status(int32_t app_id)
    {
        auto ainfo = _app_bulk_load_info[app_id];
        return ainfo.status;
    }

    bool is_app_bulk_loading(int32_t app_id)
    {
        return (_bulk_load_app_id.find(app_id) == _bulk_load_app_id.end() ? false : true);
    }

    // TODO(heyuchen): move it to common.h/.cpp
    std::string get_bulk_load_info_path(const std::string &app_name,
                                        const std::string &cluster_name)
    {
        // TODO(heyuchen): change "bulk_load_test" from value in config
        std::ostringstream oss;
        oss << "bulk_load_test/" << cluster_name << "/" << app_name << "/"
            << "bulk_load_info";
        return oss.str();
    }

private:
    friend class bulk_load_service_test;

    meta_service *_meta_svc;
    server_state *_state;

    zrwlock_nr &app_lock() const { return _state->_lock; }
    zrwlock_nr _lock; // bulk load states lock

    // bulk load root on remote stroage: {cluster_root}/bulk_load
    std::string _bulk_load_root;

    /// bulk load states
    std::unordered_set<int32_t> _bulk_load_app_id;
    std::unordered_map<app_id, bool> _apps_cleaning_up;
    std::unordered_map<app_id, app_bulk_load_info> _app_bulk_load_info;

    std::unordered_map<gpid, partition_bulk_load_info> _partition_bulk_load_info;
    std::unordered_map<app_id, int32_t> _apps_in_progress_count;
    // inflight partition_request
    std::unordered_map<gpid, dsn::task_ptr> _partitions_request;
    // partition download progress while query bulk load status
    std::unordered_map<gpid, std::map<rpc_address, partition_download_progress>>
        _partitions_download_progress;
    std::unordered_map<gpid, int32_t> _partitions_total_download_progress;
    // TODO(heyuchen): used for query bulk load status(distinguish finish and cleanup) and failover
    std::unordered_map<gpid, bool> _partitions_cleaned_up;
};

} // namespace replication
} // namespace dsn
