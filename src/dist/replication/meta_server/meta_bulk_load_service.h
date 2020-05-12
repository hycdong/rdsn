// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/server_state.h"

namespace dsn {
namespace replication {

///
/// bulk load layout on remote storage:
/// <cluster_root>/bulk_load/<app_id> -> app_bulk_load_info
/// <cluster_root>/bulk_load/<app_id>/<pidx> -> partition_bulk_load_info
///
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

struct partition_bulk_load_info
{
    bulk_load_status::type status;
    bulk_load_metadata metadata;
    DEFINE_JSON_SERIALIZATION(status, metadata)
};

// Used for remote file provider
struct bulk_load_info
{
    int32_t app_id;
    std::string app_name;
    int32_t partition_count;
    DEFINE_JSON_SERIALIZATION(app_id, app_name, partition_count)
};

class bulk_load_service
{
public:
    explicit bulk_load_service(meta_service *meta_svc, const std::string &bulk_load_dir);

    void initialize_bulk_load_service();

    // Called by `sync_apps_from_remote_stroage`, check bulk load state consistency
    // Handle inconsistent conditions below:
    // - app is_bulk_loading = true, app_bulk_load_info not existed, set is_bulk_loading=false
    // - app is_bulk_loading = false, app_bulk_load_info existed, remove useless app bulk load on
    // remote stroage
    void check_app_bulk_load_consistency(std::shared_ptr<app_state> app, bool is_app_bulk_loading);

    // client -> meta server to start bulk load
    void on_start_bulk_load(start_bulk_load_rpc rpc);
    // client -> meta server to query bulk load status
    void on_query_bulk_load_status(query_bulk_load_rpc rpc);
    // client -> meta server to pause/restart/cancel bulk load
    void on_control_bulk_load(control_bulk_load_rpc rpc);

private:
    // TODO(heyuchen): consider pause and cancel
    ///
    /// Bulk load process:
    /// when client sent `start_bulk_load_rpc` to meta server to start bulk load,
    /// meta server create bulk load structures on remote storage, and send `RPC_BULK_LOAD` rpc to
    /// each primary replica periodically until bulk load succeed or failed. whole process below:
    ///
    ///           start bulk load
    ///                  |
    ///                  v
    ///          is_bulk_loading = true
    ///                  |
    ///                  v
    ///     create bulk load info on remote storage
    ///                  |
    ///         Err      v
    ///     ---------Downloading <---------|
    ///     |            |                 |
    ///     |            v         Err     |
    ///     |        Downloaded  --------->|
    ///     |            |                 |
    ///     | IngestErr  v         Err     |
    ///     |<------- Ingesting  --------->|
    ///     |            |                 |
    ///     v            v         Err     |
    ///   Failed       Succeed   --------->|
    ///     |            |
    ///     v            v
    ///    remove bulk load info on remote storage
    ///                  |
    ///                  v
    ///         is_bulk_loading = false
    ///                  |
    ///                  v
    ///            bulk load end

    // Called by `on_start_bulk_load`, check request params
    // - ERR_OK: pass params check
    // - ERR_INVALID_PARAMETERS: wrong file_provider type
    // - ERR_FILE_OPERATION_FAILED: file_provider error
    // - ERR_OBJECT_NOT_FOUND: bulk_load_info not exist, may wrong cluster_name or app_name
    // - ERR_CORRUPTION: bulk_load_info is damaged on file_provider
    // - ERR_INCONSISTENT_STATE: app_id or partition_count inconsistent
    error_code check_bulk_load_request_params(const std::string &app_name,
                                              const std::string &cluster_name,
                                              const std::string &file_provider,
                                              const int32_t app_id,
                                              const int32_t partition_count,
                                              std::string &hint_msg);

    void do_start_app_bulk_load(std::shared_ptr<app_state> app, start_bulk_load_rpc rpc);

    void partition_bulk_load(const std::string &app_name, const gpid &pid);

    void on_partition_bulk_load_reply(error_code err,
                                      const bulk_load_request &request,
                                      const bulk_load_response &response);

    void handle_app_downloading(const bulk_load_response &response,
                                const rpc_address &primary_addr);

    void handle_app_downloaded(const bulk_load_response &response);

    void handle_app_ingestion(const bulk_load_response &response, const rpc_address &primary_addr);

    // when app status is `succeed` or `failed`, meta and replica should cleanup bulk load states
    void handle_bulk_load_finish(const bulk_load_response &response,
                                 const rpc_address &primary_addr);

    void try_rollback_to_downloading(int32_t app_id, const std::string &app_name);

    void handle_bulk_load_failed(int32_t app_id);

    void handle_app_pausing(const bulk_load_response &response, const rpc_address &primary_addr);

    // app not existed or not available during bulk load
    void handle_app_unavailable(int32_t app_id, const std::string &app_name);

    // Called when app status update to ingesting
    // create ingestion request and send it to primary
    void partition_ingestion(const std::string &app_name, const gpid &pid);

    void on_partition_ingestion_reply(error_code err,
                                      ingestion_response &&resp,
                                      const std::string &app_name,
                                      const gpid &pid);

    void reset_local_bulk_load_states(int32_t app_id, const std::string &app_name);

    ///
    /// update bulk load states to remote storage functions
    ///

    void create_app_bulk_load_dir(const std::string &app_name,
                                  int32_t app_id,
                                  int32_t partition_count,
                                  start_bulk_load_rpc rpc);

    void create_partition_bulk_load_dir(const std::string &app_name,
                                        const gpid &pid,
                                        int32_t partition_count,
                                        start_bulk_load_rpc rpc);

    // Called by `handle_app_downloading`
    // update partition bulk load metadata reported by replica server on remote storage
    void update_partition_metadata_on_remote_stroage(const std::string &app_name,
                                                     const gpid &pid,
                                                     const bulk_load_metadata &metadata);

    void update_partition_status_on_remote_stroage(const std::string &app_name,
                                                   const gpid &pid,
                                                   bulk_load_status::type new_status,
                                                   bool should_send_request = false);

    void update_partition_status_on_remote_stroage_rely(const std::string &app_name,
                                                        const gpid &pid,
                                                        bulk_load_status::type new_status,
                                                        bool should_send_request);

    void update_app_status_on_remote_storage_unlock(int32_t app_id,
                                                    bulk_load_status::type new_status,
                                                    bool should_send_request = false);

    void update_app_status_on_remote_storage_reply(const app_bulk_load_info &ainfo,
                                                   bulk_load_status::type old_status,
                                                   bulk_load_status::type new_status,
                                                   bool should_send_request);

    // `need_set_app_flag` = true: update app's is_bulk_loading to false on remote_storage
    void remove_bulk_load_dir(std::shared_ptr<app_state> app, bool need_set_app_flag);

    void remove_bulk_load_dir(int32_t app_id, const std::string &app_name);

    // update app's is_bulk_loading to <is_bulk_loading> on remote_storage
    void update_app_is_bulk_loading(std::shared_ptr<app_state> app, bool is_bulk_loading);

    ///
    /// sync bulk load states from remote storage
    /// called when service initialized or meta server leader switch
    ///
    void create_bulk_load_root_dir(error_code &err, task_tracker &tracker);

    void sync_apps_bulk_load_from_remote_stroage(error_code &err, task_tracker &tracker);

    void do_sync_app_bulk_load(int32_t app_id, error_code &err, task_tracker &tracker);

    void sync_partitions_bulk_load_from_remote_stroage(int32_t app_id,
                                                       const std::string &app_name,
                                                       error_code &err,
                                                       task_tracker &tracker);

    void do_sync_partition_bulk_load(const gpid &pid,
                                     const std::string &app_name,
                                     const std::string &partition_path,
                                     error_code &err,
                                     task_tracker &tracker);

    ///
    /// try to continue bulk load according to states from remote stroage
    /// called when service initialized or meta server leader switch
    ///
    void try_to_continue_bulk_load();

    void continue_bulk_load(
        const app_bulk_load_info &ainfo,
        std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map);

    bool check_bulk_load_status(
        int32_t app_id,
        int32_t partition_count,
        const app_bulk_load_info &ainfo,
        std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map,
        std::unordered_set<int32_t> &different_status_pidx_set /*out*/);

    bool validate_partition_bulk_load_status(
        const app_bulk_load_info &ainfo,
        std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map,
        std::unordered_set<int32_t> &different_status_pidx_set /*out*/);

    void do_continue_bulk_load(
        const app_bulk_load_info &ainfo,
        std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map,
        std::unordered_set<int32_t> &different_status_pidx_set);

    // Called by `do_continue_bulk_load`
    // only used when meta server leader switch and when app status is downloading and some
    // partition bulk load not existed on remote storage
    void create_partition_bulk_load_dir(const std::string &app_name,
                                        const gpid &pid,
                                        int32_t partition_count);
    ///
    /// helper functions
    ///
    inline std::string get_bulk_load_info_path(const std::string &app_name,
                                               const std::string &cluster_name) const
    {
        std::ostringstream oss;
        oss << _meta_svc->get_options().bulk_load_provider_root << "/" << cluster_name << "/"
            << app_name << "/" << bulk_load_constant::BULK_LOAD_INFO;
        return oss.str();
    }

    inline std::string get_app_bulk_load_path(int32_t app_id) const
    {
        std::stringstream oss;
        oss << _bulk_load_root << "/" << app_id;
        return oss.str();
    }

    inline std::string get_partition_bulk_load_path(const std::string &app_bulk_load_path,
                                                    int partition_id) const
    {
        std::stringstream oss;
        oss << app_bulk_load_path << "/" << partition_id;
        return oss.str();
    }

    inline std::string get_partition_bulk_load_path(const gpid &pid) const
    {
        std::stringstream oss;
        oss << get_app_bulk_load_path(pid.get_app_id()) << "/" << pid.get_partition_index();
        return oss.str();
    }

    bulk_load_status::type get_app_bulk_load_status(int32_t app_id)
    {
        zauto_read_lock l(_lock);
        return get_app_bulk_load_status_unlock(app_id);
    }

    bulk_load_status::type get_app_bulk_load_status_unlock(int32_t app_id)
    {
        if (_app_bulk_load_info.find(app_id) != _app_bulk_load_info.end()) {
            return _app_bulk_load_info[app_id].status;
        } else {
            return bulk_load_status::BLS_INVALID;
        }
    }

    bool is_app_bulk_loading_unlock(int32_t app_id)
    {
        return (_bulk_load_app_id.find(app_id) == _bulk_load_app_id.end() ? false : true);
    }

    bool is_partition_metadata_not_updated(gpid pid)
    {
        zauto_read_lock l(_lock);
        return is_partition_metadata_not_updated_unlock(pid);
    }

    bool is_partition_metadata_not_updated_unlock(gpid pid)
    {
        if (_partition_bulk_load_info.find(pid) != _partition_bulk_load_info.end()) {
            bulk_load_metadata metadata = _partition_bulk_load_info[pid].metadata;
            return (metadata.files.size() == 0 && metadata.file_total_size == 0);
        } else {
            return false;
        }
    }

    bulk_load_status::type get_partition_bulk_load_status_unlock(gpid pid)
    {
        if (_partition_bulk_load_info.find(pid) != _partition_bulk_load_info.end()) {
            return _partition_bulk_load_info[pid].status;
        } else {
            return bulk_load_status::BLS_INVALID;
        }
    }

    template <typename T>
    inline void erase_map_elem_by_id(int32_t app_id, std::unordered_map<gpid, T> &mymap)
    {
        for (auto iter = mymap.begin(); iter != mymap.end();) {
            if (iter->first.get_app_id() == app_id) {
                mymap.erase(iter++);
            }
        }
    }

    inline void set_control_response(bulk_load_status::type local_status,
                                     const std::string &app_name,
                                     control_bulk_load_response &resp);

private:
    friend class bulk_load_service_test;

    meta_service *_meta_svc;
    server_state *_state;

    zrwlock_nr &app_lock() const { return _state->_lock; }
    zrwlock_nr _lock; // bulk load states lock

    std::string _bulk_load_root; // <cluster_root>/bulk_load

    /// bulk load states
    std::unordered_set<int32_t> _bulk_load_app_id;
    std::unordered_map<app_id, bool> _apps_cleaning_up;
    std::unordered_map<app_id, app_bulk_load_info> _app_bulk_load_info;

    std::unordered_map<app_id, int32_t> _apps_in_progress_count;
    std::unordered_map<app_id, bool> _apps_pending_sync_flag;

    std::unordered_map<gpid, partition_bulk_load_info> _partition_bulk_load_info;
    std::unordered_map<gpid, bool> _partitions_pending_sync_flag;

    // partition download progress while query bulk load status
    std::unordered_map<gpid, int32_t> _partitions_total_download_progress;
    std::unordered_map<gpid, bool> _partitions_cleaned_up;
    std::unordered_map<gpid, std::map<rpc_address, partition_bulk_load_state>>
        _partitions_bulk_load_state;
};

} // namespace replication
} // namespace dsn
