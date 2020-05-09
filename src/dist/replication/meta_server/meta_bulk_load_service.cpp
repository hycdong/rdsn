// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/fail_point.h>

#include "meta_bulk_load_service.h"

namespace dsn {
namespace replication {

bulk_load_service::bulk_load_service(meta_service *meta_svc, const std::string &bulk_load_dir)
    : _meta_svc(meta_svc), _bulk_load_root(bulk_load_dir)
{
    _state = _meta_svc->get_server_state();
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::initialize_bulk_load_service()
{
    task_tracker tracker;
    error_code err = ERR_OK;

    create_bulk_load_root_dir(err, tracker);
    tracker.wait_outstanding_tasks();

    if (err == ERR_OK) {
        try_to_continue_bulk_load();
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::on_start_bulk_load(start_bulk_load_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    {
        zauto_read_lock l(app_lock());

        app = _state->get_app(request.app_name);
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            derror_f("app({}) is not existed or not available", request.app_name);
            response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
            response.hint_msg = fmt::format(
                "app {}", response.err == ERR_APP_NOT_EXIST ? "not existed" : "dropped");
            return;
        }

        if (app->is_bulk_loading) {
            derror_f("app({}) is already executing bulk load, please wait", app->app_name);
            response.err = ERR_BUSY;
            response.hint_msg = "app is already executing bulk load";
            return;
        }
    }

    std::string hint_msg;
    error_code e = check_bulk_load_request_params(request.app_name,
                                                  request.cluster_name,
                                                  request.file_provider_type,
                                                  app->app_id,
                                                  app->partition_count,
                                                  hint_msg);
    if (e != ERR_OK) {
        response.err = e;
        response.hint_msg = hint_msg;
        return;
    }

    ddebug_f("app({}) start bulk load, cluster_name = {}, provider = {}",
             request.app_name,
             request.cluster_name,
             request.file_provider_type);

    // avoid possible load balancing
    _meta_svc->set_function_level(meta_function_level::fl_steady);

    do_start_app_bulk_load(std::move(app), std::move(rpc));
}

// ThreadPool: THREAD_POOL_META_STATE
error_code bulk_load_service::check_bulk_load_request_params(const std::string &app_name,
                                                             const std::string &cluster_name,
                                                             const std::string &file_provider,
                                                             const int32_t app_id,
                                                             const int32_t partition_count,
                                                             std::string &hint_msg)
{
    FAIL_POINT_INJECT_F("meta_check_bulk_load_request_params",
                        [](dsn::string_view) -> error_code { return ERR_OK; });

    // check file provider
    dsn::dist::block_service::block_filesystem *blk_fs =
        _meta_svc->get_block_service_manager().get_block_filesystem(file_provider);
    if (blk_fs == nullptr) {
        derror_f("invalid remote file provider type: {}", file_provider);
        hint_msg = "invalid file_provider";
        return ERR_INVALID_PARAMETERS;
    }

    // sync get bulk_load_info file_handler
    const std::string remote_path = get_bulk_load_info_path(app_name, cluster_name);
    dsn::dist::block_service::create_file_request cf_req;
    cf_req.file_name = remote_path;
    cf_req.ignore_metadata = true;
    error_code err = ERR_OK;
    dsn::dist::block_service::block_file_ptr file_handler = nullptr;
    blk_fs
        ->create_file(
            cf_req,
            TASK_CODE_EXEC_INLINED,
            [&err, &file_handler](const dsn::dist::block_service::create_file_response &resp) {
                err = resp.err;
                file_handler = resp.file_handle;
            })
        ->wait();

    if (err != ERR_OK || file_handler == nullptr) {
        derror_f(
            "failed to get file({}) handler on remote provider({})", remote_path, file_provider);
        hint_msg = "file_provider error";
        return ERR_FILE_OPERATION_FAILED;
    }

    // sync read bulk_load_info on file provider
    dsn::dist::block_service::read_response r_resp;
    file_handler
        ->read(dsn::dist::block_service::read_request{0, -1},
               TASK_CODE_EXEC_INLINED,
               [&r_resp](const dsn::dist::block_service::read_response &resp) { r_resp = resp; })
        ->wait();
    if (r_resp.err != ERR_OK) {
        derror_f("failed to read file({}) on remote provider({}), error = {}",
                 file_provider,
                 remote_path,
                 r_resp.err.to_string());
        hint_msg = "read bulk_load_info failed";
        return r_resp.err;
    }

    bulk_load_info bl_info;
    if (!::dsn::json::json_forwarder<bulk_load_info>::decode(r_resp.buffer, bl_info)) {
        derror_f("file({}) is damaged on remote file provider({})", remote_path, file_provider);
        hint_msg = "bulk_load_info damaged";
        return ERR_CORRUPTION;
    }

    if (bl_info.app_id != app_id || bl_info.partition_count != partition_count) {
        derror_f("app({}) information is inconsistent, local app_id({}) VS remote app_id({}), "
                 "local partition_count({}) VS remote partition_count({})",
                 app_name,
                 app_id,
                 bl_info.app_id,
                 partition_count,
                 bl_info.partition_count);
        hint_msg = "app_id or partition_count is inconsistent";
        return ERR_INCONSISTENT_STATE;
    }

    return ERR_OK;
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::do_start_app_bulk_load(std::shared_ptr<app_state> app,
                                               start_bulk_load_rpc rpc)
{
    app_info info = *app;
    info.__set_is_bulk_loading(true);

    blob value = dsn::json::json_forwarder<app_info>::encode(info);
    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), [app, rpc, this]() {
            {
                zauto_write_lock l(app_lock());
                app->is_bulk_loading = true;
            }
            {
                zauto_write_lock l(_lock);
                _bulk_load_app_id.insert(app->app_id);
                _apps_in_progress_count[app->app_id] = app->partition_count;
            }
            create_app_bulk_load_dir(
                app->app_name, app->app_id, app->partition_count, std::move(rpc));
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::create_app_bulk_load_dir(const std::string &app_name,
                                                 int32_t app_id,
                                                 int32_t partition_count,
                                                 start_bulk_load_rpc rpc)
{
    const auto &req = rpc.request();

    app_bulk_load_info ainfo;
    ainfo.app_id = app_id;
    ainfo.app_name = app_name;
    ainfo.partition_count = partition_count;
    ainfo.status = bulk_load_status::BLS_DOWNLOADING;
    ainfo.cluster_name = req.cluster_name;
    ainfo.file_provider_type = req.file_provider_type;
    blob value = dsn::json::json_forwarder<app_bulk_load_info>::encode(ainfo);

    _meta_svc->get_meta_storage()->create_node(
        get_app_bulk_load_path(app_id), std::move(value), [rpc, ainfo, this]() {
            dinfo_f("create app({}) bulk load dir", ainfo.app_name);
            {
                zauto_write_lock l(_lock);
                _app_bulk_load_info[ainfo.app_id] = ainfo;
                _apps_pending_sync_flag[ainfo.app_id] = false;
            }
            for (int32_t i = 0; i < ainfo.partition_count; ++i) {
                create_partition_bulk_load_dir(
                    ainfo.app_name, gpid(ainfo.app_id, i), ainfo.partition_count, std::move(rpc));
            }
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::create_partition_bulk_load_dir(const std::string &app_name,
                                                       const gpid &pid,
                                                       int32_t partition_count,
                                                       start_bulk_load_rpc rpc)
{
    partition_bulk_load_info pinfo;
    pinfo.status = bulk_load_status::BLS_DOWNLOADING;
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->create_node(
        get_partition_bulk_load_path(pid),
        std::move(value),
        [app_name, pid, partition_count, rpc, pinfo, this]() {
            dinfo_f("app({}) create partition({}) bulk_load_info", app_name, pid.to_string());
            {
                zauto_write_lock l(_lock);
                _partition_bulk_load_info[pid] = pinfo;
                _partitions_pending_sync_flag[pid] = false;
                if (--_apps_in_progress_count[pid.get_app_id()] == 0) {
                    ddebug_f("app({}) start bulk load succeed", app_name);
                    _apps_in_progress_count[pid.get_app_id()] = partition_count;
                    auto response = rpc.response();
                    response.err = ERR_OK;
                }
            }
            // start send bulk load to replica servers
            partition_bulk_load(app_name, pid);
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::partition_bulk_load(const std::string &app_name, const gpid &pid)
{
    FAIL_POINT_INJECT_F("meta_bulk_load_partition_bulk_load", [](dsn::string_view) {});

    rpc_address primary_addr;
    ballot b;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            dwarn_f("app(name={}, id={}) is not existed, set bulk load failed",
                    app_name,
                    pid.get_app_id());
            handle_app_unavailable(pid.get_app_id(), app_name);
            return;
        }
        primary_addr = app->partitions[pid.get_partition_index()].primary;
        b = app->partitions[pid.get_partition_index()].ballot;
    }

    if (primary_addr.is_invalid()) {
        dwarn_f("app({}) partition({}) primary is invalid, try it later", app_name, pid);
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_bulk_load, this, app_name, pid),
                         0,
                         std::chrono::seconds(1));
        return;
    }

    zauto_read_lock l(_lock);
    app_bulk_load_info ainfo = _app_bulk_load_info[pid.get_app_id()];
    auto req = make_unique<bulk_load_request>();
    req->pid = pid;
    req->app_name = app_name;
    req->primary_addr = primary_addr;
    req->remote_provider_name = ainfo.file_provider_type;
    req->cluster_name = ainfo.cluster_name;
    req->meta_bulk_load_status = get_partition_bulk_load_status_unlock(pid);
    req->ballot = b;
    req->query_bulk_load_metadata = is_partition_metadata_not_updated_unlock(pid);

    ddebug_f("send bulk load request to replica server({}), app({}), partition({}), partition "
             "status = {}, remote provider = {}, cluster_name = {}",
             primary_addr.to_string(),
             app_name,
             pid,
             dsn::enum_to_string(req->meta_bulk_load_status),
             req->remote_provider_name,
             req->cluster_name);

    bulk_load_rpc rpc(std::move(req), RPC_BULK_LOAD, 0_ms, 0, pid.thread_hash());
    rpc.call(primary_addr, _meta_svc->tracker(), [this, rpc](error_code err) mutable {
        on_partition_bulk_load_reply(err, rpc.request(), rpc.response());
    });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::on_partition_bulk_load_reply(error_code err,
                                                     const bulk_load_request &request,
                                                     const bulk_load_response &response)
{
    const std::string &app_name = request.app_name;
    const gpid &pid = request.pid;
    const rpc_address &primary_addr = request.primary_addr;
    int32_t interval_ms = _meta_svc->get_options().partition_bulk_load_interval_ms;

    if (err != ERR_OK) {
        dwarn_f("app({}), partition({}) failed to recevie bulk load response, error = {}",
                pid.get_app_id(),
                pid,
                err.to_string());
        try_rollback_to_downloading(pid.get_app_id(), app_name);
    } else if (response.err == ERR_OBJECT_NOT_FOUND || response.err == ERR_INVALID_STATE) {
        dwarn_f("app({}), partition({}) doesn't exist or has invalid state on node({}), error = {}",
                app_name,
                pid,
                primary_addr.to_string(),
                response.err.to_string());
        try_rollback_to_downloading(pid.get_app_id(), app_name);
    } else if (response.err == ERR_BUSY) {
        dwarn_f("node({}) has enough replicas downloading, wait to next round to send bulk load "
                "request for app({}), partition({})",
                primary_addr.to_string(),
                app_name,
                pid);
    } else if (response.err != ERR_OK) {
        derror_f("app({}), partition({}) handle bulk load response failed, error = {}, primary "
                 "status = {}",
                 app_name,
                 pid,
                 response.err.to_string(),
                 dsn::enum_to_string(response.primary_bulk_load_status));
        handle_bulk_load_failed(pid.get_app_id());
    } else {
        ballot current_ballot;
        {
            zauto_read_lock l(app_lock());
            std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
            if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
                dwarn_f("app(name={}, id={}) is not existed, set bulk load failed",
                        app_name,
                        pid.get_app_id());
                handle_app_unavailable(pid.get_app_id(), app_name);
                return;
            }
            current_ballot = app->partitions[pid.get_partition_index()].ballot;
        }

        if (request.ballot < current_ballot) {
            dwarn_f("receive out-date response, app({}), partition({}), request ballot = {}, "
                    "current ballot= {}",
                    app_name,
                    pid,
                    request.ballot,
                    current_ballot);
            try_rollback_to_downloading(pid.get_app_id(), app_name);
        }

        // do bulk load
        bulk_load_status::type app_status = get_app_bulk_load_status(response.pid.get_app_id());
        if (app_status == bulk_load_status::BLS_DOWNLOADING) {
            handle_app_downloading(response, primary_addr);
        } else if (app_status == bulk_load_status::BLS_DOWNLOADED) {
            // when app status or partition status is ingesting, send request frequently
            interval_ms = bulk_load_constant::BULK_LOAD_REQUEST_SHORT_INTERVAL_MS;
            handle_app_downloaded(response);
        } else if (app_status == bulk_load_status::BLS_INGESTING) {
            interval_ms = bulk_load_constant::BULK_LOAD_REQUEST_SHORT_INTERVAL_MS;
            handle_app_ingestion(response, primary_addr);
        } else if (app_status == bulk_load_status::BLS_SUCCEED ||
                   app_status == bulk_load_status::BLS_FAILED ||
                   app_status == bulk_load_status::BLS_CANCELED) {
            handle_bulk_load_finish(response, primary_addr);
        } else if (app_status == bulk_load_status::BLS_PAUSING) {
            handle_app_pausing(response, primary_addr);
        } else if (app_status == bulk_load_status::BLS_PAUSED) {
            // paused not send request to replica servers
            return;
        } else {
            // do nothing in other status
        }
    }

    zauto_read_lock l(_lock);
    if (is_app_bulk_loading_unlock(pid.get_app_id())) {
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_bulk_load, this, app_name, pid),
                         0,
                         std::chrono::milliseconds(interval_ms));
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_app_downloading(const bulk_load_response &response,
                                               const rpc_address &primary_addr)
{
    std::string app_name = response.app_name;
    gpid pid = response.pid;

    if (!response.__isset.total_download_progress) {
        dwarn_f(
            "recevie bulk load response from node({}) app({}), partition({}), primary_status({}), "
            "but total_download_progress is not set",
            primary_addr.to_string(),
            app_name,
            pid,
            dsn::enum_to_string(response.primary_bulk_load_status));
        return;
    }

    for (auto iter = response.group_bulk_load_state.begin();
         iter != response.group_bulk_load_state.end();
         ++iter) {
        partition_bulk_load_state states = iter->second;
        if (!states.__isset.download_progress || !states.__isset.download_status) {
            dwarn_f("recevie bulk load response from node({}) app({}), partition({}), "
                    "primary_status({}), "
                    "but node({}) progress or status is not set",
                    primary_addr.to_string(),
                    app_name,
                    pid,
                    dsn::enum_to_string(response.primary_bulk_load_status),
                    iter->first.to_string());
            return;
        }

        // check partition download status
        if (states.download_status != ERR_OK) {
            dwarn_f(
                "app({}) partition({}) meet error during downloading sst files, node address = {}, "
                "error = {}",
                app_name,
                pid,
                iter->first.to_string(),
                states.download_status.to_string());
            handle_bulk_load_failed(pid.get_app_id());
            return;
        }
    }

    // if primary replica report metadata, update metadata to remote storage
    if (response.__isset.metadata && is_partition_metadata_not_updated(pid)) {
        update_partition_metadata_on_remote_stroage(app_name, pid, response.metadata);
    }

    // update download progress
    int32_t total_progress = response.total_download_progress;
    ddebug_f("recevie bulk load response from node({}) app({}) partition({}), primary_status({}), "
             "total_download_progress = {}",
             primary_addr.to_string(),
             app_name,
             pid.to_string(),
             dsn::enum_to_string(response.primary_bulk_load_status),
             total_progress);
    {
        zauto_write_lock l(_lock);
        _partitions_total_download_progress[pid] = total_progress;
        _partitions_bulk_load_state[pid] = response.group_bulk_load_state;
    }

    // update partition status to `downloaded` if all replica downloaded
    if (total_progress >= bulk_load_constant::PROGRESS_FINISHED) {
        ddebug_f("app({}) partirion({}) download files from remote provider succeed",
                 app_name,
                 pid.to_string());
        update_partition_status_on_remote_stroage(app_name, pid, bulk_load_status::BLS_DOWNLOADED);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_partition_metadata_on_remote_stroage(
    const std::string &app_name, const gpid &pid, const bulk_load_metadata &metadata)
{
    zauto_read_lock l(_lock);
    partition_bulk_load_info pinfo = _partition_bulk_load_info[pid];
    pinfo.metadata = metadata;
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);
    std::string path = get_partition_bulk_load_path(pid);
    _meta_svc->get_meta_storage()->set_data(
        std::move(path), std::move(value), [this, app_name, pid, pinfo]() {
            zauto_write_lock l(_lock);
            _partition_bulk_load_info[pid] = pinfo;
            ddebug_f("app({}) update partition({}) bulk load metadata, file count={}, file size={}",
                     app_name,
                     pid.to_string(),
                     pinfo.metadata.files.size(),
                     pinfo.metadata.file_total_size);
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_app_downloaded(const bulk_load_response &response)
{
    // update partition status to `ingesting` directly if app status is downloaded
    update_partition_status_on_remote_stroage(
        response.app_name, response.pid, bulk_load_status::BLS_INGESTING);
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_app_ingestion(const bulk_load_response &response,
                                             const rpc_address &primary_addr)
{
    std::string app_name = response.app_name;
    gpid pid = response.pid;

    if (!response.__isset.is_group_ingestion_finished) {
        dwarn_f("recevie bulk load response from node({}) app({}) partition({}), "
                "primary_status({}), but not set is_group_ingestion_finished",
                primary_addr.to_string(),
                response.app_name,
                pid,
                dsn::enum_to_string(response.primary_bulk_load_status));
        return;
    }

    for (auto iter = response.group_bulk_load_state.begin();
         iter != response.group_bulk_load_state.end();
         ++iter) {
        partition_bulk_load_state states = iter->second;
        if (!states.__isset.ingest_status) {
            dwarn_f("recevie bulk load response from node({}) app({}) partition({}), "
                    "primary_status({}), but node({}) not set ingestion_status",
                    primary_addr.to_string(),
                    response.app_name,
                    pid,
                    dsn::enum_to_string(response.primary_bulk_load_status),
                    iter->first.to_string());
            return;
        }

        if (states.ingest_status == ingestion_status::IS_FAILED) {
            derror_f("app({}) partition({}) node({}) ingestion failed",
                     app_name,
                     pid,
                     iter->first.to_string());
            handle_bulk_load_failed(pid.get_app_id());
            return;
        }
    }

    ddebug_f("recevie bulk load response from node({}) app({}) partition({}), primary_status({}), "
             "is_group_ingestion_finished = {}",
             primary_addr.to_string(),
             app_name,
             pid.to_string(),
             dsn::enum_to_string(response.primary_bulk_load_status),
             response.is_group_ingestion_finished);
    {
        zauto_write_lock l(_lock);
        _partitions_bulk_load_state[pid] = response.group_bulk_load_state;
    }

    if (response.is_group_ingestion_finished) {
        ddebug_f("app({}) partition({}) ingestion files succeed", app_name, pid.to_string());
        update_partition_status_on_remote_stroage(app_name, pid, bulk_load_status::BLS_SUCCEED);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_bulk_load_finish(const bulk_load_response &response,
                                                const rpc_address &primary_addr)
{
    gpid pid = response.pid;
    if (!response.__isset.is_group_bulk_load_context_cleaned) {
        dwarn_f("recevie bulk load response from node({}) app({}) partition({}), "
                "primary_status({}), but not checking cleanup",
                primary_addr.to_string(),
                response.app_name,
                pid,
                dsn::enum_to_string(response.primary_bulk_load_status));
        return;
    }

    for (auto iter = response.group_bulk_load_state.begin();
         iter != response.group_bulk_load_state.end();
         ++iter) {
        partition_bulk_load_state states = iter->second;
        if (!states.__isset.is_cleanuped) {
            dwarn_f("recevie bulk load response from node({}) app({}) partition({}), "
                    "primary_status({}), but node({}) not set cleanup flag",
                    primary_addr.to_string(),
                    response.app_name,
                    pid,
                    dsn::enum_to_string(response.primary_bulk_load_status),
                    iter->first.to_string());
            return;
        }
    }

    {
        zauto_read_lock l(_lock);
        if (_partitions_cleaned_up[pid]) {
            dwarn_f(
                "recevie bulk load response from node({}) app({}) partition({}), current partition "
                "has already be cleaned up",
                primary_addr.to_string(),
                response.app_name,
                pid.to_string());
            return;
        }
    }

    bool all_clean_up = response.is_group_bulk_load_context_cleaned;
    ddebug_f("recevie bulk load response from node({}) app({}) partition({}), primary status = {}, "
             "group_bulk_load_context_cleaned = {}",
             primary_addr.to_string(),
             response.app_name,
             pid.to_string(),
             dsn::enum_to_string(response.primary_bulk_load_status),
             all_clean_up);
    {
        zauto_write_lock l(_lock);
        _partitions_cleaned_up[pid] = all_clean_up;
        _partitions_bulk_load_state[pid] = response.group_bulk_load_state;
    }

    if (all_clean_up) {
        int count;
        std::shared_ptr<app_state> app;
        {
            zauto_write_lock l(_lock);
            count = --_apps_in_progress_count[pid.get_app_id()];
        }
        if (count == 0) {
            {
                zauto_read_lock l(app_lock());
                app = _state->get_app(pid.get_app_id());
                if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
                    dwarn_f("app(name={}, id={}) is not existed, set bulk load failed",
                            response.app_name,
                            pid.get_app_id());
                    remove_bulk_load_dir(pid.get_app_id(), response.app_name);
                    return;
                }
            }
            ddebug_f("app({}) all partitions cleanup bulk load context", response.app_name);
            remove_bulk_load_dir(std::move(app), true);
        }
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::try_rollback_to_downloading(int32_t app_id, const std::string &app_name)
{
    zauto_read_lock l(_lock);
    const auto app_status = get_app_bulk_load_status_unlock(app_id);
    if (app_status == bulk_load_status::BLS_DOWNLOADING ||
        app_status == bulk_load_status::BLS_DOWNLOADED ||
        app_status == bulk_load_status::BLS_INGESTING ||
        app_status == bulk_load_status::BLS_SUCCEED) {
        update_app_status_on_remote_storage_unlock(app_id, bulk_load_status::type::BLS_DOWNLOADING);
    } else {
        ddebug_f("app({}) status={}, no need to rollback to downloading, wait for next round",
                 app_name,
                 dsn::enum_to_string(app_status));
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_bulk_load_failed(int32_t app_id)
{
    zauto_write_lock l(_lock);
    if (!_apps_cleaning_up[app_id]) {
        _apps_cleaning_up[app_id] = true;
        update_app_status_on_remote_storage_unlock(app_id, bulk_load_status::BLS_FAILED);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_app_pausing(const bulk_load_response &response,
                                           const rpc_address &primary_addr)
{
    gpid pid = response.pid;
    if (!response.__isset.is_group_bulk_load_paused) {
        dwarn_f("recevie bulk load response from node({}) app({}) partition({}), primary "
                "status({}), but not checking group is_paused",
                primary_addr.to_string(),
                response.app_name,
                pid,
                dsn::enum_to_string(response.primary_bulk_load_status));
        return;
    }

    for (auto iter = response.group_bulk_load_state.begin();
         iter != response.group_bulk_load_state.end();
         ++iter) {
        partition_bulk_load_state states = iter->second;
        if (!states.__isset.is_paused) {
            dwarn_f("recevie bulk load response from node({}) app({}) partition({}), "
                    "primary_status({}), but node({}) not set paused flag",
                    primary_addr.to_string(),
                    response.app_name,
                    pid,
                    dsn::enum_to_string(response.primary_bulk_load_status),
                    iter->first.to_string());
            return;
        }
    }

    bool is_group_paused = response.is_group_bulk_load_paused;
    ddebug_f("recevie bulk load response from node({}) app({}) partition({}), primary status = {}, "
             "group_bulk_load_paused = {}",
             primary_addr.to_string(),
             response.app_name,
             pid.to_string(),
             dsn::enum_to_string(response.primary_bulk_load_status),
             is_group_paused);

    {
        zauto_write_lock l(_lock);
        _partitions_bulk_load_state[pid] = response.group_bulk_load_state;
    }

    if (is_group_paused) {
        ddebug_f(
            "app({}) partirion({}) pause bulk load succeed", response.app_name, pid.to_string());
        update_partition_status_on_remote_stroage(
            response.app_name, pid, bulk_load_status::BLS_PAUSED);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::handle_app_unavailable(int32_t app_id, const std::string &app_name)
{
    zauto_write_lock l(_lock);
    if (is_app_bulk_loading_unlock(app_id) && !_apps_cleaning_up[app_id]) {
        _apps_cleaning_up[app_id] = true;
        remove_bulk_load_dir(app_id, app_name);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_partition_status_on_remote_stroage(const std::string &app_name,
                                                                  const gpid &pid,
                                                                  bulk_load_status::type new_status,
                                                                  bool should_send_request)
{
    zauto_read_lock l(_lock);
    partition_bulk_load_info pinfo = _partition_bulk_load_info[pid];

    if (pinfo.status == new_status && new_status != bulk_load_status::BLS_DOWNLOADING) {
        dinfo_f("app({}) partition({}) old status:{} VS new status:{}, ignore it",
                app_name,
                pid.to_string(),
                dsn::enum_to_string(pinfo.status),
                dsn::enum_to_string(new_status));
        return;
    }

    if (_partitions_pending_sync_flag[pid]) {
        ddebug_f("app({}) partition({}) has already sync bulk load status, wait for next round",
                 app_name,
                 pid);
        return;
    }

    pinfo.status = new_status;
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _partitions_pending_sync_flag[pid] = true;
    std::string path = get_partition_bulk_load_path(pid);
    _meta_svc->get_meta_storage()->set_data(
        std::move(path),
        std::move(value),
        std::bind(&bulk_load_service::update_partition_status_on_remote_stroage_rely,
                  this,
                  app_name,
                  pid,
                  new_status,
                  should_send_request));
}

void bulk_load_service::update_partition_status_on_remote_stroage_rely(
    const std::string &app_name,
    const gpid &pid,
    bulk_load_status::type new_status,
    bool should_send_request)
{
    {
        zauto_write_lock l(_lock);
        bulk_load_status::type old_status = _partition_bulk_load_info[pid].status;
        _partition_bulk_load_info[pid].status = new_status;
        _partitions_pending_sync_flag[pid] = false;

        ddebug_f("app({}) update partition({}) status from {} to {}",
                 app_name,
                 pid.to_string(),
                 dsn::enum_to_string(old_status),
                 dsn::enum_to_string(new_status));

        if ((new_status == bulk_load_status::BLS_DOWNLOADED ||
             new_status == bulk_load_status::BLS_INGESTING ||
             new_status == bulk_load_status::BLS_SUCCEED ||
             new_status == bulk_load_status::BLS_PAUSED) &&
            old_status != new_status) {
            if (--_apps_in_progress_count[pid.get_app_id()] == 0) {
                update_app_status_on_remote_storage_unlock(pid.get_app_id(), new_status);
            }
        } else if (new_status == bulk_load_status::BLS_DOWNLOADING && old_status != new_status) {
            // TODO(heyuchen): consider here
            _partitions_bulk_load_state[pid].clear();
            _partitions_total_download_progress[pid] = 0;
            _partitions_cleaned_up[pid] = false;
            if (--_apps_in_progress_count[pid.get_app_id()] == 0) {
                _apps_in_progress_count[pid.get_app_id()] =
                    _app_bulk_load_info[pid.get_app_id()].partition_count;
                ddebug_f("app({}) restart to bulk load", app_name);
            }

        } else if (new_status == bulk_load_status::BLS_FAILED ||
                   new_status == bulk_load_status::BLS_PAUSING ||
                   new_status == bulk_load_status::BLS_CANCELED) {
            // do nothing
        }
    }
    if (should_send_request) {
        partition_bulk_load(app_name, pid);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_app_status_on_remote_storage_unlock(
    int32_t app_id, bulk_load_status::type new_status, bool should_send_request)
{
    FAIL_POINT_INJECT_F("meta_update_app_status_on_remote_storage_unlock", [](dsn::string_view) {});

    app_bulk_load_info ainfo = _app_bulk_load_info[app_id];
    auto old_status = ainfo.status;

    if (old_status == new_status && new_status != bulk_load_status::BLS_DOWNLOADING) {
        dwarn_f("app({}) old status:{} VS new status:{}, ignore it",
                ainfo.app_name,
                dsn::enum_to_string(old_status),
                dsn::enum_to_string(new_status));
        return;
    }

    if (_apps_pending_sync_flag[app_id]) {
        ddebug_f("app({}) has already sync bulk load status, wait and retry, cur={}, new={}",
                 ainfo.app_name,
                 dsn::enum_to_string(old_status),
                 dsn::enum_to_string(new_status));
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::update_app_status_on_remote_storage_unlock,
                                   this,
                                   app_id,
                                   new_status,
                                   should_send_request),
                         0,
                         std::chrono::seconds(1));
        return;
    }

    ainfo.status = new_status;
    blob value = dsn::json::json_forwarder<app_bulk_load_info>::encode(ainfo);

    _apps_pending_sync_flag[app_id] = true;
    _meta_svc->get_meta_storage()->set_data(
        get_app_bulk_load_path(app_id),
        std::move(value),
        std::bind(&bulk_load_service::update_app_status_on_remote_storage_reply,
                  this,
                  ainfo,
                  old_status,
                  new_status,
                  should_send_request));
}

void bulk_load_service::update_app_status_on_remote_storage_reply(const app_bulk_load_info &ainfo,
                                                                  bulk_load_status::type old_status,
                                                                  bulk_load_status::type new_status,
                                                                  bool should_send_request)
{
    ddebug_f("update app({}) status from {} to {}",
             ainfo.app_name,
             dsn::enum_to_string(old_status),
             dsn::enum_to_string(new_status));

    int32_t app_id = ainfo.app_id;
    {
        zauto_write_lock l(_lock);
        _app_bulk_load_info[app_id] = ainfo;
        _apps_pending_sync_flag[app_id] = false;
        _apps_in_progress_count[app_id] = ainfo.partition_count;
    }

    if (new_status == bulk_load_status::BLS_INGESTING) {
        for (int i = 0; i < ainfo.partition_count; ++i) {
            // send ingestion request to primary
            tasking::enqueue(LPC_BULK_LOAD_INGESTION,
                             _meta_svc->tracker(),
                             std::bind(&bulk_load_service::partition_ingestion,
                                       this,
                                       ainfo.app_name,
                                       gpid(app_id, i)));
        }
    }

    if (new_status == bulk_load_status::BLS_FAILED ||
        new_status == bulk_load_status::BLS_DOWNLOADING ||
        new_status == bulk_load_status::BLS_PAUSING ||
        new_status == bulk_load_status::BLS_CANCELED) {
        for (int i = 0; i < ainfo.partition_count; ++i) {
            update_partition_status_on_remote_stroage(
                ainfo.app_name, gpid(app_id, i), new_status, should_send_request);
        }
    }
}

// ThreadPool: THREAD_POOL_DEFAULT
void bulk_load_service::partition_ingestion(const std::string &app_name, const gpid &pid)
{
    FAIL_POINT_INJECT_F("meta_bulk_load_partition_ingestion", [](dsn::string_view) {});

    rpc_address primary_addr;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
        // app not existed or not available now
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            dwarn_f("app(name={}, id={}) is not existed, set bulk load failed",
                    app_name,
                    pid.get_app_id());
            handle_app_unavailable(pid.get_app_id(), app_name);
            return;
        }
        primary_addr = app->partitions[pid.get_partition_index()].primary;
    }

    // pid primary is invalid
    if (primary_addr.is_invalid()) {
        dwarn_f(
            "app({}) partition({}) primary is invalid, try it later", app_name, pid.to_string());
        tasking::enqueue(LPC_BULK_LOAD_INGESTION,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_ingestion, this, app_name, pid),
                         pid.thread_hash(),
                         std::chrono::seconds(1));
        return;
    }

    if (is_partition_metadata_not_updated(pid)) {
        derror_f("app({}) doesn't have bulk load metadata, set bulk load failed", app_name);
        handle_bulk_load_failed(pid.get_app_id());
        return;
    }

    ingestion_request req;
    req.app_name = app_name;
    {
        zauto_read_lock l(_lock);
        req.metadata = _partition_bulk_load_info[pid].metadata;
    }
    message_ex *msg =
        dsn::message_ex::create_client_request(dsn::apps::RPC_RRDB_RRDB_BULK_LOAD, pid);
    dsn::marshall(msg, req);
    dsn::rpc_response_task_ptr rpc_callback = rpc::create_rpc_response_task(
        msg,
        _meta_svc->tracker(),
        [this, app_name, pid](error_code err, ingestion_response &&resp) {
            on_partition_ingestion_reply(err, std::move(resp), app_name, pid);
        });
    ddebug_f("send ingest request to replica server({}), app({}) partition({})",
             primary_addr.to_string(),
             app_name,
             pid.to_string());
    _meta_svc->send_request(msg, primary_addr, rpc_callback);
}

// ThreadPool: THREAD_POOL_DEFAULT
void bulk_load_service::on_partition_ingestion_reply(error_code err,
                                                     ingestion_response &&resp,
                                                     const std::string &app_name,
                                                     const gpid &pid)
{
    // if meet 2pc error, ingesting will rollback to downloading, no need to retry here
    if (err != ERR_OK) {
        derror_f("app({}) partition({}) ingestion files failed, error = {}",
                 app_name,
                 pid.to_string(),
                 err.to_string());
        tasking::enqueue(
            LPC_META_STATE_NORMAL,
            _meta_svc->tracker(),
            std::bind(
                &bulk_load_service::try_rollback_to_downloading, this, pid.get_app_id(), app_name));
        return;
    }

    if (resp.err == ERR_TRY_AGAIN && resp.rocksdb_error != 0) {
        derror_f("app({}) partition({}) ingestion files failed while empty write, rocksdb error = "
                 "{}, retry it later",
                 app_name,
                 pid.to_string(),
                 resp.rocksdb_error);
        tasking::enqueue(LPC_BULK_LOAD_INGESTION,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_ingestion, this, app_name, pid),
                         0,
                         std::chrono::milliseconds(10));
        return;
    }

    if (resp.err != ERR_OK || resp.rocksdb_error != 0) {
        derror_f("app({}) partition({}) failed to ingestion files, error = {}, rocksdb error = {}",
                 app_name,
                 pid.to_string(),
                 resp.err.to_string(),
                 resp.rocksdb_error);
        tasking::enqueue(
            LPC_META_STATE_NORMAL,
            _meta_svc->tracker(),
            std::bind(&bulk_load_service::handle_bulk_load_failed, this, pid.get_app_id()));
        return;
    }

    ddebug_f("app({}) partition({}) receive ingestion reply succeed", app_name, pid.to_string());
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::remove_bulk_load_dir(int32_t app_id, const std::string &app_name)
{
    std::string bulk_load_path = get_app_bulk_load_path(app_id);
    _meta_svc->get_meta_storage()->delete_node_recursively(
        std::move(bulk_load_path), [this, app_id, app_name, bulk_load_path]() {
            ddebug_f("remove app({}) bulk load dir {}", app_name, bulk_load_path);
            reset_local_bulk_load_states(app_id, app_name);
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::remove_bulk_load_dir(std::shared_ptr<app_state> app, bool need_set_app_flag)
{
    std::string bulk_load_path = get_app_bulk_load_path(app->app_id);
    _meta_svc->get_meta_storage()->delete_node_recursively(
        std::move(bulk_load_path), [this, app, need_set_app_flag, bulk_load_path]() {
            ddebug_f("remove app({}) bulk load dir {}", app->app_name, bulk_load_path);
            reset_local_bulk_load_states(app->app_id, app->app_name);
            if (need_set_app_flag) {
                update_app_is_bulk_loading(std::move(app), false);
            }
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::update_app_is_bulk_loading(std::shared_ptr<app_state> app,
                                                   bool is_bulk_loading)
{
    app_info info = *app;
    info.__set_is_bulk_loading(is_bulk_loading);

    blob value = dsn::json::json_forwarder<app_info>::encode(info);
    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), [app, is_bulk_loading, this]() {
            {
                zauto_write_lock l(app_lock());
                app->is_bulk_loading = is_bulk_loading;
            }
            ddebug_f("app({}) update app is_bulk_loading to {}", app->app_name, is_bulk_loading);
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::reset_local_bulk_load_states(int32_t app_id, const std::string &app_name)
{
    zauto_write_lock l(_lock);
    _app_bulk_load_info.erase(app_id);
    _apps_in_progress_count.erase(app_id);
    _apps_pending_sync_flag.erase(app_id);
    erase_map_elem_by_id(app_id, _partitions_pending_sync_flag);
    erase_map_elem_by_id(app_id, _partitions_bulk_load_state);
    erase_map_elem_by_id(app_id, _partition_bulk_load_info);
    erase_map_elem_by_id(app_id, _partitions_total_download_progress);
    erase_map_elem_by_id(app_id, _partitions_cleaned_up);
    _apps_cleaning_up.erase(app_id);
    _bulk_load_app_id.erase(app_id);
    ddebug_f("reset local app({}) bulk load context", app_name);
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::on_query_bulk_load_status(query_bulk_load_rpc rpc)
{
    const auto &request = rpc.request();
    const std::string &app_name = request.app_name;

    configuration_query_bulk_load_response &response = rpc.response();
    response.err = ERR_OK;
    response.app_name = app_name;

    int32_t app_id, partition_count, max_replica_count;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(app_name);

        if (app == nullptr) {
            derror_f("app({}) is not existed", app_name);
            response.err = ERR_OBJECT_NOT_FOUND;
            return;
        }

        if (app->status != app_status::AS_AVAILABLE) {
            derror_f("app({}) is not available", app_name);
            response.err = ERR_APP_DROPPED;
            return;
        }

        if (!app->is_bulk_loading) {
            dwarn_f("app({}) is not during bulk load", app_name);
            response.err = ERR_INVALID_STATE;
            return;
        }

        app_id = app->app_id;
        partition_count = app->partition_count;
        max_replica_count = app->max_replica_count;
    }

    {
        zauto_read_lock l(_lock);
        response.max_replica_count = max_replica_count;
        response.app_status = get_app_bulk_load_status_unlock(app_id);
        response.partitions_status.resize(partition_count);
        ddebug_f("query app({}) bulk_load_status({}) succeed",
                 app_name,
                 dsn::enum_to_string(response.app_status));

        // TODO(heyuchen): refactor it
        if (response.app_status == bulk_load_status::BLS_DOWNLOADING ||
            response.app_status == bulk_load_status::BLS_DOWNLOADED) {
            response.__isset.download_progresses = true;
            response.download_progresses.resize(partition_count);
        }

        if (response.app_status == bulk_load_status::BLS_SUCCEED ||
            response.app_status == bulk_load_status::BLS_FAILED ||
            response.app_status == bulk_load_status::BLS_CANCELED) {
            response.__isset.cleanup_flags = true;
            response.cleanup_flags.resize(partition_count);
        }

        for (auto iter = _partition_bulk_load_info.begin(); iter != _partition_bulk_load_info.end();
             iter++) {
            int idx = iter->first.get_partition_index();
            response.partitions_status[idx] = iter->second.status;
            if (response.__isset.download_progresses) {
                std::map<rpc_address, partition_download_progress> progress;
                std::map<rpc_address, partition_bulk_load_state> t =
                    _partitions_bulk_load_state[iter->first];
                for (auto it = t.begin(); it != t.end(); ++it) {
                    partition_download_progress p;
                    p.progress = it->second.download_progress;
                    p.status = it->second.download_status;
                    progress[it->first] = p;
                }
                response.download_progresses[idx] = progress;
            }
            if (response.__isset.cleanup_flags) {
                response.cleanup_flags[idx] = _partitions_cleaned_up[iter->first];
            }
        }
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::create_bulk_load_root_dir(error_code &err, task_tracker &tracker)
{
    blob value = blob();
    _meta_svc->get_remote_storage()->create_node(
        _bulk_load_root,
        LPC_META_CALLBACK,
        [this, &err, &tracker](error_code ec) {
            if (ERR_OK == ec || ERR_NODE_ALREADY_EXIST == ec) {
                ddebug_f("create bulk load root({}) succeed", _bulk_load_root);
                sync_apps_bulk_load_from_remote_stroage(err, tracker);
            } else if (ERR_TIMEOUT == ec) {
                dwarn_f("create bulk load root({}) failed, retry later", _bulk_load_root);
                tasking::enqueue(
                    LPC_META_STATE_NORMAL,
                    nullptr,
                    std::bind(&bulk_load_service::create_bulk_load_root_dir, this, err, tracker),
                    0,
                    std::chrono::milliseconds(1000));
            } else {
                err = ec;
                dfatal_f(
                    "create bulk load root({}) failed, error={}", _bulk_load_root, ec.to_string());
            }
        },
        value,
        &tracker);
}

// ThreadPool: THREAD_POOL_META_SERVER
void bulk_load_service::sync_apps_bulk_load_from_remote_stroage(error_code &err,
                                                                task_tracker &tracker)
{
    std::string path = _bulk_load_root;
    _meta_svc->get_remote_storage()->get_children(
        path,
        LPC_META_CALLBACK,
        [this, &err, &tracker](error_code ec, const std::vector<std::string> &children) {
            if (ec != ERR_OK) {
                derror_f("get path({}) children failed, err = {}", _bulk_load_root, ec.to_string());
                err = ec;
                return;
            }
            if (children.size() > 0) {
                ddebug_f("There are {} apps need to sync bulk load status", children.size());
                for (auto &elem : children) {
                    uint32_t app_id = boost::lexical_cast<uint32_t>(elem);
                    ddebug_f("start to sync app({}) bulk load status", app_id);
                    do_sync_app_bulk_load(app_id, err, tracker);
                }
            }
        },
        &tracker);
}

// ThreadPool: THREAD_POOL_META_SERVER
void bulk_load_service::do_sync_app_bulk_load(int32_t app_id,
                                              error_code &err,
                                              task_tracker &tracker)
{
    std::string app_path = get_app_bulk_load_path(app_id);
    _meta_svc->get_remote_storage()->get_data(
        app_path,
        LPC_META_CALLBACK,
        [this, app_id, app_path, &err, &tracker](error_code ec, const blob &value) {
            if (ec == ERR_OK) {
                app_bulk_load_info ainfo;
                dsn::json::json_forwarder<app_bulk_load_info>::decode(value, ainfo);
                {
                    zauto_write_lock l(_lock);
                    _bulk_load_app_id.insert(app_id);
                    _app_bulk_load_info[app_id] = ainfo;
                }
                sync_partitions_bulk_load_from_remote_stroage(
                    ainfo.app_id, ainfo.app_name, err, tracker);
            } else {
                derror_f("get app bulk load info from remote stroage failed, path = {}, err = {}",
                         app_path,
                         ec.to_string());
                err = ec;
            }
        },
        &tracker);
}

// ThreadPool: THREAD_POOL_META_SERVER
void bulk_load_service::sync_partitions_bulk_load_from_remote_stroage(int32_t app_id,
                                                                      const std::string &app_name,
                                                                      error_code &err,
                                                                      task_tracker &tracker)
{
    std::string app_path = get_app_bulk_load_path(app_id);
    _meta_svc->get_remote_storage()->get_children(
        app_path,
        LPC_META_CALLBACK,
        [this, app_path, app_id, app_name, &err, &tracker](
            error_code ec, const std::vector<std::string> &children) {
            if (ec != ERR_OK) {
                derror_f("get path({}) children failed, err = {}", app_path, ec.to_string());
                err = ec;
                return;
            }
            ddebug_f("app(name={},app_id={}) has {} partition bulk load info to be synced",
                     app_name,
                     app_id,
                     children.size());
            for (const auto &child_pidx : children) {
                uint32_t pidx = boost::lexical_cast<uint32_t>(child_pidx);
                std::string partition_path = get_partition_bulk_load_path(app_path, pidx);
                do_sync_partition_bulk_load(
                    gpid(app_id, pidx), app_name, partition_path, err, tracker);
            }
        },
        &tracker);
}

// ThreadPool: THREAD_POOL_META_SERVER
void bulk_load_service::do_sync_partition_bulk_load(const gpid &pid,
                                                    const std::string &app_name,
                                                    const std::string &partition_path,
                                                    error_code &err,
                                                    task_tracker &tracker)
{
    _meta_svc->get_remote_storage()->get_data(
        partition_path,
        LPC_META_CALLBACK,
        [this, pid, app_name, partition_path, &err](error_code ec, const blob &value) {
            if (ec == ERR_OK) {
                partition_bulk_load_info pinfo;
                dsn::json::json_forwarder<partition_bulk_load_info>::decode(value, pinfo);
                {
                    zauto_write_lock l(_lock);
                    _partition_bulk_load_info[pid] = pinfo;
                }
            } else {
                derror_f("get app({}) partition({}) bulk load bulk from remote stroage failed, "
                         "path={}, err={}",
                         app_name,
                         pid.to_string(),
                         partition_path,
                         ec.to_string());
                err = ec;
            }
        },
        &tracker);
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::try_to_continue_bulk_load()
{
    FAIL_POINT_INJECT_F("meta_try_to_continue_bulk_load", [](dsn::string_view) {});

    // TODO(heyuchen): consider add lock here
    std::unordered_set<int32_t> bulk_load_app_id_set = _bulk_load_app_id;
    for (auto iter = bulk_load_app_id_set.begin(); iter != bulk_load_app_id_set.end(); ++iter) {
        int32_t app_id = *iter;
        app_bulk_load_info ainfo = _app_bulk_load_info[app_id];
        // <pidx, partition_bulk_load_info>
        std::unordered_map<int32_t, partition_bulk_load_info> partition_bulk_load_info_map;
        for (auto it = _partition_bulk_load_info.begin(); it != _partition_bulk_load_info.end();
             ++it) {
            if (it->first.get_app_id() == app_id) {
                partition_bulk_load_info_map[it->first.get_partition_index()] = it->second;
            }
        }
        continue_bulk_load(ainfo, partition_bulk_load_info_map);
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::continue_bulk_load(
    const app_bulk_load_info &ainfo,
    std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map)
{
    std::shared_ptr<app_state> app;
    {
        zauto_read_lock l(app_lock());
        app = _state->get_app(ainfo.app_name);
    }
    // if app is not available, remove bulk load dir
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        derror_f(
            "app(name={},app_id={}) is not existed or not available", ainfo.app_name, ainfo.app_id);
        if (app == nullptr) {
            remove_bulk_load_dir(ainfo.app_id, ainfo.app_name);
        } else {
            remove_bulk_load_dir(std::move(app), true);
        }
        return;
    }

    // if bulk load status is invalid, remove bulk load dir
    std::unordered_set<int32_t> different_status_pidx_set;
    if (!check_bulk_load_status(app->app_id,
                                app->partition_count,
                                ainfo,
                                partition_bulk_load_info_map,
                                different_status_pidx_set)) {
        remove_bulk_load_dir(std::move(app), true);
        return;
    }

    do_continue_bulk_load(ainfo, partition_bulk_load_info_map, different_status_pidx_set);
}

// ThreadPool: THREAD_POOL_META_STATE
/// meta bulk load failover cases
/// 1. lack children
///     1 - downloading
///         create all partition with downloading and send bulk load request
///     2 - other status with no children
///         remove dir and reset flag
/// 2. some partition has same status with app status, some not
///     1 - app:downloading + not existed
///         create not existed partition with downloading and send bulk load request
///     2 - app:downloading + downloaded
///         count = not downloading count, send bulk load request (app aim-> downloaded)
///     3 - app:downloaded + ingesting
///         count = downloaded count, send bulk load request (app aim -> ingesting)
///     4 - app:downloaded + not downloaded && not ingesting
///         remove dir and reset flag
///     5 - app:ingesting + finish
///         count = ingesting count, send bulk load request and ingestion request(app aim ->
///         ingesting)
///     6 - app:ingesting + not ingesting && not finish
///         remove dir and reset flag
///     7 - app:finish + not finish
///         remove dir and reset flag
///     8 - app:failed + not failed
///         set partition failed, send bulk load request (app aim cleanup)
///     9 - app:downloading + not downloading && not downloaded
///         count = not downloading count, send bulk load request
///     10- app:pausing + not pausing
///         set partition pausing, send bulk load request (app aim paused)
///     11- app:paused + not paused
///         remove dir and reset flag
///     12- app:cancel + not cancel
///         set partition cancel, send bulk load request (app aim cleanup)
/// 3. all child partition status isconsistency with app (count = partition count)
///     1 - downloading: send request
///     2 - downloaded: send request
///     3 - ingesting: send request and send ingestion
///     4 - finish: send request
///     5 - failed: send request
///     6 - pausing: send request
///     7 - paused: not send request
///     8 - cancel: send request
bool bulk_load_service::check_bulk_load_status(
    int32_t app_id,
    int32_t partition_count,
    const app_bulk_load_info &ainfo,
    std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map,
    std::unordered_set<int32_t> &different_status_pidx_set)
{
    // app info from bulk load dir is inconsistent with current app info
    if (app_id != ainfo.app_id || partition_count != ainfo.partition_count) {
        dwarn_f("app({}) has different app_id or partition_count, bulk load app_id = {}, "
                "partition_count = {}, current app_id={}, partition_count = {}",
                ainfo.app_name,
                ainfo.app_id,
                ainfo.partition_count,
                app_id,
                partition_count);
        return false;
    }

    // partition buld load dir count should not be greater than partition_count
    int partition_size = partition_bulk_load_info_map.size();
    if (partition_count < partition_size) {
        derror_f("app({}) has invalid count, app partition_count = {}, remote "
                 "partition_bulk_load_info count = {}",
                 ainfo.app_name,
                 partition_count,
                 partition_size);
        return false;
    }

    // partition buld load dir count is not equal to partition_count can only be happended when app
    // status is downlading
    if (partition_size != partition_count && ainfo.status != bulk_load_status::BLS_DOWNLOADING) {
        derror_f(
            "app({}) bulk_load_status={}, but there are {} partitions lack partition_bulk_load dir",
            ainfo.app_name,
            dsn::enum_to_string(ainfo.status),
            partition_count - partition_size);
        return false;
    }

    // check if partition bulk_load_status which is different from app bulk_load_status is valid
    return validate_partition_bulk_load_status(
        ainfo, partition_bulk_load_info_map, different_status_pidx_set);
}

// ThreadPool: THREAD_POOL_META_STATE
bool bulk_load_service::validate_partition_bulk_load_status(
    const app_bulk_load_info &ainfo,
    std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map,
    std::unordered_set<int32_t> &different_status_pidx_set)
{
    // `different_status_pidx_set` contains partition index whose bulk load status is not equal to
    // app's status
    bulk_load_status::type app_status = ainfo.status;
    for (auto iter = partition_bulk_load_info_map.begin();
         iter != partition_bulk_load_info_map.end();
         ++iter) {
        if (iter->second.status != app_status) {
            different_status_pidx_set.insert(iter->first);
        }
    }

    int32_t different_count = different_status_pidx_set.size();
    if (app_status == bulk_load_status::BLS_DOWNLOADING && different_count > 0 &&
        ainfo.partition_count - partition_bulk_load_info_map.size() > 0) {
        derror_f("app({}) bulk_load_status={}, {} partitions bulk_load_status is same "
                 "with app, {} partitions is different, there are {} partitions not "
                 "existed, this is invalid",
                 ainfo.app_name,
                 dsn::enum_to_string(app_status),
                 partition_bulk_load_info_map.size() - different_count,
                 different_count,
                 ainfo.partition_count - partition_bulk_load_info_map.size());
        return false;
    }

    // app: downloaded, valid partition: downloaded or ingesting
    // app: ingesting, valid partition: ingesting or finish
    if (app_status == bulk_load_status::BLS_DOWNLOADED ||
        app_status == bulk_load_status::BLS_INGESTING) {
        bulk_load_status::type valid_status = (app_status == bulk_load_status::BLS_DOWNLOADED)
                                                  ? bulk_load_status::BLS_INGESTING
                                                  : bulk_load_status::BLS_SUCCEED;
        ddebug_f("app({}) bulk_load_status={}, valid partition status may be {} or {}",
                 ainfo.app_name,
                 dsn::enum_to_string(app_status),
                 dsn::enum_to_string(app_status),
                 dsn::enum_to_string(valid_status));
        for (auto iter = different_status_pidx_set.begin(); iter != different_status_pidx_set.end();
             ++iter) {
            int32_t pidx = *iter;
            if (partition_bulk_load_info_map[pidx].status != valid_status) {
                derror_f("app({}) bulk_load_status={}, but partition[{}] "
                         "bulk_load_status={}, only {} and {} is valid",
                         ainfo.app_name,
                         app_status,
                         pidx,
                         dsn::enum_to_string(partition_bulk_load_info_map[pidx].status),
                         dsn::enum_to_string(app_status),
                         dsn::enum_to_string(valid_status));
                return false;
            }
        }
        return true;
    }

    // app: succeed, valid partition: succeed
    // app: paused, valid partition: paused
    if ((app_status == bulk_load_status::BLS_SUCCEED ||
         app_status == bulk_load_status::BLS_PAUSED) &&
        different_count > 0) {
        derror_f("app({}) bulk_load_status={}, {} partitions bulk_load_status is different from "
                 "app, this is invalid",
                 ainfo.app_name,
                 dsn::enum_to_string(app_status),
                 different_count);
        return false;
    }

    // app: downloading, failed, pausing, cancel, valid partition: all status
    return true;
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::do_continue_bulk_load(
    const app_bulk_load_info &ainfo,
    std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map,
    std::unordered_set<int32_t> &different_status_pidx_set)
{
    int32_t app_id = ainfo.app_id;
    int32_t partition_count = ainfo.partition_count;
    bulk_load_status::type app_status = ainfo.status;

    int32_t different_count = different_status_pidx_set.size();
    int32_t same_count = partition_bulk_load_info_map.size() - different_count;
    int32_t invalid_count = partition_count - partition_bulk_load_info_map.size();
    ddebug_f("app({}) continue bulk load, app_id={}, partition_count={}, status={}, there are {} "
             "partitions have bulk_load_info, {} partitions have same status with app, {} "
             "partitions different",
             ainfo.app_name,
             app_id,
             partition_count,
             dsn::enum_to_string(app_status),
             partition_bulk_load_info_map.size(),
             same_count,
             different_count);

    // calculate in_progress_partition_count
    int in_progress_partition_count = partition_count; // failed, pausing, paused, cancel
    if (app_status == bulk_load_status::BLS_DOWNLOADING) {
        if (invalid_count > 0) {
            in_progress_partition_count = invalid_count;
        } else if (different_count > 0) {
            in_progress_partition_count = different_count;
        }
    } else if (app_status == bulk_load_status::BLS_DOWNLOADED ||
               app_status == bulk_load_status::BLS_INGESTING ||
               app_status == bulk_load_status::BLS_SUCCEED) {
        in_progress_partition_count = same_count;
    }
    {
        zauto_write_lock l(_lock);
        _apps_in_progress_count[app_id] = in_progress_partition_count;
    }

    // if bulk load paused, return directly
    if (app_status == bulk_load_status::BLS_PAUSED) {
        ddebug_f("app({}) status = {}", ainfo.app_name, dsn::enum_to_string(app_status));
        return;
    }

    // if app status is downloading and all partition exist, set all partition status to downloading
    if ((app_status == bulk_load_status::BLS_FAILED ||
         app_status == bulk_load_status::BLS_CANCELED ||
         app_status == bulk_load_status::BLS_PAUSING ||
         (app_status == bulk_load_status::BLS_DOWNLOADING && invalid_count == 0)) &&
        different_count > 0) {
        for (auto iter = different_status_pidx_set.begin(); iter != different_status_pidx_set.end();
             ++iter) {
            int32_t pidx = *iter;
            update_partition_status_on_remote_stroage(
                ainfo.app_name, gpid(app_id, pidx), app_status);
        }
    }

    // send bulk_load_request to primary
    for (int i = 0; i < partition_count; ++i) {
        gpid pid = gpid(app_id, i);
        if (app_status == bulk_load_status::BLS_DOWNLOADING &&
            partition_bulk_load_info_map.find(i) == partition_bulk_load_info_map.end()) {
            // partition dir not exist on remote storage, firstly create it
            create_partition_bulk_load_dir(ainfo.app_name, pid, partition_count);
        } else if (app_status == bulk_load_status::BLS_DOWNLOADING ||
                   app_status == bulk_load_status::BLS_DOWNLOADED ||
                   app_status == bulk_load_status::BLS_INGESTING ||
                   app_status == bulk_load_status::BLS_SUCCEED ||
                   app_status == bulk_load_status::BLS_FAILED ||
                   app_status == bulk_load_status::BLS_CANCELED ||
                   app_status == bulk_load_status::BLS_PAUSING) {
            partition_bulk_load(ainfo.app_name, pid);
        }
        if (app_status == bulk_load_status::BLS_INGESTING) {
            tasking::enqueue(
                LPC_BULK_LOAD_INGESTION,
                _meta_svc->tracker(),
                std::bind(&bulk_load_service::partition_ingestion, this, ainfo.app_name, pid));
        }
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::create_partition_bulk_load_dir(const std::string &app_name,
                                                       const gpid &pid,
                                                       int32_t partition_count)
{
    partition_bulk_load_info pinfo;
    pinfo.status = bulk_load_status::BLS_DOWNLOADING;
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->create_node(
        get_partition_bulk_load_path(pid),
        std::move(value),
        [app_name, pid, partition_count, pinfo, this]() {
            dinfo_f("app({}) create partition({}) bulk_load_info", app_name, pid.to_string());
            {
                zauto_write_lock l(_lock);
                --_apps_in_progress_count[pid.get_app_id()];
                _partition_bulk_load_info[pid] = pinfo;

                if (_apps_in_progress_count[pid.get_app_id()] == 0) {
                    ddebug_f("app({}) restart bulk load succeed", app_name);
                    _apps_in_progress_count[pid.get_app_id()] = partition_count;
                }
            }
            // start send bulk load to replica servers
            partition_bulk_load(app_name, pid);
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::check_app_bulk_load_consistency(std::shared_ptr<app_state> app,
                                                        bool is_app_bulk_loading)
{
    std::string app_path = get_app_bulk_load_path(app->app_id);
    _meta_svc->get_remote_storage()->node_exist(
        app_path, LPC_META_STATE_HIGH, [this, app_path, app, is_app_bulk_loading](error_code err) {
            if (err == ERR_TIMEOUT) {
                ddebug_f(
                    "check app({}) bulk load dir({}) timeout, try later", app->app_name, app_path);
                tasking::enqueue(LPC_META_STATE_HIGH,
                                 nullptr,
                                 std::bind(&bulk_load_service::check_app_bulk_load_consistency,
                                           this,
                                           app,
                                           is_app_bulk_loading),
                                 0,
                                 std::chrono::seconds(1));
                return;
            } else {
                if (err != ERR_OK && is_app_bulk_loading) {
                    dwarn_f("app({}): bulk load dir({}) not exist, but is_bulk_loading = {}, reset "
                            "app is_bulk_loading flag",
                            app->app_name,
                            app_path,
                            is_app_bulk_loading);
                    update_app_is_bulk_loading(std::move(app), false);
                    return;
                }
                if (err == ERR_OK && !is_app_bulk_loading) {
                    dwarn_f("app({}): bulk load dir({}) exist, but is_bulk_loading={}, remove "
                            "useless bulk load dir",
                            app->app_name,
                            app_path,
                            is_app_bulk_loading);
                    remove_bulk_load_dir(std::move(app), false);
                    return;
                }
            }
        });
}

// ThreadPool: THREAD_POOL_META_STATE
void bulk_load_service::on_control_bulk_load(control_bulk_load_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    {
        zauto_read_lock l(app_lock());

        app = _state->get_app(request.app_id);
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
            response.msg = fmt::format("app({}) is not existed or not available", request.app_id);
            return;
        }

        if (!app->is_bulk_loading) {
            response.err = ERR_INACTIVE_STATE;
            response.msg = fmt::format("app({}) is not executing bulk load", app->app_name);
            return;
        }
    }

    zauto_write_lock l(_lock);
    bulk_load_status::type local_status = get_app_bulk_load_status_unlock(request.app_id);

    switch (request.type) {
    case bulk_load_control_type::BLC_PAUSE: {
        if (local_status != bulk_load_status::BLS_DOWNLOADING) {
            set_control_response(local_status, app->app_name, response);
            return;
        }
        ddebug_f("app({}) start to pause bulk load", app->app_name);
        update_app_status_on_remote_storage_unlock(request.app_id,
                                                   bulk_load_status::type::BLS_PAUSING);
    } break;
    case bulk_load_control_type::BLC_RESTART: {
        if (local_status != bulk_load_status::BLS_PAUSED) {
            set_control_response(local_status, app->app_name, response);
            return;
        }
        ddebug_f("app({}) restart bulk load", app->app_name);
        update_app_status_on_remote_storage_unlock(
            request.app_id, bulk_load_status::type::BLS_DOWNLOADING, true);
    } break;
    case bulk_load_control_type::BLC_CANCEL:
        if (local_status != bulk_load_status::BLS_DOWNLOADING &&
            local_status != bulk_load_status::BLS_PAUSED) {
            set_control_response(local_status, app->app_name, response);
            return;
        }
    case bulk_load_control_type::BLC_FORCE_CANCEL: {
        ddebug_f("app({}) start to {} cancel bulk load, original status = {}",
                 app->app_name,
                 request.type == bulk_load_control_type::BLC_FORCE_CANCEL ? "force" : "",
                 dsn::enum_to_string(local_status));
        update_app_status_on_remote_storage_unlock(request.app_id,
                                                   bulk_load_status::type::BLS_CANCELED,
                                                   local_status == bulk_load_status::BLS_PAUSED);
    } break;
    default:
        break;
    }
}

void bulk_load_service::set_control_response(bulk_load_status::type local_status,
                                             const std::string &app_name,
                                             configuration_control_bulk_load_response &resp)
{
    resp.err = ERR_INVALID_STATE;
    resp.msg = fmt::format("app({}) status={}", app_name, dsn::enum_to_string(local_status));
}

} // namespace replication
} // namespace dsn
