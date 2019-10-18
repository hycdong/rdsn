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
            return;
        }

        if (app->is_bulk_loading) {
            derror_f("app({}) is already executing bulk load, please wait", app->app_name);
            response.err = ERR_BUSY;
            return;
        }
    }

    error_code e = check_bulk_load_request_params(request.app_name,
                                                  request.cluster_name,
                                                  request.file_provider_type,
                                                  app->app_id,
                                                  app->partition_count);
    if (e != ERR_OK) {
        response.err = e;
        return;
    }

    ddebug_f("app({}) start bulk load, cluster_name = {}, provider = {}",
             request.app_name,
             request.cluster_name,
             request.file_provider_type);

    // avoid possible load balancing
    _meta_svc->set_function_level(meta_function_level::fl_steady);

    start_bulk_load_on_remote_storage(std::move(app), std::move(rpc));
}

// - ERR_OK: pass params check
// - ERR_INVALID_PARAMETERS: wrong file_provider type
// - ERR_FILE_OPERATION_FAILED: file_provider error
// - ERR_OBJECT_NOT_FOUNT: bulk_load_info not exist, may wrong cluster_name or app_name
// - ERR_INCOMPLETE_DATA: bulk_load_info is damaged on file_provider
// - ERR_INCONSISTENT_STATE: app_id or partition_count inconsistent
error_code bulk_load_service::check_bulk_load_request_params(const std::string &app_name,
                                                             const std::string &cluster_name,
                                                             const std::string &file_provider,
                                                             const int32_t app_id,
                                                             const int32_t partition_count)
{
    FAIL_POINT_INJECT_F("meta_check_bulk_load_request_params",
                        [](dsn::string_view) -> error_code { return ERR_OK; });
    FAIL_POINT_INJECT_F("meta_check_bulk_load_request_params_file_failed",
                        [](dsn::string_view) -> error_code { return ERR_FILE_OPERATION_FAILED; });
    FAIL_POINT_INJECT_F(
        "meta_check_bulk_load_request_app_info_failed", [=](dsn::string_view) -> error_code {
            int32_t invalid_app_id = 0;
            int32_t invalid_partition_count = 9;
            if (app_id != invalid_app_id || partition_count != invalid_partition_count) {
                derror_f(
                    "app({}) information is inconsistent, local app_id({}) VS remote app_id({}), "
                    "local partition_count({}) VS remote partition_count({})",
                    app_name,
                    app_id,
                    invalid_app_id,
                    partition_count,
                    invalid_partition_count);
                return ERR_INCONSISTENT_STATE;
            }
            return ERR_OK;
        });

    // check file provider
    dsn::dist::block_service::block_filesystem *blk_fs =
        _meta_svc->get_block_service_manager().get_block_filesystem(file_provider);
    if (blk_fs == nullptr) {
        derror_f("invalid remote file provider type: {}", file_provider);
        return ERR_INVALID_PARAMETERS;
    }

    // sync get bulk_load_info file_handler
    std::string remote_path = get_bulk_load_info_path(app_name, cluster_name);
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
        return r_resp.err;
    }

    bulk_load_info bl_info;
    if (!::dsn::json::json_forwarder<bulk_load_info>::decode(r_resp.buffer, bl_info)) {
        derror_f("file({}) is damaged on remote file provider({})", remote_path, file_provider);
        return ERR_INCOMPLETE_DATA;
    }

    if (bl_info.app_id != app_id || bl_info.partition_count != partition_count) {
        derror_f("app({}) information is inconsistent, local app_id({}) VS remote app_id({}), "
                 "local partition_count({}) VS remote partition_count({})",
                 app_name,
                 app_id,
                 bl_info.app_id,
                 partition_count,
                 bl_info.partition_count);
        return ERR_INCONSISTENT_STATE;
    }

    return ERR_OK;
}

void bulk_load_service::start_bulk_load_on_remote_storage(std::shared_ptr<app_state> app,
                                                          start_bulk_load_rpc rpc)
{
    app_info info = *app;
    info.is_bulk_loading = true;

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
            create_app_bulk_load_dir_with_rpc(std::move(app), std::move(rpc));
        });
}

void bulk_load_service::create_app_bulk_load_dir_with_rpc(std::shared_ptr<app_state> app,
                                                          start_bulk_load_rpc rpc)
{
    std::string app_path = get_app_bulk_load_path(app->app_id);
    const auto req = rpc.request();

    app_bulk_load_info ainfo;
    ainfo.app_id = app->app_id;
    ainfo.app_name = app->app_name;
    ainfo.partition_count = app->partition_count;
    ainfo.status = bulk_load_status::BLS_DOWNLOADING;
    ainfo.cluster_name = req.cluster_name;
    ainfo.file_provider_type = req.file_provider_type;
    {
        zauto_write_lock l(_lock);
        _app_bulk_load_info[ainfo.app_id] = ainfo;
    }
    blob value = dsn::json::json_forwarder<app_bulk_load_info>::encode(ainfo);

    _meta_svc->get_meta_storage()->create_node(
        std::move(app_path), std::move(value), [app, rpc, app_path, this]() {
            dinfo_f("create app({}) bulk load dir", app->app_name);
            for (int i = 0; i < app->partition_count; ++i) {
                create_partition_bulk_load_dir_with_rpc(app->app_name,
                                                        gpid(app->app_id, i),
                                                        app->partition_count,
                                                        app_path,
                                                        std::move(rpc));
            }
        });
}

void bulk_load_service::create_partition_bulk_load_dir_with_rpc(const std::string &app_name,
                                                                const gpid &pid,
                                                                const int32_t partition_count,
                                                                const std::string &app_path,
                                                                start_bulk_load_rpc rpc)
{
    partition_bulk_load_info pinfo;
    pinfo.status = bulk_load_status::BLS_DOWNLOADING;
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->create_node(
        get_partition_bulk_load_path(app_path, pid.get_partition_index()),
        std::move(value),
        [app_name, pid, partition_count, app_path, rpc, pinfo, this]() {
            dinfo_f("app({}) create partition({}) bulk_load_info", app_name, pid.to_string());
            {
                zauto_write_lock l(_lock);
                _partition_bulk_load_info[pid] = pinfo;
                if (--_apps_in_progress_count[pid.get_app_id()] == 0) {
                    ddebug_f("app({}) start bulk load succeed", app_name);
                    _apps_in_progress_count[pid.get_app_id()] = partition_count;
                    auto response = rpc.response();
                    response.err = ERR_OK;
                }
            }
            // start send bulk load to replica servers
            partition_bulk_load(pid);
        });
}

void bulk_load_service::partition_bulk_load(const gpid &pid)
{
    FAIL_POINT_INJECT_F("meta_bulk_load_partition_bulk_load", [](dsn::string_view) {});

    rpc_address primary_addr;
    std::string app_name;
    ballot b;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
        // app not existed or not available now
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            dwarn_f("app(id={}) is not existed, set bulk load finish", pid.get_app_id());
            // TODO(heyuchen): handler it
            return;
        }
        app_name = app->app_name;
        primary_addr = app->partitions[pid.get_partition_index()].primary;
        b = app->partitions[pid.get_partition_index()].ballot;
    }

    // pid primary is invalid
    if (primary_addr.is_invalid()) {
        dwarn_f(
            "app({}) partition({}) primary is invalid, try it later", app_name, pid.to_string());
        tasking::enqueue(LPC_META_CALLBACK,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_bulk_load, this, pid),
                         0,
                         std::chrono::seconds(1));
        return;
    }

    partition_bulk_load_info pbl_info;
    app_bulk_load_info ainfo;
    {
        zauto_read_lock l(_lock);
        pbl_info = _partition_bulk_load_info[pid];
        ainfo = _app_bulk_load_info[pid.get_app_id()];
    }

    bulk_load_request req;
    req.pid = pid;
    req.app_name = app_name;
    req.primary_addr = primary_addr;
    req.app_bulk_load_status = ainfo.status;
    req.remote_provider_name = ainfo.file_provider_type;
    req.cluster_name = ainfo.cluster_name;
    req.partition_bulk_load_status = pbl_info.status;
    req.ballot = b;

    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_BULK_LOAD, 0, pid.thread_hash());
    dsn::marshall(msg, req);
    dsn::rpc_response_task_ptr rpc_callback = rpc::create_rpc_response_task(
        msg,
        _meta_svc->tracker(),
        [this, req, pid, primary_addr](error_code err, bulk_load_response &&resp) {
            on_partition_bulk_load_reply(err, req.ballot, std::move(resp), pid, primary_addr);
        });

    zauto_write_lock l(_lock);
    _partitions_request[pid] = rpc_callback;

    ddebug_f("send bulk load request to replica server({}), app({}), partition({}), app status = "
             "{}, partition status = {}, remote provider = {}, cluster_name = {}",
             primary_addr.to_string(),
             app_name,
             pid.to_string(),
             enum_to_string(req.app_bulk_load_status),
             enum_to_string(pbl_info.status),
             req.remote_provider_name,
             req.cluster_name);
    _meta_svc->send_request(msg, primary_addr, rpc_callback);
}

void bulk_load_service::on_partition_bulk_load_reply(error_code err,
                                                     ballot req_ballot,
                                                     bulk_load_response &&response,
                                                     const gpid &pid,
                                                     const rpc_address &primary_addr)
{
    if (err != ERR_OK) {
        dwarn_f("app({}), partition({}) failed to recevie bulk load response, error = {}",
                pid.get_app_id(),
                pid.to_string(),
                err.to_string());
        rollback_to_downloading(pid.get_app_id());
        return;
    } else if (response.err == ERR_OBJECT_NOT_FOUND || response.err == ERR_INVALID_STATE) {
        dwarn_f("app({}), partition({}) doesn't exist or has invalid state on node({}), error = {}",
                response.app_name,
                pid.to_string(),
                primary_addr.to_string(),
                response.err.to_string());
        rollback_to_downloading(pid.get_app_id());
        return;
    } else if (response.err != ERR_OK) {
        // TODO(heyuchen): add bulk load status check, not only downloading error handler below
        if (response.primary_bulk_load_status == bulk_load_status::BLS_DOWNLOADING) {
            if (response.err == ERR_CORRUPTION) {
                derror_f("app({}), partition({}) failed to download files from remote provider, "
                         "because files are damaged, error = {}",
                         response.app_name,
                         pid.to_string(),
                         response.err.to_string());
            } else {
                dwarn_f("app({}), partition({}) failed to download files from remote provider, "
                        "because file system error, error = {}",
                        response.app_name,
                        pid.to_string(),
                        response.err.to_string());
            }
            handle_partition_download_error(pid);
        }
    } else {
        ballot current_ballot;
        {
            zauto_read_lock l(app_lock());
            std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
            // app not existed or not available now
            if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
                dwarn_f("app(id={}) is not existed, set bulk load finish", pid.get_app_id());
                // TODO(heyuchen): handler it
                return;
            }
            current_ballot = app->partitions[pid.get_partition_index()].ballot;
        }

        if (req_ballot < current_ballot) {
            dwarn_f("receive out-date response, app({}), partition({}), request ballot = {}, "
                    "current ballot= {}",
                    response.app_name,
                    pid.to_string(),
                    req_ballot,
                    current_ballot);
            rollback_to_downloading(pid.get_app_id());
            return;
        }

        // do bulk load
        bulk_load_status::type app_status = get_app_bulk_load_status(response.pid.get_app_id());
        if (app_status == bulk_load_status::BLS_DOWNLOADING) {
            handle_app_bulk_load_downloading(response, primary_addr);
        } else if (app_status == bulk_load_status::BLS_DOWNLOADED) {
            handle_app_bulk_load_downloaded(response, primary_addr);
        } else if (app_status == bulk_load_status::BLS_FINISH ||
                   app_status == bulk_load_status::BLS_FAILED) {
            handle_app_bulk_load_cleanup(response, primary_addr);
        } else {
            // do nothing if during ingesting
        }
    }

    if (is_app_bulk_loading(pid.get_app_id())) {
        // TODO(heyuchen): delay time to config
        tasking::enqueue(LPC_META_CALLBACK,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_bulk_load, this, pid),
                         0,
                         std::chrono::seconds(10));
    }
}

void bulk_load_service::rollback_to_downloading(int32_t app_id)
{
    zauto_write_lock l(_lock);
    update_app_bulk_load_status_unlock(app_id, bulk_load_status::type::BLS_DOWNLOADING);
}

void bulk_load_service::handle_app_bulk_load_downloading(const bulk_load_response &response,
                                                         const rpc_address &primary_addr)
{
    std::string app_name = response.app_name;
    gpid pid = response.pid;

    error_code ec = check_download_status(response);
    if (ec != ERR_OK) {
        dwarn_f("recevie bulk load response from {} app({}), partition({}), primary status = {}, "
                "download meet error: {}",
                primary_addr.to_string(),
                response.app_name,
                pid.to_string(),
                enum_to_string(response.primary_bulk_load_status),
                ec.to_string());
        handle_partition_download_error(pid);
        return;
    }

    // download sst files succeed
    int32_t total_progress =
        response.__isset.total_download_progress ? response.total_download_progress : 0;
    ddebug_f("recevie bulk load response from node({}) app({}) partition({}), primary status = {}, "
             "total_download_progress = {}",
             primary_addr.to_string(),
             response.app_name,
             pid.to_string(),
             enum_to_string(response.primary_bulk_load_status),
             total_progress);
    {
        zauto_write_lock l(_lock);
        _partitions_total_download_progress[pid] = total_progress;
        if (response.__isset.download_progresses) {
            _partitions_download_progress[pid] = response.download_progresses;
        }
    }

    // TODO(heyuchen): change it to common value
    int32_t max_progress = 100;
    if (total_progress >= max_progress) {
        ddebug_f("app({}) partirion({}) download files from remote provider succeed",
                 app_name,
                 pid.to_string());
        {
            zauto_read_lock l(app_lock());
            std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
            if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
                dwarn_f("app({}) is not existed, set bulk load finish", app_name);
                // TODO(heyuchen): handler it
                return;
            }

            std::string partition_bulk_load_path = get_partition_bulk_load_path(
                get_app_bulk_load_path(pid.get_app_id()), pid.get_partition_index());
            update_partition_bulk_load_status(
                app->app_name, pid, partition_bulk_load_path, bulk_load_status::BLS_DOWNLOADED);
        }
    }
}

void bulk_load_service::handle_app_bulk_load_cleanup(const bulk_load_response &response,
                                                     const rpc_address &primary_addr)
{
    gpid pid = response.pid;
    if (!response.__isset.is_group_bulk_load_context_cleaned) {
        dwarn_f("recevie bulk load response from node({}) app({}) partition({}), primary status = "
                "{}, but "
                "not checking cleanup",
                primary_addr.to_string(),
                response.app_name,
                pid.to_string(),
                enum_to_string(response.primary_bulk_load_status));
        return;
    }

    bool all_clean_up = response.is_group_bulk_load_context_cleaned;
    ddebug_f("recevie bulk load response from node({}) app({}) partition({}), primary status = {}, "
             "group_bulk_load_context_cleaned = {}",
             primary_addr.to_string(),
             response.app_name,
             pid.to_string(),
             enum_to_string(response.primary_bulk_load_status),
             all_clean_up);
    {
        zauto_write_lock l(_lock);
        _partitions_cleaned_up[pid] = all_clean_up;
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
                    dwarn_f("app({}) is not existed, set bulk load finish", response.app_name);
                    // TODO(heyuchen): handler it
                    return;
                }
            }
            ddebug_f("app({}) all partitions cleanup bulk load context", response.app_name);
            remove_app_bulk_load_dir(std::move(app), true);
        }
    }
}

void bulk_load_service::handle_partition_download_error(const gpid &pid)
{
    zauto_write_lock l(_lock);
    if (!_apps_cleaning_up[pid.get_app_id()]) {
        _apps_cleaning_up[pid.get_app_id()] = true;
        update_app_bulk_load_status_unlock(pid.get_app_id(), bulk_load_status::BLS_FAILED);
    }
}

error_code bulk_load_service::check_download_status(const bulk_load_response &response)
{
    // download_progresses filed is not set
    if (!response.__isset.download_progresses) {
        return ERR_INVALID_STATE;
    }

    for (auto iter = response.download_progresses.begin();
         iter != response.download_progresses.end();
         ++iter) {
        partition_download_progress progress = iter->second;
        if (progress.status != ERR_OK) {
            dwarn_f("app({}) partition({}) meet error during downloading sst files, address = {}, "
                    "error = {}",
                    response.app_name,
                    progress.pid.get_app_id(),
                    progress.pid.get_partition_index(),
                    iter->first.to_string(),
                    progress.status.to_string());
            return progress.status;
        }
    }
    return ERR_OK;
}

void bulk_load_service::update_partition_bulk_load_status(const std::string &app_name,
                                                          const gpid &pid,
                                                          std::string &path,
                                                          bulk_load_status::type status,
                                                          bool send_request)
{
    zauto_read_lock l(_lock);
    partition_bulk_load_info pinfo = _partition_bulk_load_info[pid];
    if (pinfo.status == status) {
        dwarn_f("app({}) partition({}) old status:{} VS new status:{}, ignore it",
                app_name,
                pid.to_string(),
                enum_to_string(pinfo.status),
                enum_to_string(status));
        return;
    }
    pinfo.status = status;
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->set_data(
        std::move(path), std::move(value), [this, app_name, pid, path, status, send_request]() {
            zauto_write_lock l(_lock);
            {
                bulk_load_status::type old_status = _partition_bulk_load_info[pid].status;
                ddebug_f("app({}) update partition({}) status from {} to {}",
                         app_name,
                         pid.to_string(),
                         enum_to_string(old_status),
                         enum_to_string(status));

                _partition_bulk_load_info[pid].status = status;

                if (status == bulk_load_status::BLS_DOWNLOADED) {
                    _partitions_request[pid] = nullptr;
                    if (--_apps_in_progress_count[pid.get_app_id()] == 0) {
                        update_app_bulk_load_status_unlock(pid.get_app_id(), status);
                    }
                } else if (status == bulk_load_status::BLS_INGESTING ||
                           status == bulk_load_status::BLS_FINISH) {
                    if (--_apps_in_progress_count[pid.get_app_id()] == 0) {
                        update_app_bulk_load_status_unlock(pid.get_app_id(), status);
                    }
                } else if (status == bulk_load_status::BLS_DOWNLOADING) {
                    if (--_apps_in_progress_count[pid.get_app_id()] == 0) {
                        _apps_in_progress_count[pid.get_app_id()] =
                            _app_bulk_load_info[pid.get_app_id()].partition_count;
                        ddebug_f("app({}) restart to bulk load", app_name);
                    }
                } else if (status == bulk_load_status::BLS_FAILED) {
                    // do nothing
                }
            }
            if (status == bulk_load_status::BLS_DOWNLOADING && send_request) {
                partition_bulk_load(pid);
            }
        });
}

void bulk_load_service::update_app_bulk_load_status_unlock(int32_t app_id,
                                                           bulk_load_status::type new_status)
{
    app_bulk_load_info ainfo = _app_bulk_load_info[app_id];
    auto old_status = ainfo.status;
    if (old_status == new_status) {
        dwarn_f("app({}) old status:{} VS new status:{}, ignore it",
                ainfo.app_name,
                enum_to_string(old_status),
                enum_to_string(new_status));
        return;
    }

    ainfo.status = new_status;
    blob value = dsn::json::json_forwarder<app_bulk_load_info>::encode(ainfo);
    auto callback = [this, old_status, ainfo, app_id, new_status]() {
        ddebug_f("update app({}) status from {} to {}",
                 ainfo.app_name,
                 enum_to_string(old_status),
                 enum_to_string(new_status));

        // TODO(heyuchen): add write lock here?
        _app_bulk_load_info[app_id] = ainfo;
        _apps_in_progress_count[app_id] = ainfo.partition_count;

        if (new_status == bulk_load_status::BLS_INGESTING) {
            for (int i = 0; i < ainfo.partition_count; ++i) {
                // send ingestion request to primary
                tasking::enqueue(
                    LPC_BULK_LOAD_INGESTION,
                    _meta_svc->tracker(),
                    std::bind(&bulk_load_service::partition_ingestion, this, gpid(app_id, i)));
            }
        }

        if (new_status == bulk_load_status::BLS_FAILED ||
            new_status == bulk_load_status::BLS_DOWNLOADING) {
            for (int i = 0; i < ainfo.partition_count; ++i) {
                std::string path = get_partition_bulk_load_path(get_app_bulk_load_path(app_id), i);
                update_partition_bulk_load_status(
                    ainfo.app_name, gpid(app_id, i), path, new_status);
            }
        }
    };

    _meta_svc->get_meta_storage()->set_data(
        get_app_bulk_load_path(app_id), std::move(value), std::move(callback));
}

void bulk_load_service::remove_app_bulk_load_dir(int32_t app_id, const std::string &app_name)
{
    std::string bulk_load_path = get_app_bulk_load_path(app_id);
    _meta_svc->get_meta_storage()->delete_node_recursively(
        std::move(bulk_load_path), [this, app_id, app_name, bulk_load_path]() {
            ddebug_f("remove app({}) bulk load dir {}", app_name, bulk_load_path);
            clear_app_bulk_load_states(app_id, app_name);
        });
}

void bulk_load_service::remove_app_bulk_load_dir(std::shared_ptr<app_state> app,
                                                 bool need_set_app_flag)
{
    std::string bulk_load_path = get_app_bulk_load_path(app->app_id);
    _meta_svc->get_meta_storage()->delete_node_recursively(
        std::move(bulk_load_path), [this, app, need_set_app_flag, bulk_load_path]() {
            ddebug_f("remove app({}) bulk load dir {}", app->app_name, bulk_load_path);
            clear_app_bulk_load_states(app->app_id, app->app_name);
            if (need_set_app_flag) {
                update_app_bulk_load_flag(std::move(app), false);
            }
        });
}

void bulk_load_service::update_app_bulk_load_flag(std::shared_ptr<app_state> app,
                                                  bool is_bulk_loading)
{
    app_info info = *app;
    info.is_bulk_loading = is_bulk_loading;

    blob value = dsn::json::json_forwarder<app_info>::encode(info);
    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), [app, is_bulk_loading, this]() {
            {
                zauto_write_lock l(app_lock());
                app->is_bulk_loading = is_bulk_loading;
            }
            ddebug_f("app({}) update app is_bulk_loading to {}", app->app_name, is_bulk_loading);
            if (!is_bulk_loading) {
                zauto_write_lock l(_lock);
                _bulk_load_app_id.erase(app->app_id);
            }
        });
}

void bulk_load_service::clear_app_bulk_load_states(int32_t app_id, const std::string &app_name)
{
    zauto_write_lock l(_lock);
    _app_bulk_load_info.erase(app_id);
    _apps_in_progress_count.erase(app_id);
    erase_map_elem_by_id(app_id, _partitions_download_progress);
    erase_map_elem_by_id(app_id, _partition_bulk_load_info);
    erase_map_elem_by_id(app_id, _partitions_request);
    erase_map_elem_by_id(app_id, _partitions_total_download_progress);
    erase_map_elem_by_id(app_id, _partitions_cleaned_up);
    _apps_cleaning_up.erase(app_id);
    ddebug_f("clear app({}) bulk load context", app_name);
}

template <typename T>
void bulk_load_service::erase_map_elem_by_id(int32_t app_id, std::unordered_map<gpid, T> &mymap)
{
    for (auto iter = mymap.begin(); iter != mymap.end();) {
        if (iter->first.get_app_id() == app_id) {
            mymap.erase(iter++);
        }
    }
}

void bulk_load_service::handle_app_bulk_load_downloaded(const bulk_load_response &response,
                                                        const rpc_address &primary_addr)
{
    gpid pid = response.pid;
    ddebug_f(
        "recevie bulk load response from node({}) app({}) partition({}), bulk load status = {}",
        primary_addr.to_string(),
        response.app_name,
        pid.to_string(),
        enum_to_string(response.primary_bulk_load_status));

    std::string partition_bulk_load_path = get_partition_bulk_load_path(
        get_app_bulk_load_path(pid.get_app_id()), pid.get_partition_index());
    update_partition_bulk_load_status(
        response.app_name, pid, partition_bulk_load_path, bulk_load_status::BLS_INGESTING);
}

void bulk_load_service::partition_ingestion(const gpid &pid)
{
    FAIL_POINT_INJECT_F("meta_bulk_load_partition_ingestion", [](dsn::string_view) {});

    rpc_address primary_addr;
    std::string app_name;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
        // app not existed or not available now
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            dwarn_f("app({}) is not existed, set bulk load finish", app->app_name);
            // TODO(heyuchen): handler it
            return;
        }
        app_name = app->app_name;
        primary_addr = app->partitions[pid.get_partition_index()].primary;
    }

    // pid primary is invalid
    if (primary_addr.is_invalid()) {
        dwarn_f(
            "app({}) partition({}) primary is invalid, try it later", app_name, pid.to_string());
        tasking::enqueue(LPC_BULK_LOAD_INGESTION,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_ingestion, this, pid),
                         pid.thread_hash(),
                         std::chrono::seconds(1));
        return;
    }

    ingestion_request req;
    req.app_name = app_name;
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

void bulk_load_service::on_partition_ingestion_reply(error_code err,
                                                     ingestion_response &&resp,
                                                     const std::string &app_name,
                                                     const gpid &pid)
{
    // TODO(heyuchen):consider!!!
    if (err != ERR_OK) {
        derror_f("app({}) partition({}) failed to ingestion files, error = {}",
                 app_name,
                 pid.to_string(),
                 err.to_string());
        tasking::enqueue(LPC_BULK_LOAD_INGESTION,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_ingestion, this, pid),
                         pid.thread_hash(),
                         std::chrono::seconds(1));
        return;
    }

    // TODO(heyuchen):consider!!!
    if (resp.error != 0) {
        derror_f("app({}) partition({}) failed to ingestion files, error = {}",
                 app_name,
                 pid.to_string(),
                 resp.error);
        tasking::enqueue(LPC_BULK_LOAD_INGESTION,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_ingestion, this, pid),
                         pid.thread_hash(),
                         std::chrono::seconds(1));
        return;
    }

    ddebug_f("app({}) partition({}) ingestion files succeed", app_name, pid.to_string());
    std::string partition_bulk_load_path = get_partition_bulk_load_path(
        get_app_bulk_load_path(pid.get_app_id()), pid.get_partition_index());
    update_partition_bulk_load_status(
        app_name, pid, partition_bulk_load_path, bulk_load_status::BLS_FINISH);
}

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
        app_id = app->app_id;
        partition_count = app->partition_count;
        max_replica_count = app->max_replica_count;

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
    }

    {
        zauto_read_lock l(_lock);
        response.max_replica_count = max_replica_count;
        response.app_status = get_app_bulk_load_status(app_id);
        response.partitions_status.resize(partition_count);
        ddebug_f("query app({}) bulk_load_status({}) succeed",
                 app_name,
                 enum_to_string(response.app_status));

        if (response.app_status == bulk_load_status::BLS_DOWNLOADING ||
            response.app_status == bulk_load_status::BLS_DOWNLOADED) {
            response.__isset.download_progresses = true;
            response.download_progresses.resize(partition_count);
        }

        for (auto iter = _partition_bulk_load_info.begin(); iter != _partition_bulk_load_info.end();
             iter++) {
            int idx = iter->first.get_partition_index();
            response.partitions_status[idx] = iter->second.status;
            if (response.__isset.download_progresses) {
                response.download_progresses[idx] = _partitions_download_progress[iter->first];
            }
        }
    }
}

void bulk_load_service::create_bulk_load_root_dir(error_code &err, task_tracker &tracker)
{
    blob value = blob();
    _meta_svc->get_remote_storage()->create_node(
        _bulk_load_root,
        LPC_META_CALLBACK,
        [this, &err, &tracker](error_code ec) {
            if (ERR_OK == ec || ERR_NODE_ALREADY_EXIST == ec) {
                ddebug_f("create bulk load root({}) succeed", _bulk_load_root);
                sync_apps_bulk_load(err, tracker);
            } else if (ERR_TIMEOUT == ec) {
                dwarn_f("create bulk load root({}) failed, retry later", _bulk_load_root);
                tasking::enqueue(
                    LPC_META_CALLBACK,
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

void bulk_load_service::sync_apps_bulk_load(error_code &err, task_tracker &tracker)
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

void bulk_load_service::do_sync_app_bulk_load(int32_t app_id,
                                              error_code &err,
                                              task_tracker &tracker)
{
    std::string app_path = get_app_bulk_load_path(app_id);
    // get app_bulk_load_info
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
                sync_partitions_bulk_load(ainfo.app_id, ainfo.app_name, err, tracker);
            } else {
                derror_f("get app bulk load info from remote stroage failed, path = {}, err = {}",
                         app_path,
                         ec.to_string());
                err = ec;
            }
        },
        &tracker);
}

void bulk_load_service::sync_partitions_bulk_load(int32_t app_id,
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
            // sync_partitions_bulk_load
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

void bulk_load_service::try_to_continue_bulk_load()
{
    FAIL_POINT_INJECT_F("meta_try_to_continue_bulk_load", [](dsn::string_view) {});

    std::unordered_set<int32_t> bulk_load_app_id_set = _bulk_load_app_id;
    for (auto iter = bulk_load_app_id_set.begin(); iter != bulk_load_app_id_set.end(); ++iter) {
        int32_t app_id = *iter;
        app_bulk_load_info ainfo = _app_bulk_load_info[app_id];
        std::unordered_map<int32_t, partition_bulk_load_info> partition_bulk_load_info_map;
        get_partition_bulk_load_info_by_app_id(app_id, partition_bulk_load_info_map);
        try_to_continue_app_bulk_load(ainfo, partition_bulk_load_info_map);
    }
}

void bulk_load_service::get_partition_bulk_load_info_by_app_id(
    int32_t app_id,
    std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map)
{
    for (auto iter = _partition_bulk_load_info.begin(); iter != _partition_bulk_load_info.end();
         ++iter) {
        if (iter->first.get_app_id() == app_id) {
            partition_bulk_load_info_map[iter->first.get_partition_index()] = iter->second;
        }
    }
}

void bulk_load_service::try_to_continue_app_bulk_load(
    const app_bulk_load_info &ainfo,
    std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map)
{
    std::shared_ptr<app_state> app;
    {
        zauto_read_lock l(app_lock());
        app = _state->get_app(ainfo.app_name);
    }
    // app is not available here
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        derror_f(
            "app(name={},app_id={}) is not existed or not available", ainfo.app_name, ainfo.app_id);
        if (app == nullptr) {
            remove_app_bulk_load_dir(ainfo.app_id, ainfo.app_name);
        } else {
            remove_app_bulk_load_dir(std::move(app), true);
        }
        return;
    }

    std::unordered_set<int32_t> different_status_pidx_set;
    if (!check_continue_bulk_load(app->app_id,
                                  app->partition_count,
                                  ainfo,
                                  partition_bulk_load_info_map,
                                  different_status_pidx_set)) {
        remove_app_bulk_load_dir(std::move(app), true);
        return;
    }

    continue_app_bulk_load(ainfo, partition_bulk_load_info_map, different_status_pidx_set);
}

/// meta bulk load failover cases
/// 1. no children
///     1 - downloading
///         create all partition with downloading and send bulk load request
///     2 - donwloaded/ingesting/finish + no children
///         remove dir and reset flag
///     3 - failed + no children
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
/// 3. all child partition status isconsistency with app (count = partition count)
///     1 - downloading: send request
///     2 - downloaded: send request
///     3 - ingesting: send request and send ingestion
///     4 - finish: send request
///     5 - failed: send request
///
bool bulk_load_service::check_continue_bulk_load(
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
    if (ainfo.partition_count < partition_size) {
        derror_f("app({}) has invalid count, app partition_count = {}, remote "
                 "partition_bulk_load_info count = {}",
                 ainfo.app_name,
                 ainfo.partition_count,
                 partition_size);
        return false;
    }

    // no children with not downloading(case1.2, 1.3)
    if (partition_size != ainfo.partition_count &&
        ainfo.status != bulk_load_status::BLS_DOWNLOADING) {
        derror_f("app({}) bulk_load_status={}, but there are no partition_bulk_load dir existed",
                 ainfo.app_name,
                 enum_to_string(ainfo.status));
        return false;
    }

    // check if partition bulk_load_status which is different from app bulk_load_status is valid
    return validate_partition_bulk_load_status(
        ainfo, partition_bulk_load_info_map, different_status_pidx_set);
}

bool bulk_load_service::validate_partition_bulk_load_status(
    const app_bulk_load_info &ainfo,
    std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map,
    std::unordered_set<int32_t> &different_status_pidx_set)
{
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
                 enum_to_string(app_status),
                 partition_bulk_load_info_map.size() - different_count,
                 different_count,
                 ainfo.partition_count - partition_bulk_load_info_map.size());
        return false;
    }

    if (app_status == bulk_load_status::BLS_DOWNLOADED ||
        app_status == bulk_load_status::BLS_INGESTING) {
        bulk_load_status::type valid_status = get_valid_partition_status(app_status);
        ddebug_f("app({}) bulk_load_status={}, valid partition status may be {} or {}",
                 ainfo.app_name,
                 enum_to_string(app_status),
                 enum_to_string(app_status),
                 enum_to_string(valid_status));
        for (auto iter = different_status_pidx_set.begin(); iter != different_status_pidx_set.end();
             ++iter) {
            int32_t pidx = *iter;
            if (partition_bulk_load_info_map[pidx].status != valid_status) {
                derror_f("app({}) bulk_load_status={}, but partition[{}] "
                         "bulk_load_status={}, only {} and {} is valid",
                         ainfo.app_name,
                         app_status,
                         pidx,
                         enum_to_string(partition_bulk_load_info_map[pidx].status),
                         enum_to_string(app_status),
                         enum_to_string(valid_status));
                return false;
            }
        }
        return true;
    }

    // if app status is finish, only finish is valid for partition status
    if (app_status == bulk_load_status::BLS_FINISH && different_count > 0) {
        derror_f("app({}) bulk_load_status={}, there are {} partitions "
                 "bulk_load_status is not {}, this is invalid",
                 ainfo.app_name,
                 app_status,
                 different_count,
                 enum_to_string(bulk_load_status::BLS_FINISH));
        return false;
    }

    // if app status is downloading or failed, partition status can be any status
    if (app_status == bulk_load_status::BLS_DOWNLOADING ||
        app_status == bulk_load_status::BLS_FAILED) {
        return true;
    }

    // invalid, paused, canceled
    return true;
}

bulk_load_status::type
bulk_load_service::get_valid_partition_status(bulk_load_status::type app_status)
{
    if (app_status == bulk_load_status::BLS_DOWNLOADED) {
        return bulk_load_status::BLS_INGESTING;
    } else if (app_status == bulk_load_status::BLS_INGESTING ||
               app_status == bulk_load_status::BLS_FINISH) {
        return bulk_load_status::BLS_FINISH;
    } else {
        // BLS_DOWNLOADING, BLS_FAILED no limit
        // TODO(heyuchen): consider paused and cancel
        return app_status;
    }
}

void bulk_load_service::continue_app_bulk_load(
    const app_bulk_load_info &ainfo,
    std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map,
    std::unordered_set<int32_t> different_status_pidx_set)
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
             enum_to_string(app_status),
             partition_bulk_load_info_map.size(),
             same_count,
             different_count);

    // get in_progress_partition_count
    int in_progress_partition_count = partition_count; // failed or all downloading
    if (app_status == bulk_load_status::BLS_DOWNLOADING) {
        if (invalid_count > 0) {
            in_progress_partition_count = invalid_count;
        } else if (different_count > 0) {
            in_progress_partition_count = different_count;
        }
    } else if (app_status == bulk_load_status::BLS_DOWNLOADED ||
               app_status == bulk_load_status::BLS_INGESTING ||
               app_status == bulk_load_status::BLS_FINISH) {
        in_progress_partition_count = same_count;
    }
    {
        zauto_write_lock l(_lock);
        _apps_in_progress_count[app_id] = in_progress_partition_count;
    }

    // if app status is downloading and all partition exist, set all partition status to downloading
    // if app status is failed, set all partition status to failed
    if ((app_status == bulk_load_status::BLS_FAILED ||
         (app_status == bulk_load_status::BLS_DOWNLOADING && invalid_count == 0)) &&
        different_count > 0) {
        for (auto iter = different_status_pidx_set.begin(); iter != different_status_pidx_set.end();
             ++iter) {
            int32_t pidx = *iter;
            std::string path = get_partition_bulk_load_path(get_app_bulk_load_path(app_id), pidx);
            update_partition_bulk_load_status(
                ainfo.app_name, gpid(app_id, pidx), path, app_status, false);
        }
    }

    // send bulk_load_request to primary
    for (int i = 0; i < partition_count; ++i) {
        gpid pid = gpid(app_id, i);
        if (app_status == bulk_load_status::BLS_DOWNLOADING &&
            partition_bulk_load_info_map.find(i) == partition_bulk_load_info_map.end()) {
            // partition dir not exist on remote storage
            // case1.1, case2.1
            create_partition_bulk_load_info(ainfo.app_name, pid, partition_count);
        } else if (app_status == bulk_load_status::BLS_DOWNLOADING ||
                   app_status == bulk_load_status::BLS_DOWNLOADED ||
                   app_status == bulk_load_status::BLS_INGESTING ||
                   app_status == bulk_load_status::BLS_FINISH ||
                   app_status == bulk_load_status::BLS_FAILED) {
            // case2.2~2.8, case3.1~3.5
            partition_bulk_load(pid);
        }
        if (app_status == bulk_load_status::BLS_INGESTING) {
            partition_ingestion(pid);
        }
    }
}

void bulk_load_service::create_partition_bulk_load_info(const std::string &app_name,
                                                        const gpid &pid,
                                                        int32_t partition_count)
{
    partition_bulk_load_info pinfo;
    pinfo.status = bulk_load_status::BLS_DOWNLOADING;
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);
    std::string app_bulk_load_path = get_app_bulk_load_path(pid.get_app_id());

    _meta_svc->get_meta_storage()->create_node(
        get_partition_bulk_load_path(app_bulk_load_path, pid.get_partition_index()),
        std::move(value),
        [app_name, pid, partition_count, pinfo, this]() {
            dinfo_f("app({}) create partition({}) bulk_load_info", app_name, pid.to_string());
            {
                zauto_write_lock l(_lock);
                --_apps_in_progress_count[pid.get_app_id()];
                _partition_bulk_load_info[pid] = pinfo;

                if (_apps_in_progress_count[pid.get_app_id()] == 0) {
                    ddebug_f("app({}) start bulk load succeed", app_name);
                    _apps_in_progress_count[pid.get_app_id()] = partition_count;
                }
            }
            // start send bulk load to replica servers
            partition_bulk_load(pid);
        });
}

void bulk_load_service::check_app_bulk_load_consistency(std::shared_ptr<app_state> app,
                                                        bool is_app_bulk_loading)
{
    std::string app_path = get_app_bulk_load_path(app->app_id);
    // TODO(heyuchen): create node_exist function in mms
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
                    update_app_bulk_load_flag(std::move(app), false);
                    return;
                }
                if (err == ERR_OK && !is_app_bulk_loading) {
                    dwarn_f("app({}): bulk load dir({}) exist, but is_bulk_loading={}, remove "
                            "useless bulk load dir",
                            app->app_name,
                            app_path,
                            is_app_bulk_loading);
                    remove_app_bulk_load_dir(std::move(app), false);
                    return;
                }
            }
        });
}

} // namespace replication
} // namespace dsn
