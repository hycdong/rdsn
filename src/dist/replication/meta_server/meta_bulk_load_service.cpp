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

#include <dsn/dist/fmt_logging.h>

#include "meta_bulk_load_service.h"

namespace dsn {
namespace replication {

bulk_load_service::bulk_load_service(meta_service *meta_svc, const std::string &bulk_load_dir)
    : _meta_svc(meta_svc), _bulk_load_root(bulk_load_dir)
{
    _state = _meta_svc->get_server_state();
}

void bulk_load_service::create_bulk_load_dir_on_remote_stroage()
{
    std::string path = _bulk_load_root;
    blob value = blob();
    _meta_svc->get_meta_storage()->create_node(std::move(path), std::move(value), [this]() {
        ddebug_f("create bulk load root({}) succeed", _bulk_load_root);
        start_sync_apps_bulk_load();
    });
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
            derror_f("app {} is not existed or not available", request.app_name);
            response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
            return;
        }

        if (app->is_bulk_loading) {
            derror_f("app {} is already executing bulk load, please wait", app->app_name.c_str());
            response.err = ERR_BUSY;
            return;
        }

        if (_meta_svc->get_block_service_manager().get_block_filesystem(
                request.file_provider_type) == nullptr) {
            derror_f("invalid remote file provider type {}", request.file_provider_type);
            response.err = ERR_INVALID_PARAMETERS;
            return;
        }
    }

    // TODO(heyuchen): validate in bulk load info file
    // 1. check file existed
    // 2. check partition count
    ddebug_f("start app {} bulk load, cluster_name={}, provider={}",
             request.app_name,
             request.cluster_name,
             request.file_provider_type);

    // set meta level to steady
    meta_function_level::type level = _meta_svc->get_function_level();
    if (level != meta_function_level::fl_steady) {
        _meta_svc->set_function_level(meta_function_level::fl_steady);
        ddebug_f("change meta server function level from {} to {} to avoid possible balance",
                 _meta_function_level_VALUES_TO_NAMES.find(level)->second,
                 _meta_function_level_VALUES_TO_NAMES.find(meta_function_level::fl_steady)->second);
    }

    // set bulk_load_status to true on zk
    start_bulk_load_on_remote_storage(std::move(app), std::move(rpc));
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
                _bulk_load_states.apps_in_progress_count[app->app_id] = app->partition_count;
            }
            create_app_bulk_load_dir_with_rpc(std::move(app), std::move(rpc));
        });
}

void bulk_load_service::create_app_bulk_load_dir_with_rpc(std::shared_ptr<app_state> app,
                                                          start_bulk_load_rpc rpc)
{
    std::string bulk_load_path = get_app_bulk_load_path(app->app_id);
    const auto req = rpc.request();
    app_bulk_load_info ainfo;
    ainfo.app_id = app->app_id;
    ainfo.app_name = app->app_name;
    ainfo.partition_count = app->partition_count;
    ainfo.status = bulk_load_status::BLS_DOWNLOADING;
    ainfo.cluster_name = req.cluster_name;
    ainfo.file_provider_type = req.file_provider_type;
    blob value = dsn::json::json_forwarder<app_bulk_load_info>::encode(ainfo);

    {
        zauto_write_lock l(_lock);
        _bulk_load_info[ainfo.app_id] = ainfo;
    }

    _meta_svc->get_meta_storage()->create_node(
        std::move(bulk_load_path), std::move(value), [app, rpc, bulk_load_path, this]() {
            ddebug_f("create app {} bulk load dir", app->app_name);
            for (int i = 0; i < app->partition_count; ++i) {
                create_partition_bulk_load_dir_with_rpc(
                    app->app_name, gpid(app->app_id, i), app->partition_count, bulk_load_path, rpc);
            }
        });
}

void bulk_load_service::create_partition_bulk_load_dir_with_rpc(const std::string &app_name,
                                                                gpid pid,
                                                                uint32_t partition_count,
                                                                const std::string &bulk_load_path,
                                                                start_bulk_load_rpc rpc)
{
    partition_bulk_load_info pinfo;
    pinfo.status = bulk_load_status::BLS_DOWNLOADING;
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->create_node(
        get_partition_bulk_load_path(bulk_load_path, pid.get_partition_index()),
        std::move(value),
        [app_name, pid, partition_count, bulk_load_path, rpc, pinfo, this]() {
            ddebug_f(
                "app {} create partition[{}] bulk_load_info", app_name, pid.get_partition_index());
            {
                zauto_write_lock l(_lock);
                --_bulk_load_states.apps_in_progress_count[pid.get_app_id()];
                _bulk_load_states.partitions_info[pid] = pinfo;

                if (_bulk_load_states.apps_in_progress_count[pid.get_app_id()] == 0) {
                    ddebug_f("app {} start bulk load succeed", app_name);
                    _bulk_load_states.apps_in_progress_count[pid.get_app_id()] = partition_count;
                    auto response = rpc.response();
                    response.err = ERR_OK;
                }
            }
            // start send bulk load to replica servers
            partition_bulk_load(pid);
        });
}

void bulk_load_service::partition_bulk_load(gpid pid)
{
    dsn::rpc_address primary_addr;
    std::string app_name;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            dwarn_f("app {} not exist, set bulk load finish", app->app_name);
            // TODO(heyuchen): handler it
            return;
        }
        app_name = app->app_name;
        primary_addr = app->partitions[pid.get_partition_index()].primary;
    }

    if (primary_addr.is_invalid()) {
        dwarn_f("app {} gpid({}.{}) primary is invalid, try it later");
        tasking::enqueue(LPC_META_STATE_NORMAL,
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
        pbl_info = _bulk_load_states.partitions_info[pid];
        ainfo = _bulk_load_info[pid.get_app_id()];
    }

    bulk_load_request req;
    req.pid = pid;
    req.app_name = app_name;
    req.primary_addr = primary_addr;
    req.app_bl_status = ainfo.status;
    req.remote_provider_name = ainfo.file_provider_type;
    req.cluster_name = ainfo.cluster_name;
    req.partition_bl_info = pbl_info;

    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_BULK_LOAD, 0, pid.thread_hash());
    dsn::marshall(msg, req);
    dsn::rpc_response_task_ptr rpc_callback = rpc::create_rpc_response_task(
        msg,
        _meta_svc->tracker(),
        [this, pid, primary_addr](error_code err, bulk_load_response &&resp) {
            on_partition_bulk_load_reply(err, std::move(resp), pid, primary_addr);
        });

    zauto_write_lock l(_lock);
    _bulk_load_states.partitions_request[pid] = rpc_callback;

    ddebug_f(
        "send bulk load request to replica server, app({}.{}), target_addr = {}, app bulk load "
        "= {}, partition bulk load status = {}, remote provider = {}, cluster_name = {}",
        pid.get_app_id(),
        pid.get_partition_index(),
        primary_addr.to_string(),
        enum_to_string(req.app_bl_status),
        enum_to_string(pbl_info.status),
        req.remote_provider_name,
        req.cluster_name);
    _meta_svc->send_request(msg, primary_addr, rpc_callback);
}

void bulk_load_service::on_partition_bulk_load_reply(dsn::error_code err,
                                                     bulk_load_response &&response,
                                                     gpid pid,
                                                     const rpc_address &primary_addr)
{
    if (err != ERR_OK) {
        dwarn_f("gpid({}.{}) failed to recevie bulk load response, err={}",
                pid.get_app_id(),
                pid.get_partition_index(),
                err.to_string());
    } else if (response.err == ERR_OBJECT_NOT_FOUND || response.err == ERR_INVALID_STATE) {
        dwarn_f("gpid({}.{}) doesn't exist or has invalid state on node({}), err={}, plesae retry "
                "later",
                pid.get_app_id(),
                pid.get_partition_index(),
                primary_addr.to_string(),
                response.err.to_string());
    } else if (response.err != ERR_OK) {
        // TODO(heyuchen): add bulk load status check, not only downloading error handler below
        if (response.err == ERR_CORRUPTION) {
            derror_f("gpid({}.{}) failed to download files from remote provider, coz files "
                     "are damaged, err={}",
                     pid.get_app_id(),
                     pid.get_partition_index(),
                     response.err.to_string());
        } else {
            dwarn_f("gpid({}.{}) failed to download files from remote provider, coz file "
                    "system error, err={}",
                    pid.get_app_id(),
                    pid.get_partition_index(),
                    response.err.to_string());
        }
        {
            zauto_write_lock l(_lock);
            if (!_bulk_load_states.apps_cleaning_up[pid.get_app_id()]) {
                _bulk_load_states.apps_cleaning_up[pid.get_app_id()] = true;
                update_app_bulk_load_status_unlock(response.pid.get_app_id(),
                                                   bulk_load_status::BLS_FAILED);
            }
        }
    } else {
        // download sst files
        // TODO(heyuchen): add bulk load status check, not only downloading error handler below
        // handle file damaged error during downloading files from remote stroage
        auto cur_status = get_app_bulk_load_status(response.pid.get_app_id());
        if (cur_status == bulk_load_status::BLS_DOWNLOADING) {
            dsn::error_code ec = check_download_status(response);
            if (ec != ERR_OK) {
                dwarn_f(
                    "recevie bulk load response from {} app({}), gpid({}.{}), bulk load status={}, "
                    "download meet error={}",
                    primary_addr.to_string(),
                    response.app_name,
                    pid.get_app_id(),
                    pid.get_partition_index(),
                    enum_to_string(response.partition_bl_status),
                    ec.to_string());
                {
                    zauto_read_lock l(_lock);
                    if (!_bulk_load_states.apps_cleaning_up[pid.get_app_id()]) {
                        _bulk_load_states.apps_cleaning_up[pid.get_app_id()] = true;
                        update_app_bulk_load_status_unlock(response.pid.get_app_id(),
                                                           bulk_load_status::BLS_FAILED);
                    }
                }
            } else {
                // download sst files succeed
                int32_t cur_progress =
                    response.__isset.total_download_progress ? response.total_download_progress : 0;
                ddebug_f(
                    "recevie bulk load response from {} app({}) gpid({}.{}), bulk load status={}, "
                    "total_download_progress={}",
                    primary_addr.to_string(),
                    response.app_name,
                    pid.get_app_id(),
                    pid.get_partition_index(),
                    enum_to_string(response.partition_bl_status),
                    cur_progress);
                {
                    zauto_write_lock l(_lock);
                    _bulk_load_states.partitions_total_download_progress[pid] = cur_progress;
                    if (response.__isset.download_progresses) {
                        _bulk_load_states.partitions_download_progress[pid] =
                            response.download_progresses;
                    }
                }

                // TODO(heyuchen): change it to common value
                int32_t max_progress = 100;
                if (cur_progress >= max_progress) {
                    ddebug_f("gpid({}.{}) finish download remote files from remote provider",
                             pid.get_app_id(),
                             pid.get_partition_index());

                    // TODO(heyuchen): usage to get app lock
                    {
                        zauto_read_lock l(app_lock());
                        std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
                        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
                            dwarn_f("app {} not exist, set bulk load finish", app->app_name);
                            // TODO(heyuchen): handler it
                            return;
                        }

                        std::string partition_bulk_load_path = get_partition_bulk_load_path(
                            get_app_bulk_load_path(pid.get_app_id()), pid.get_partition_index());
                        // TODO(heyuchen): check if status switch is valid
                        update_partition_bulk_load_status(app->app_name,
                                                          pid,
                                                          partition_bulk_load_path,
                                                          bulk_load_status::BLS_DOWNLOADED);
                    }
                    // TODO(heyuchen): not return here
                    return;
                }
            }
        } else if (cur_status == bulk_load_status::BLS_FAILED) {
            bool all_clean_up = true;
            bool checking_clean_up = response.__isset.context_clean_flags;
            ddebug_f("recevie bulk load response from {} app({}) gpid({}.{}), bulk load status={}, "
                     "checking cleanup={}",
                     primary_addr.to_string(),
                     response.app_name,
                     pid.get_app_id(),
                     pid.get_partition_index(),
                     enum_to_string(response.partition_bl_status),
                     checking_clean_up);
            if (checking_clean_up) {
                for (auto iter = response.context_clean_flags.begin();
                     iter != response.context_clean_flags.end();
                     ++iter) {
                    all_clean_up = all_clean_up && iter->second;
                    if (!all_clean_up) {
                        ddebug_f("app({}) gpid({}.{}) node({}) not cleanup bulk load context",
                                 response.app_name,
                                 pid.get_app_id(),
                                 pid.get_partition_index(),
                                 iter->first.to_string());
                    }
                }
                if (all_clean_up) {
                    ddebug_f("app({}) gpid({}.{}) cleanup bulk load context",
                             response.app_name,
                             pid.get_app_id(),
                             pid.get_partition_index());

                    int count;
                    std::shared_ptr<app_state> app;
                    {
                        zauto_write_lock l(_lock);
                        count = --_bulk_load_states.apps_in_progress_count[pid.get_app_id()];
                    }
                    if (count == 0) {
                        {
                            zauto_read_lock l(app_lock());
                            app = _state->get_app(pid.get_app_id());
                            if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
                                dwarn_f("app {} not exist, set bulk load finish", app->app_name);
                                // TODO(heyuchen): handler it
                                return;
                            }
                        }
                        ddebug_f("app({}) all partitions cleanup bulk load context",
                                 response.app_name);
                        update_app_bulk_load_flag(app, false, true);
                    }
                }
            }
        }
    }

    if (is_app_bulk_loading(pid.get_app_id())) {

        // TODO(heyuchen): delay time to config
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&bulk_load_service::partition_bulk_load, this, pid),
                         0,
                         std::chrono::seconds(10));
    } else {
        ddebug_f("app {} is not bulk loading, stop to send request to partition[{}] primary "
                 "replica server",
                 response.app_name,
                 pid.get_partition_index());
    }
}

dsn::error_code bulk_load_service::check_download_status(bulk_load_response &response)
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
            dwarn_f("app({}) gpid({}.{}) meet error during downloading sst files, node_address={}, "
                    "error={}",
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
                                                          dsn::gpid pid,
                                                          std::string &path,
                                                          bulk_load_status::type status)
{
    zauto_read_lock l(_lock);
    partition_bulk_load_info pinfo = _bulk_load_states.partitions_info[pid];
    pinfo.status = status;
    dsn::blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->set_data(
        std::move(path), std::move(value), [this, app_name, pid, path, status]() {
            zauto_write_lock l(_lock);
            bulk_load_status::type old_status = _bulk_load_states.partitions_info[pid].status;
            ddebug_f("app {} update partition[{}] bulk_load status from {} to {}",
                     app_name,
                     pid.get_partition_index(),
                     enum_to_string(old_status),
                     enum_to_string(status));

            _bulk_load_states.partitions_info[pid].status = status;

            if (status == bulk_load_status::BLS_DOWNLOADED) {
                _bulk_load_states.partitions_request[pid] = nullptr;
                if (--_bulk_load_states.apps_in_progress_count[pid.get_app_id()] == 0) {
                    update_app_bulk_load_status_unlock(pid.get_app_id(), status);
                }
            } else if (status == bulk_load_status::BLS_FAILED) {
                ddebug_f("app {} gpid({}.{}) set bulk load status as {}",
                         app_name,
                         pid.get_app_id(),
                         pid.get_partition_index(),
                         enum_to_string(status));
            }
        });
}

void bulk_load_service::update_app_bulk_load_status_unlock(uint32_t app_id,
                                                           bulk_load_status::type new_status)
{
    app_bulk_load_info ainfo = _bulk_load_info[app_id];
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
        ddebug_f("update app({}) bulk load status from {} to {}",
                 ainfo.app_name,
                 enum_to_string(old_status),
                 enum_to_string(new_status));
        {
            zauto_write_lock l(_lock);
            _bulk_load_info[app_id] = ainfo;
        }

        if (new_status == bulk_load_status::BLS_FAILED) {
            {
                zauto_write_lock l(_lock);
                _bulk_load_states.apps_in_progress_count[app_id] = ainfo.partition_count;
            }
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

void bulk_load_service::update_app_bulk_load_flag(std::shared_ptr<app_state> app,
                                                  bool is_bulk_loading,
                                                  bool need_remove)
{
    app_info info = *app;
    info.is_bulk_loading = is_bulk_loading;

    blob value = dsn::json::json_forwarder<app_info>::encode(info);
    _meta_svc->get_meta_storage()->set_data(
        _state->get_app_path(*app), std::move(value), [app, is_bulk_loading, need_remove, this]() {
            {
                zauto_write_lock l(app_lock());
                app->is_bulk_loading = is_bulk_loading;
            }
            ddebug_f("app({}) update app is_bulk_loading to {}", app->app_id, is_bulk_loading);
            if (!is_bulk_loading && need_remove) {
                remove_app_bulk_load_dir(app->app_id);
            }
        });
}

void bulk_load_service::remove_app_bulk_load_dir(uint32_t app_id)
{
    std::string bulk_load_path = get_app_bulk_load_path(app_id);
    _meta_svc->get_meta_storage()->delete_node_recursively(
        std::move(bulk_load_path), [this, app_id, bulk_load_path]() {
            ddebug_f("remove app({}) bulk load dir {}", app_id, bulk_load_path);
            clear_app_bulk_load_context(app_id);
            zauto_write_lock l(_lock);
            _bulk_load_app_id.erase(app_id);
        });
}

void bulk_load_service::clear_app_bulk_load_context(uint32_t app_id)
{
    zauto_write_lock l(_lock);
    _bulk_load_info.erase(app_id);
    _bulk_load_states.apps_in_progress_count.erase(app_id);
    erase_map_elem_by_id(app_id, _bulk_load_states.partitions_download_progress);
    erase_map_elem_by_id(app_id, _bulk_load_states.partitions_info);
    // ddebug_f("partitions_info is_empty={}", _bulk_load_states.partitions_info.empty());
    erase_map_elem_by_id(app_id, _bulk_load_states.partitions_request);
    erase_map_elem_by_id(app_id, _bulk_load_states.partitions_total_download_progress);
    _bulk_load_states.apps_cleaning_up.erase(app_id);
    ddebug_f("clear app({}) bulk load context", app_id);
}

template <typename T>
void bulk_load_service::erase_map_elem_by_id(uint32_t app_id, std::map<gpid, T> &mymap)
{
    for (auto iter = mymap.begin(); iter != mymap.end();) {
        if (iter->first.get_app_id() == app_id) {
            mymap.erase(iter++);
        }
    }
}

void bulk_load_service::on_query_bulk_load_status(query_bulk_load_rpc rpc)
{
    const auto &request = rpc.request();
    const std::string &app_name = request.app_name;

    configuration_query_bulk_load_response &response = rpc.response();
    response.err = ERR_OK;
    response.app_name = app_name;

    uint32_t app_id, partition_count, max_replica_count;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(app_name);
        app_id = app->app_id;
        partition_count = app->partition_count;
        max_replica_count = app->max_replica_count;

        if (app == nullptr) {
            derror_f("app {} is not existed", app_name);
            response.err = ERR_OBJECT_NOT_FOUND;
            return;
        }

        if (app->status != app_status::AS_AVAILABLE) {
            derror_f("app {} is not available", app_name);
            response.err = ERR_APP_DROPPED;
            return;
        }

        if (!app->is_bulk_loading) {
            dwarn_f("app {} is not during bulk load", app_name);
            response.err = ERR_INVALID_STATE;
            return;
        }
    }

    {
        zauto_read_lock l(_lock);
        response.max_replica_count = max_replica_count;
        response.app_status = get_app_bulk_load_status(app_id);
        response.partition_status.resize(partition_count);
        ddebug_f("query app({}) bulk_load_status({}) succeed",
                 app_name,
                 enum_to_string(response.app_status));

        if (response.app_status == bulk_load_status::BLS_DOWNLOADING ||
            response.app_status == bulk_load_status::BLS_DOWNLOADED) {
            response.__isset.download_progresses = true;
            response.download_progresses.resize(partition_count);
        }

        auto partition_bulk_load_info_map = _bulk_load_states.partitions_info;
        for (auto iter = partition_bulk_load_info_map.begin();
             iter != partition_bulk_load_info_map.end();
             iter++) {
            int idx = iter->first.get_partition_index();
            response.partition_status[idx] = iter->second.status;
            if (response.__isset.download_progresses) {
                response.download_progresses[idx] =
                    _bulk_load_states.partitions_download_progress[iter->first];
            }
        }
    }
}

void bulk_load_service::start_sync_apps_bulk_load()
{
    std::string path = _bulk_load_root;
    _meta_svc->get_meta_storage()->get_children(
        std::move(path), [this, path](bool flag, const std::vector<std::string> &children) {
            if (!flag) {
                ddebug_f("get {} children failed", path);
                return;
            }
            if (children.size() > 0) {
                ddebug_f("There are {} apps need to sync bulk load status", children.size());
                for (auto &elem : children) {
                    uint32_t app_id = boost::lexical_cast<uint32_t>(elem);
                    ddebug_f("start to sync app({}) bulk load status", app_id);
                    do_sync_app_bulk_load(app_id, get_app_bulk_load_path(app_id));
                }
            }
        });
}

void bulk_load_service::do_sync_app_bulk_load(uint32_t app_id, std::string app_path)
{
    // get app_bulk_load_info
    _meta_svc->get_meta_storage()->get_data(
        std::move(app_path), [this, app_id, app_path](const blob &value) {
            app_bulk_load_info ainfo;
            dsn::json::json_forwarder<app_bulk_load_info>::decode(value, ainfo);
            {
                zauto_write_lock l(_lock);
                _bulk_load_app_id.insert(app_id);
                _bulk_load_info[app_id] = ainfo;
            }
            do_sync_partitions_bulk_load(
                app_path, ainfo.app_id, ainfo.app_name, ainfo.partition_count);
        });
}

void bulk_load_service::do_sync_partitions_bulk_load(std::string app_path,
                                                     uint32_t app_id,
                                                     std::string app_name,
                                                     uint32_t partition_count)
{
    _meta_svc->get_meta_storage()->get_children(
        std::move(app_path),
        [this, app_path, app_id, app_name, partition_count](
            bool flag, const std::vector<std::string> &children) {
            // TODO(heyuchen): handle failure
            if (!flag) {
                dwarn_f("remote dir({}) children not found", app_path);
                return;
            }

            int in_progress_count = partition_count - children.size();
            // TODO(heyuchen): handle failure
            if (in_progress_count < 0) {
                dwarn_f("bulk load failed, partition_count={}, remote path count={}",
                        partition_count,
                        children.size());
                return;
            }

            {
                zauto_write_lock l(_lock);
                _bulk_load_states.apps_in_progress_count[app_id] =
                    in_progress_count == 0 ? partition_count : in_progress_count;
            }

            // pidx -> exist_on_remote_storage
            std::map<uint32_t, bool> partitions;
            for (int i = 0; i < partition_count; ++i) {
                partitions[i] = false;
            }
            for (const auto &path : children) {
                uint32_t pidx = boost::lexical_cast<uint32_t>(path);
                partitions[pidx] = true;
            }

            for (auto iter = partitions.begin(); iter != partitions.end(); ++iter) {
                gpid pid = gpid(app_id, iter->first);
                std::string partition_path = get_partition_bulk_load_path(app_path, iter->first);
                // partition dir exist on remote storage
                if (iter->second) {
                    _meta_svc->get_meta_storage()->get_data(
                        std::move(partition_path), [this, pid](const blob &value) {
                            partition_bulk_load_info pinfo;
                            dsn::json::json_forwarder<partition_bulk_load_info>::decode(value,
                                                                                        pinfo);
                            {
                                zauto_write_lock l(_lock);
                                _bulk_load_states.partitions_info[pid] = pinfo;
                            }
                            partition_bulk_load(pid);
                        });
                } else {
                    create_partition_bulk_load_info(app_name, pid, partition_count, app_path);
                }
            }
        });
}

void bulk_load_service::create_partition_bulk_load_info(const std::string &app_name,
                                                        gpid pid,
                                                        uint32_t partition_count,
                                                        const std::string &bulk_load_path)
{
    partition_bulk_load_info pinfo;
    pinfo.status = bulk_load_status::BLS_DOWNLOADING;
    blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

    _meta_svc->get_meta_storage()->create_node(
        get_partition_bulk_load_path(bulk_load_path, pid.get_partition_index()),
        std::move(value),
        [app_name, pid, partition_count, bulk_load_path, pinfo, this]() {
            ddebug_f(
                "app {} create partition[{}] bulk_load_info", app_name, pid.get_partition_index());
            {
                zauto_write_lock l(_lock);
                --_bulk_load_states.apps_in_progress_count[pid.get_app_id()];
                _bulk_load_states.partitions_info[pid] = pinfo;

                if (_bulk_load_states.apps_in_progress_count[pid.get_app_id()] == 0) {
                    ddebug_f("app {} start bulk load succeed", app_name);
                    _bulk_load_states.apps_in_progress_count[pid.get_app_id()] = partition_count;
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
        app_path,
        LPC_META_STATE_HIGH,
        [this, app_path, app, is_app_bulk_loading](dsn::error_code err) {
            if (err == ERR_TIMEOUT) {
                ddebug_f("check app bulk load dir({}) timeout, try later", app_path);
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
                    dwarn_f("app({}): bulk load dir({}) not exist, but is_bulk_loading={}, reset "
                            "app is_bulk_loading flag",
                            app->app_name,
                            app_path,
                            is_app_bulk_loading);
                    update_app_bulk_load_flag(app, false, false);
                    return;
                }
                if (err == ERR_OK && !is_app_bulk_loading) {
                    dwarn_f("app({}): bulk load dir({}) exist, but is_bulk_loading={}, remove "
                            "useless bulk load dir",
                            app->app_name,
                            app_path,
                            is_app_bulk_loading);
                    remove_app_bulk_load_dir(app->app_id);
                    return;
                }
            }
        });
}

} // namespace replication
} // namespace dsn
