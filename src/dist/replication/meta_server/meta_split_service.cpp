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

#include "dist/replication/meta_server/meta_split_service.h"
#include "dist/replication/meta_server/meta_state_service_utils.h"

namespace dsn{
namespace replication {

void meta_split_service::app_partition_split(app_partition_split_rpc rpc){
    const auto &request = rpc.request();
    ddebug_f("split partition for app({}), new_partition_count={}",
             request.app_name,
             request.new_partition_count);

    auto &response = rpc.response();
    response.err = ERR_OK;

    std::shared_ptr<app_state> app;
    { // validate rpc parameters

        zauto_write_lock l(app_lock());

        // if app is not available
        app = _state->get_app(request.app_name);
        if (!app || app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_APP_NOT_EXIST;
            return;
        }

        response.app_id = app->app_id;
        response.partition_count = app->partition_count;

        // if new_partition_count != old_partition_count*2
        if (request.new_partition_count != app->partition_count * 2) {
            response.err = ERR_INVALID_PARAMETERS;
            return;
        }

        // if there's ongoing split already.
        for (const auto &partition_config : app->partitions) {
            if (partition_config.ballot < 0) {
                response.err = ERR_BUSY;
                dwarn_f("client({}) sent repeated split request: app({}), new_partition_count({})",
                        ((message_ex *)rpc.dsn_request())->header->from_address.to_string(),
                        request.app_name,
                        request.new_partition_count);
                return;
            }
        }
    }

    // validation passed
    do_app_partition_split(std::move(app), std::move(rpc));
}

void meta_split_service::do_app_partition_split(std::shared_ptr<app_state> app, app_partition_split_rpc rpc){

    auto on_write_storage_complete = [app, rpc, this](error_code ec) {
        if(ec == ERR_OK) {
            ddebug_f("app {} partition split succeed, new partition count is {}",
                     app->app_name.c_str(),
                     app->partition_count);

            zauto_write_lock l(app_lock());
            app->partition_count *= 2;
            app->helpers->contexts.resize(app->partition_count);
            app->partitions.resize(app->partition_count);

            for(int i = 0; i < app->partition_count; ++i){
                app->helpers->contexts[i].config_owner = &app->partitions[i];
                if(i >= app->partition_count/2){
                    app->partitions[i].ballot = invalid_ballot;
                    app->partitions[i].pid = gpid(app->app_id, i);
                }
            }

            auto &response = rpc.response();
            response.err = ERR_OK;
            response.partition_count = app->partition_count;

        } else if( ec == ERR_TIMEOUT){
            dwarn_f("remote storage is not available now, please try it later");
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             std::bind(&meta_split_service::do_app_partition_split, this, std::move(app), std::move(rpc)),
                             0,
                             std::chrono::seconds(1));
            //TODO(hyc):consider tracker
        } else {
            dassert(false, "failed to write to remote stroage, error is %s", ec.to_string());
        }
    };


    // if init_partition_count is invalid, init_partition_count = original partition_count
    if (app->init_partition_count < 0){
        app->init_partition_count = app->partition_count;
    }
    auto copy = *app;
    copy.partition_count *= 2;
    blob value = dsn::json::json_forwarder<app_info>::encode(copy);

    _meta_svc->get_remote_storage()->set_data(
                _state->get_app_path(*app),
                std::move(value),
                LPC_META_STATE_HIGH,
                on_write_storage_complete,
                _meta_svc->tracker());

    //TODO: refactor
//    _meta_svc->get_meta_storage()->set_data(
//        _state->get_app_path(*app), std::move(value), [app, rpc, this]() {
//            zauto_write_lock l(_lock);
//            app->partition_count *= 2;
//            app->partitions.resize(static_cast<size_t>(app->partition_count));
//            for (int i = app->partition_count / 2; i < app->partition_count; i++) {
//                app->partitions[i].ballot = invalid_ballot;
//                app->partitions[i].pid = gpid(app->app_id, i);
//            }
//            rpc.response().partition_count = app->partition_count;
//        });
}

meta_split_service::meta_split_service(meta_service* meta_srv){
    this->_meta_svc = meta_srv;
    this->_state = meta_srv->get_server_state();
}

} // namespace replication
} // namespace dsn