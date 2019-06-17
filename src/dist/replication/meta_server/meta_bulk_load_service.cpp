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
//#include "dist/replication/meta_server/meta_state_service_utils.h"

//#include "dist/replication/meta_server/meta_service.h"
//#include "dist/replication/meta_server/server_state.h"

namespace dsn {
namespace replication {

bulk_load_service::bulk_load_service(meta_service *meta_svc) : _meta_svc(meta_svc)
{
    _state = _meta_svc->get_server_state();
}

void bulk_load_service::on_start_bulk_load(start_bulk_load_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    // <cluster_name>/<timestamp>?/bulk_load_info+<table_name_table_id>id?/<partition_index>/sst
    // files
    {
        // zauto_write_lock l(app_lock());
        zauto_read_lock l;
        _state->lock_read(l);

        std::shared_ptr<app_state> app = _state->get_app(request.app_name);
        int partition_count = app->partition_count;

        // Validate:
        // 1. check app status
        // 2. check provider type
        // 3. check file existed
        // 4. check partition count

        // Execute:
        ddebug_f("start app {} bulk load", request.app_name);
        // set meta level to steady
        meta_function_level::type level = _meta_svc->get_function_level();
        if (level != meta_function_level::fl_steady) {
            _meta_svc->set_function_level(meta_function_level::fl_steady);
            ddebug_f(
                "change meta server function level from {} to {} to avoid possible balance",
                _meta_function_level_VALUES_TO_NAMES.find(level)->second,
                _meta_function_level_VALUES_TO_NAMES.find(meta_function_level::fl_steady)->second);
        }

        // set partition_configuration's load_status to BK_DOWMLOADING on zk
        for (int i = 0; i < partition_count; ++i) {
            auto cur_status = app->partitions[i].load_status;
            if (cur_status != bulk_load_status::BS_INVALID) {
                dwarn_f("app {} partition[{}] status is {}, can not start bulk load",
                        request.app_name,
                        i,
                        dsn::enum_to_string(cur_status));
                response.err = ERR_BUSY;
                break;
            }
            // update_bulk_load_status_on_remote_storage(app, i);
            // 3. app->helpers->partitions_in_progress
        }
    }
}

// void bulk_load_service::update_bulk_load_status_on_remote_storage(std::shared_ptr<app_state>
// &app, int pidx)
//{
//    partition_configuration &pc = app->partitions[pidx];
//    config_context &cc = app->helpers->contexts[pidx];

//}

} // namespace replication
} // namespace dsn
