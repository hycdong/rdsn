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

#include <gtest/gtest.h>

#include "dist/replication/meta_server/meta_server_failure_detector.h"
#include "dist/replication/meta_server/meta_service.h"
#include "meta_bulk_load_service_test_helper.h"

namespace dsn {
namespace replication {
class meta_bulk_load_service_test : public ::testing::Test
{
public:
    meta_bulk_load_service_test() {}

    void SetUp() override
    {
        _meta_svc.reset(meta_service_test_app::initialize_meta_service());
        _meta_svc->set_function_level(meta_function_level::fl_steady);
        _meta_svc->_failure_detector.reset(new meta_server_failure_detector(_meta_svc.get()));

        // initialize bulk load service
        _meta_svc->_bulk_load_svc.reset(new bulk_load_service_mock(
            _meta_svc.get(),
            meta_options::concat_path_unix_style(_meta_svc->_cluster_root, "bulk_load")));
        _meta_svc->_bulk_load_svc->create_bulk_load_dir_on_remote_stroage();

        _state = _meta_svc->_state;
        create_app(NAME, PARTITION_COUNT);
    }

    void TearDown() override
    {
        if (_state && _meta_svc) {
            meta_service_test_app::delete_all_on_meta_storage(_meta_svc.get());
        }

        _state.reset();
        _meta_svc.reset(nullptr);
    }

    bulk_load_service *bulk_svc() { return _meta_svc->_bulk_load_svc.get(); }

    void create_app(const std::string &name, int partition_count)
    {
        configuration_create_app_request req;
        configuration_create_app_response resp;
        req.app_name = name;
        req.options.app_type = "simple_kv";
        req.options.partition_count = partition_count;
        req.options.replica_count = 3;
        req.options.success_if_exist = false;
        req.options.is_stateful = true;

        auto result = fake_create_app(_state.get(), req);
        fake_wait_rpc(result, resp);
        ASSERT_EQ(resp.err, ERR_OK) << resp.err.to_string() << " " << name;

        // wait for the table to create
        ASSERT_TRUE(_state->spin_wait_staging(30));
    }

    std::shared_ptr<app_state> find_app(const std::string &name) { return _state->get_app(name); }

    void wait_all() { _meta_svc->tracker()->wait_outstanding_tasks(); }

    start_bulk_load_response start_bulk_load(const std::string &app_name,
                                             const std::string &cluster_name,
                                             const std::string &file_provider)
    {
        auto request = dsn::make_unique<start_bulk_load_request>();
        request->app_name = app_name;
        request->cluster_name = cluster_name;
        request->file_provider_type = file_provider;

        start_bulk_load_rpc rpc(std::move(request), RPC_CM_START_BULK_LOAD);
        bulk_svc()->on_start_bulk_load(rpc);
        wait_all();
        return rpc.response();
    }

    std::shared_ptr<server_state> _state;
    std::unique_ptr<meta_service> _meta_svc;
};

TEST_F(meta_bulk_load_service_test, start_bulk_load_with_not_existed_app)
{
    auto resp = start_bulk_load("table_not_exist", CLUSTER, PROVIDER);
    ASSERT_EQ(resp.err, ERR_APP_NOT_EXIST);
}

TEST_F(meta_bulk_load_service_test, start_bulk_load_with_wrong_provider)
{
    auto resp = start_bulk_load(NAME, CLUSTER, "provider_not_exist");
    ASSERT_EQ(resp.err, ERR_INVALID_PARAMETERS);
}

TEST_F(meta_bulk_load_service_test, start_bulk_load_succeed)
{
    mock_funcs["partition_bulk_load"] = nullptr;
    auto resp = start_bulk_load(NAME, CLUSTER, PROVIDER);
    ASSERT_EQ(resp.err, ERR_OK);
    mock_funcs.erase("partition_bulk_load");
}

} // namespace replication
} // namespace dsn

// TODO(heyuchen): start_sync_apps_bulk_load test
