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
        _app_root = _state->_apps_root;
    }

    void TearDown() override
    {
        if (_state && _meta_svc) {
            meta_service_test_app::delete_all_on_meta_storage(_meta_svc.get());
        }

        _state.reset();
        _meta_svc.reset(nullptr);
    }

    void setup_without_create_bulk_load_dir()
    {
        _meta_svc.reset(meta_service_test_app::initialize_meta_service());
        _meta_svc->set_function_level(meta_function_level::fl_steady);
        _meta_svc->_failure_detector.reset(new meta_server_failure_detector(_meta_svc.get()));

        // initialize bulk load service
        _meta_svc->_bulk_load_svc.reset(new bulk_load_service_mock(
            _meta_svc.get(),
            meta_options::concat_path_unix_style(_meta_svc->_cluster_root, "bulk_load")));

        _state = _meta_svc->_state;
    }

    bulk_load_service *bulk_svc() { return _meta_svc->_bulk_load_svc.get(); }

    void create_app(const std::string &name)
    {
        configuration_create_app_request req;
        configuration_create_app_response resp;
        req.app_name = name;
        req.options.app_type = "simple_kv";
        req.options.partition_count = PARTITION_COUNT;
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

    std::shared_ptr<server_state> _state;
    std::unique_ptr<meta_service> _meta_svc;
    std::string _app_root;
    std::function<void(dsn::gpid)> partition_bulk_load_mock = [](gpid pid) {
        std::cout << "send bulk load request to gpid(" << pid.get_app_id() << ","
                  << pid.get_partition_index() << ")" << std::endl;
    };
};

class bulk_load_start_test : public meta_bulk_load_service_test
{
public:
    bulk_load_start_test() {}

    void SetUp()
    {
        meta_bulk_load_service_test::SetUp();
        create_app(NAME);
    }

    void TearDown() { meta_bulk_load_service_test::TearDown(); }

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
};

TEST_F(bulk_load_start_test, wrong_app)
{
    auto resp = start_bulk_load("table_not_exist", CLUSTER, PROVIDER);
    ASSERT_EQ(resp.err, ERR_APP_NOT_EXIST);
}

TEST_F(bulk_load_start_test, wrong_provider)
{
    auto resp = start_bulk_load(NAME, CLUSTER, "provider_not_exist");
    ASSERT_EQ(resp.err, ERR_INVALID_PARAMETERS);
}

// TODO(heyuchen): validate cluster_name and partition_count

TEST_F(bulk_load_start_test, success)
{
    mock_funcs["partition_bulk_load"] = &partition_bulk_load_mock;

    auto resp = start_bulk_load(NAME, CLUSTER, PROVIDER);
    ASSERT_EQ(resp.err, ERR_OK);
    std::shared_ptr<app_state> app = find_app(NAME);
    ASSERT_EQ(app->is_bulk_loading, true);

    mock_funcs.erase("partition_bulk_load");
}

class bulk_load_query_test : public meta_bulk_load_service_test
{
public:
    bulk_load_query_test() {}

    void SetUp()
    {
        meta_bulk_load_service_test::SetUp();
        create_app(NAME);
    }

    void TearDown() { meta_bulk_load_service_test::TearDown(); }

    configuration_query_bulk_load_response query_bulk_load(const std::string &app_name)
    {
        auto request = dsn::make_unique<configuration_query_bulk_load_request>();
        request->app_name = app_name;

        query_bulk_load_rpc rpc(std::move(request), RPC_CM_QUERY_BULK_LOAD_STATUS);
        bulk_svc()->on_query_bulk_load_status(rpc);
        wait_all();
        return rpc.response();
    }
};

TEST_F(bulk_load_query_test, wrong_state)
{
    auto resp = query_bulk_load(NAME);
    ASSERT_EQ(resp.err, ERR_INVALID_STATE);
}

TEST_F(bulk_load_query_test, success)
{
    std::shared_ptr<app_state> app = find_app(NAME);
    app->is_bulk_loading = true;
    auto resp = query_bulk_load(NAME);
    ASSERT_EQ(resp.err, ERR_OK);
}

class bulk_load_sync_apps_test : public meta_bulk_load_service_test
{
public:
    bulk_load_sync_apps_test() {}

    void SetUp()
    {
        meta_bulk_load_service_test::setup_without_create_bulk_load_dir();
        mock_funcs["partition_bulk_load"] = &partition_bulk_load_mock;
    }

    void TearDown()
    {
        mock_funcs.erase("partition_bulk_load");
        meta_bulk_load_service_test::TearDown();
    }

    void mock_app_on_remote_stroage()
    {
        app_info info;
        info.app_id = 7;
        info.app_name = "half_bulk_load_downloading";
        info.app_type = "pegasus";
        info.is_stateful = true;
        info.is_bulk_loading = true;
        info.max_replica_count = 3;
        info.partition_count = 4;
        blob value = dsn::json::json_forwarder<app_info>::encode(info);

        _meta_svc->get_meta_storage()->create_node(
            _app_root + "/" + boost::lexical_cast<std::string>(info.app_id),
            std::move(value),
            []() {});
        _meta_svc->tracker()->wait_outstanding_tasks();
    }

    void mock_app_half_bulk_load_downloading(bool all_partitions_not_bulk_load)
    {
        std::string path = bulk_svc()->_bulk_load_root;
        blob value = blob();
        _meta_svc->get_meta_storage()->create_node(
            std::move(path), std::move(value), [this, all_partitions_not_bulk_load]() {
                app_bulk_load_info ainfo;
                ainfo.app_id = 7;
                ainfo.partition_count = 4;
                ainfo.app_name = "half_bulk_load_downloading";
                ainfo.cluster_name = CLUSTER;
                ainfo.file_provider_type = PROVIDER;
                ainfo.status = bulk_load_status::BLS_DOWNLOADING;
                blob value = dsn::json::json_forwarder<app_bulk_load_info>::encode(ainfo);
                std::string app_path = bulk_svc()->get_app_bulk_load_path(ainfo.app_id);

                _meta_svc->get_meta_storage()->create_node(
                    std::move(app_path),
                    std::move(value),
                    [app_path, all_partitions_not_bulk_load, this]() {
                        // mock partition
                        std::cout << "create app bulk load dir " << app_path << std::endl;
                        mock_partition_half_bulk_load_downloading(app_path,
                                                                  all_partitions_not_bulk_load);
                    });
            });
        _meta_svc->tracker()->wait_outstanding_tasks();
    }

    void mock_partition_half_bulk_load_downloading(std::string app_path,
                                                   bool all_partitions_not_bulk_load)
    {
        partition_bulk_load_info pinfo;
        pinfo.status = bulk_load_status::BLS_DOWNLOADING;
        blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);

        if (!all_partitions_not_bulk_load) {
            _meta_svc->get_meta_storage()->create_node(
                bulk_svc()->get_partition_bulk_load_path(app_path, 0),
                std::move(value),
                [app_path, this]() {
                    std::cout << "create partition bulk load dir "
                              << bulk_svc()->get_partition_bulk_load_path(app_path, 0) << std::endl;
                });
            _meta_svc->get_meta_storage()->create_node(
                bulk_svc()->get_partition_bulk_load_path(app_path, 3),
                std::move(value),
                [app_path, this]() {
                    std::cout << "create partition bulk load dir "
                              << bulk_svc()->get_partition_bulk_load_path(app_path, 3) << std::endl;
                });
        }
    }
};

TEST_F(bulk_load_sync_apps_test, only_app_bulk_load_exist)
{
    mock_app_on_remote_stroage();
    mock_app_half_bulk_load_downloading(true);

    bulk_svc()->create_bulk_load_dir_on_remote_stroage();
    _meta_svc->tracker()->wait_outstanding_tasks();
}

TEST_F(bulk_load_sync_apps_test, partition_bulk_load_half_exist)
{
    mock_app_on_remote_stroage();
    mock_app_half_bulk_load_downloading(false);

    bulk_svc()->create_bulk_load_dir_on_remote_stroage();
    _meta_svc->tracker()->wait_outstanding_tasks();
}

} // namespace replication
} // namespace dsn
