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
#include <dsn/utility/fail_point.h>

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

    void setup_without_create_bulk_load_dir(bool is_bulk_loading,
                                            bool mock_app_bulk_load_dir,
                                            bool all_partitions_not_bulk_load)
    {
        // initialize meta service
        auto meta_svc = new fake_receiver_meta_service();
        meta_svc->remote_storage_initialize();

        // initialize server_state
        auto state = meta_svc->_state;
        state->initialize(meta_svc, meta_svc->_cluster_root + "/apps");
        _app_root = state->_apps_root;
        meta_svc->_started = true;

        _meta_svc.reset(meta_svc);

        // initialize bulk load service
        _meta_svc->_bulk_load_svc.reset(new bulk_load_service_mock(
            _meta_svc.get(),
            meta_options::concat_path_unix_style(_meta_svc->_cluster_root, "bulk_load")));
        if (mock_app_bulk_load_dir) {
            mock_app_half_bulk_load_downloading(all_partitions_not_bulk_load);
        }

        // mock a app
        mock_app_on_remote_stroage(is_bulk_loading);
        state->initialize_data_structure();

        _meta_svc->set_function_level(meta_function_level::fl_steady);
        _meta_svc->_failure_detector.reset(new meta_server_failure_detector(_meta_svc.get()));
        _state = _meta_svc->_state;
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
        wait_all();
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

    void mock_app_on_remote_stroage(bool is_bulk_loading)
    {
        static const char *lock_state = "lock";
        static const char *unlock_state = "unlock";
        std::string path = _app_root;

        _meta_svc->get_meta_storage()->create_node(
            std::move(path), blob(lock_state, 0, strlen(lock_state)), [is_bulk_loading, this]() {
                std::cout << "create app root" << _app_root << std::endl;
            });
        wait_all();

        app_info info;
        info.app_id = 7;
        info.app_name = "half_bulk_load_downloading";
        info.app_type = "pegasus";
        info.is_stateful = true;
        info.is_bulk_loading = is_bulk_loading;
        info.max_replica_count = 3;
        info.partition_count = 4;
        info.status = app_status::AS_AVAILABLE;
        blob value = dsn::json::json_forwarder<app_info>::encode(info);

        _meta_svc->get_meta_storage()->create_node(
            _app_root + "/" + boost::lexical_cast<std::string>(info.app_id),
            std::move(value),
            [info, this]() {
                std::cout << "create app " << info.app_name << "(" << info.app_id << ") dir succeed"
                          << std::endl;
                for (int i = 0; i < info.partition_count; ++i) {
                    partition_configuration config;
                    config.max_replica_count = 3;
                    config.pid = gpid(info.app_id, i);
                    blob v = dsn::json::json_forwarder<partition_configuration>::encode(config);
                    _meta_svc->get_meta_storage()->create_node(
                        _app_root + "/" + boost::lexical_cast<std::string>(info.app_id) + "/" +
                            boost::lexical_cast<std::string>(i),
                        std::move(v),
                        [info, i, this]() {
                            std::cout << "create app " << info.app_name << " gpid(" << info.app_id
                                      << "." << i << ") dir succeed" << std::endl;
                        });
                }
            });
        wait_all();

        std::string app_root = _app_root;
        _meta_svc->get_meta_storage()->set_data(
            std::move(app_root), blob(unlock_state, 0, strlen(unlock_state)), []() {});
        wait_all();
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

    std::shared_ptr<app_state> find_app(const std::string &name) { return _state->get_app(name); }

    void wait_all() { _meta_svc->tracker()->wait_outstanding_tasks(); }

    std::shared_ptr<server_state> _state;
    std::unique_ptr<meta_service> _meta_svc;
    std::string _app_root;

    bulk_load_status::type get_app_bulk_load_status(uint32_t app_id)
    {
        return bulk_svc()->get_app_bulk_load_status(app_id);
    }
};

class bulk_load_start_test : public meta_bulk_load_service_test
{
public:
    bulk_load_start_test() {}

    void SetUp()
    {
        meta_bulk_load_service_test::SetUp();
        create_app(NAME);
        fail::setup();
        fail::cfg("meta_bulk_load_request_params_check", "return()");
    }

    void TearDown()
    {
        fail::teardown();
        meta_bulk_load_service_test::TearDown();
    }
};

TEST_F(bulk_load_start_test, wrong_app)
{
    auto resp = start_bulk_load("table_not_exist", CLUSTER, PROVIDER);
    ASSERT_EQ(resp.err, ERR_APP_NOT_EXIST);
}

// TODO(heyuchen): add ut for request_params_check

TEST_F(bulk_load_start_test, success)
{
    fail::cfg("meta_bulk_load_partition_bulk_load", "return()");

    auto resp = start_bulk_load(NAME, CLUSTER, PROVIDER);
    ASSERT_EQ(resp.err, ERR_OK);
    std::shared_ptr<app_state> app = find_app(NAME);
    ASSERT_EQ(app->is_bulk_loading, true);
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
        fail::setup();
        fail::cfg("meta_bulk_load_partition_bulk_load", "return()");
    }

    void TearDown()
    {
        fail::teardown();
        meta_bulk_load_service_test::TearDown();
    }
};

TEST_F(bulk_load_sync_apps_test, only_app_bulk_load_exist)
{
    meta_bulk_load_service_test::setup_without_create_bulk_load_dir(true, true, true);
    bulk_svc()->create_bulk_load_dir_on_remote_stroage();
    wait_all();
}

TEST_F(bulk_load_sync_apps_test, partition_bulk_load_half_exist)
{
    meta_bulk_load_service_test::setup_without_create_bulk_load_dir(true, true, false);
    bulk_svc()->create_bulk_load_dir_on_remote_stroage();
    wait_all();
}

TEST_F(bulk_load_sync_apps_test, status_inconsistency_wrong_app_status)
{
    meta_bulk_load_service_test::setup_without_create_bulk_load_dir(true, false, false);
    bulk_svc()->create_bulk_load_dir_on_remote_stroage();
    wait_all();

    std::shared_ptr<app_state> app = find_app("half_bulk_load_downloading");
    ASSERT_EQ(app->is_bulk_loading, false);
}

TEST_F(bulk_load_sync_apps_test, status_inconsistency_wrong_bulk_load_dir)
{
    meta_bulk_load_service_test::setup_without_create_bulk_load_dir(false, true, true);
    bulk_svc()->create_bulk_load_dir_on_remote_stroage();
    wait_all();

    std::shared_ptr<app_state> app = find_app("half_bulk_load_downloading");
    ASSERT_EQ(app->is_bulk_loading, false);
}

class bulk_load_partition_bulk_load_test : public meta_bulk_load_service_test
{
public:
    bulk_load_partition_bulk_load_test() {}

    void SetUp()
    {
        meta_bulk_load_service_test::SetUp();
        create_app(NAME);

        fail::setup();
        fail::cfg("meta_bulk_load_request_params_check", "return()");
        fail::cfg("meta_bulk_load_partition_bulk_load", "return()");

        auto resp = start_bulk_load(NAME, CLUSTER, PROVIDER);
        ASSERT_EQ(resp.err, ERR_OK);
        std::shared_ptr<app_state> app = find_app(NAME);
        _app_id = app->app_id;
        _partition_count = app->partition_count;
        ASSERT_EQ(app->is_bulk_loading, true);
    }

    void TearDown()
    {
        fail::teardown();
        meta_bulk_load_service_test::TearDown();
    }

    void create_basic_response(error_code err, bulk_load_status::type status, uint32_t pidx)
    {
        _resp.app_name = NAME;
        _resp.pid = gpid(_app_id, pidx);
        _resp.err = err;
        _resp.primary_bulk_load_status = status;
    }

    void mock_response_progress(error_code progress_err, bool finish_download, uint32_t pidx)
    {
        create_basic_response(ERR_OK, bulk_load_status::BLS_DOWNLOADING, pidx);
        partition_download_progress progress;
        progress.pid = gpid(_app_id, pidx);
        progress.progress = 100;
        progress.status = ERR_OK;

        _resp.__isset.download_progresses = true;
        _resp.__isset.total_download_progress = true;
        _resp.download_progresses[rpc_address("127.0.0.1", 10085)] = progress;
        _resp.download_progresses[_primary] = progress;

        if (finish_download) {
            _resp.download_progresses[rpc_address("127.0.0.1", 10087)] = progress;
            _resp.total_download_progress = 100;
        } else {
            progress.progress = 0;
            _resp.download_progresses[rpc_address("127.0.0.1", 10087)] = progress;
            _resp.total_download_progress = 66;
        }

        if (progress_err != ERR_OK) {
            progress.status = progress_err;
            _resp.download_progresses[rpc_address("127.0.0.1", 10087)] = progress;
        }
    }

    void mock_response_cleanup_flag(bool finish_cleanup, uint32_t pidx)
    {
        create_basic_response(ERR_OK, bulk_load_status::BLS_FAILED, pidx);
        _resp.__isset.is_group_bulk_load_context_cleaned = true;
        _resp.is_group_bulk_load_context_cleaned = finish_cleanup;
    }

    uint32_t _app_id;
    uint32_t _partition_count;
    bulk_load_response _resp;
    rpc_address _primary = rpc_address("127.0.0.1", 10086);
};

TEST_F(bulk_load_partition_bulk_load_test, response_fs_error)
{
    create_basic_response(ERR_FS_INTERNAL, bulk_load_status::BLS_DOWNLOADING, 0);
    auto response = _resp;
    bulk_svc()->on_partition_bulk_load_reply(ERR_OK, std::move(response), _resp.pid, _primary);
    wait_all();
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);
}

TEST_F(bulk_load_partition_bulk_load_test, downloading_corrupt)
{
    mock_response_progress(ERR_CORRUPTION, false, 0);
    auto response = _resp;
    bulk_svc()->on_partition_bulk_load_reply(ERR_OK, std::move(response), _resp.pid, _primary);
    wait_all();
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);
}

TEST_F(bulk_load_partition_bulk_load_test, downloading)
{
    mock_response_progress(ERR_OK, false, 0);
    auto response = _resp;
    bulk_svc()->on_partition_bulk_load_reply(ERR_OK, std::move(response), _resp.pid, _primary);
    wait_all();
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
}

TEST_F(bulk_load_partition_bulk_load_test, finish_download)
{
    for (int i = 0; i < _partition_count; ++i) {
        mock_response_progress(ERR_OK, true, i);
        auto response = _resp;
        bulk_svc()->on_partition_bulk_load_reply(ERR_OK, std::move(response), _resp.pid, _primary);
    }
    wait_all();
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADED);
}

TEST_F(bulk_load_partition_bulk_load_test, cleanup_half)
{
    mock_response_progress(ERR_CORRUPTION, false, 0);
    auto response = _resp;
    bulk_svc()->on_partition_bulk_load_reply(ERR_OK, std::move(response), _resp.pid, _primary);
    wait_all();
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);

    mock_response_cleanup_flag(false, 0);
    auto resp = _resp;
    bulk_svc()->on_partition_bulk_load_reply(ERR_OK, std::move(resp), _resp.pid, _primary);
    wait_all();
}

TEST_F(bulk_load_partition_bulk_load_test, finish_cleanup)
{
    mock_response_progress(ERR_CORRUPTION, false, 0);
    auto response = _resp;
    bulk_svc()->on_partition_bulk_load_reply(ERR_OK, std::move(response), _resp.pid, _primary);
    wait_all();
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);

    for (int i = 0; i < _partition_count; ++i) {
        mock_response_cleanup_flag(true, i);
        auto resp = _resp;
        bulk_svc()->on_partition_bulk_load_reply(ERR_OK, std::move(resp), _resp.pid, _primary);
    }
    wait_all();
    std::shared_ptr<app_state> app = find_app(NAME);
    ASSERT_EQ(app->is_bulk_loading, false);
}

} // namespace replication
} // namespace dsn
