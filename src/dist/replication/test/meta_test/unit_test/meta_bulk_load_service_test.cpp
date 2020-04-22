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
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/fail_point.h>

#include "dist/replication/meta_server/meta_server_failure_detector.h"
#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/meta_bulk_load_service.h"
#include "meta_service_test_app.h"

namespace dsn {
namespace replication {
class bulk_load_service_test : public ::testing::Test
{
public:
    bulk_load_service_test() {}

    void SetUp() override { initialize_meta_server(); }

    void TearDown() override
    {
        if (_state && _meta_svc) {
            meta_service_test_app::delete_all_on_meta_storage(_meta_svc.get());
        }

        _state.reset();
        _meta_svc.reset(nullptr);
    }

    /// initialize functions

    void initialize_meta_server()
    {
        _meta_svc.reset(meta_service_test_app::initialize_meta_service());
        _meta_svc->set_function_level(meta_function_level::fl_steady);
        _meta_svc->_failure_detector.reset(new meta_server_failure_detector(_meta_svc.get()));

        // initialize bulk load service
        _meta_svc->_bulk_load_svc.reset(new bulk_load_service(
            _meta_svc.get(),
            meta_options::concat_path_unix_style(_meta_svc->_cluster_root, "bulk_load")));
        _meta_svc->_bulk_load_svc->initialize_bulk_load_service();

        _state = _meta_svc->_state;
        _app_root = _state->_apps_root;
    }

    void initialize_meta_server_with_mock_bulk_load(
        std::unordered_set<int32_t> app_id_set,
        std::unordered_map<app_id, app_bulk_load_info> app_bulk_load_info_map,
        std::unordered_map<app_id, std::unordered_map<int32_t, partition_bulk_load_info>>
            partition_bulk_load_info_map,
        std::vector<app_info> app_list)
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
        _meta_svc->_bulk_load_svc.reset(new bulk_load_service(
            _meta_svc.get(),
            meta_options::concat_path_unix_style(_meta_svc->_cluster_root, "bulk_load")));
        mock_bulk_load_on_remote_storage(
            app_id_set, app_bulk_load_info_map, partition_bulk_load_info_map);

        // mock app
        for (auto iter = app_list.begin(); iter != app_list.end(); ++iter) {
            mock_app_on_remote_stroage(*iter);
        }
        state->initialize_data_structure();

        _meta_svc->set_function_level(meta_function_level::fl_steady);
        _meta_svc->_failure_detector.reset(new meta_server_failure_detector(_meta_svc.get()));
        _state = _meta_svc->_state;
    }

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

    /// mock structure functions

    void mock_bulk_load_on_remote_storage(
        std::unordered_set<int32_t> app_id_set,
        std::unordered_map<app_id, app_bulk_load_info> app_bulk_load_info_map,
        std::unordered_map<app_id, std::unordered_map<int32_t, partition_bulk_load_info>>
            partition_bulk_load_info_map)
    {
        std::string path = bulk_svc()->_bulk_load_root;
        blob value = blob();
        // create bulk_load_root
        _meta_svc->get_meta_storage()->create_node(
            std::move(path),
            std::move(value),
            [this, &app_id_set, &app_bulk_load_info_map, &partition_bulk_load_info_map]() {
                for (auto iter = app_id_set.begin(); iter != app_id_set.end(); ++iter) {
                    mock_app_bulk_load_info_on_remote_stroage(app_bulk_load_info_map[*iter],
                                                              partition_bulk_load_info_map[*iter]);
                }
            });
        wait_all();
    }

    void mock_app_bulk_load_info_on_remote_stroage(
        app_bulk_load_info ainfo,
        std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map)
    {
        std::string app_path = bulk_svc()->get_app_bulk_load_path(ainfo.app_id);
        blob value = dsn::json::json_forwarder<app_bulk_load_info>::encode(ainfo);
        // create app_bulk_load_info
        _meta_svc->get_meta_storage()->create_node(
            std::move(app_path),
            std::move(value),
            [this, app_path, ainfo, &partition_bulk_load_info_map]() {
                ddebug_f("create app({}) app_id={} bulk load dir({}), bulk_load_status={}",
                         ainfo.app_name,
                         ainfo.app_id,
                         app_path,
                         dsn::enum_to_string(ainfo.status));
                for (auto iter = partition_bulk_load_info_map.begin();
                     iter != partition_bulk_load_info_map.end();
                     ++iter) {
                    gpid pid = gpid(ainfo.app_id, iter->first);
                    mock_partition_bulk_load_info_on_remote_stroage(pid, iter->second);
                }
            });
    }

    void mock_partition_bulk_load_info_on_remote_stroage(gpid pid, partition_bulk_load_info pinfo)
    {
        blob value = dsn::json::json_forwarder<partition_bulk_load_info>::encode(pinfo);
        std::string partition_path = bulk_svc()->get_partition_bulk_load_path(pid);
        _meta_svc->get_meta_storage()->create_node(
            std::move(partition_path), std::move(value), [this, partition_path, pid, pinfo]() {
                ddebug_f("create partition[{}] bulk load dir({}), bulk_load_status={}",
                         pid.to_string(),
                         partition_path,
                         dsn::enum_to_string(pinfo.status));
            });
    }

    void mock_app_on_remote_stroage(app_info &info)
    {
        static const char *lock_state = "lock";
        static const char *unlock_state = "unlock";
        std::string path = _app_root;

        _meta_svc->get_meta_storage()->create_node(
            std::move(path), blob(lock_state, 0, strlen(lock_state)), [this]() {
                ddebug_f("create app root {}", _app_root);
            });
        wait_all();

        blob value = dsn::json::json_forwarder<app_info>::encode(info);
        _meta_svc->get_meta_storage()->create_node(
            _app_root + "/" + boost::lexical_cast<std::string>(info.app_id),
            std::move(value),
            [this, info]() {
                ddebug_f("create app({}) app_id={}, dir succeed", info.app_name, info.app_id);
                for (int i = 0; i < info.partition_count; ++i) {
                    partition_configuration config;
                    config.max_replica_count = 3;
                    config.pid = gpid(info.app_id, i);
                    config.ballot = BALLOT;
                    blob v = dsn::json::json_forwarder<partition_configuration>::encode(config);
                    _meta_svc->get_meta_storage()->create_node(
                        _app_root + "/" + boost::lexical_cast<std::string>(info.app_id) + "/" +
                            boost::lexical_cast<std::string>(i),
                        std::move(v),
                        [info, i, this]() {
                            ddebug_f("create app({}), partition({}.{}) dir succeed",
                                     info.app_name,
                                     info.app_id,
                                     i);
                        });
                }
            });
        wait_all();

        std::string app_root = _app_root;
        _meta_svc->get_meta_storage()->set_data(
            std::move(app_root), blob(unlock_state, 0, strlen(unlock_state)), []() {});
        wait_all();
    }

    void update_partition_config_on_remote_storage(partition_configuration &config,
                                                   std::string &app_name)
    {
        gpid pid = config.pid;
        blob v = dsn::json::json_forwarder<partition_configuration>::encode(config);
        _meta_svc->get_meta_storage()->set_data(
            _app_root + "/" + boost::lexical_cast<std::string>(pid.get_app_id()) + "/" +
                boost::lexical_cast<std::string>(pid.get_partition_index()),
            std::move(v),
            [app_name, pid, config, this]() {
                std::shared_ptr<app_state> app = find_app(APP_NAME);
                app->partitions[pid.get_partition_index()] = config;
                ddebug_f("update app({}), partition({}) succeed", app_name, pid.to_string());
            });
    }

    void mock_meta_bulk_load_context(int32_t app_id,
                                     int32_t partition_count,
                                     bulk_load_status::type status)
    {
        bulk_svc()->_bulk_load_app_id.insert(app_id);
        bulk_svc()->_apps_in_progress_count[app_id] = partition_count;
        bulk_svc()->_app_bulk_load_info[app_id].status = status;
        for (int i = 0; i < partition_count; ++i) {
            gpid pid = gpid(app_id, i);
            bulk_svc()->_partition_bulk_load_info[pid].status = status;
        }
    }

    /// bulk load functions

    start_bulk_load_response start_bulk_load(const std::string &app_name)
    {
        auto request = dsn::make_unique<start_bulk_load_request>();
        request->app_name = app_name;
        request->cluster_name = CLUSTER;
        request->file_provider_type = PROVIDER;

        start_bulk_load_rpc rpc(std::move(request), RPC_CM_START_BULK_LOAD);
        bulk_svc()->on_start_bulk_load(rpc);
        wait_all();
        return rpc.response();
    }

    error_code check_start_bulk_load_request_params(const std::string provider,
                                                    int32_t app_id,
                                                    int32_t partition_count)
    {
        std::string hint_msg;
        return bulk_svc()->check_bulk_load_request_params(
            APP_NAME, CLUSTER, provider, app_id, partition_count, hint_msg);
    }

    error_code query_bulk_load(const std::string &app_name)
    {
        auto request = dsn::make_unique<configuration_query_bulk_load_request>();
        request->app_name = app_name;

        query_bulk_load_rpc rpc(std::move(request), RPC_CM_QUERY_BULK_LOAD_STATUS);
        bulk_svc()->on_query_bulk_load_status(rpc);
        wait_all();
        return rpc.response().err;
    }

    error_code control_bulk_load(int32_t app_id,
                                 bulk_load_control_type::type type,
                                 bulk_load_status::type app_status)
    {
        bulk_svc()->_app_bulk_load_info[app_id].status = app_status;

        auto request = dsn::make_unique<configuration_control_bulk_load_request>();
        request->app_id = app_id;
        request->type = type;

        control_bulk_load_rpc rpc(std::move(request), RPC_CM_CONTROL_BULK_LOAD);
        bulk_svc()->on_control_bulk_load(rpc);
        wait_all();
        return rpc.response().err;
    }

    void on_partition_bulk_load_reply(error_code err,
                                      ballot b,
                                      bulk_load_response &response,
                                      const gpid &pid,
                                      const rpc_address &primary_addr)
    {
        bulk_svc()->on_partition_bulk_load_reply(
            err, APP_NAME, b, std::move(response), pid, primary_addr);
    }

    void reset_local_bulk_load_states(int32_t app_id, const std::string &app_name)
    {
        bulk_svc()->reset_local_bulk_load_states(app_id, app_name);
    }

    bulk_load_status::type get_app_bulk_load_status(int32_t app_id)
    {
        return bulk_svc()->get_app_bulk_load_status_unlock(app_id);
    }

    int32_t get_app_id_set_size() { return bulk_svc()->_bulk_load_app_id.size(); }

    int32_t get_partition_bulk_load_info_size(int32_t app_id)
    {
        int count = 0;
        std::unordered_map<gpid, partition_bulk_load_info> temp =
            bulk_svc()->_partition_bulk_load_info;
        for (auto iter = temp.begin(); iter != temp.end(); ++iter) {
            if (iter->first.get_app_id() == app_id) {
                ++count;
            }
        }
        return count;
    }

    int32_t get_app_in_process_count(int32_t app_id)
    {
        int32_t count = bulk_svc()->_apps_in_progress_count[app_id];
        return count;
    }

    bool app_is_bulk_loading(const std::string &app_name)
    {
        return find_app(app_name)->is_bulk_loading;
    }

    bool need_update_metadata(gpid pid)
    {
        return bulk_svc()->is_partition_metadata_not_updated(pid);
    }

    void on_partition_ingestion_reply(error_code err, ingestion_response &resp, const gpid &pid)
    {
        return bulk_svc()->on_partition_ingestion_reply(err, std::move(resp), APP_NAME, pid);
    }

    /// helper functions
    bulk_load_service *bulk_svc() { return _meta_svc->_bulk_load_svc.get(); }
    std::shared_ptr<app_state> find_app(const std::string &name) { return _state->get_app(name); }
    void wait_all() { _meta_svc->tracker()->wait_outstanding_tasks(); }

public:
    int32_t APP_ID = 1;
    std::string APP_NAME = "bulk_load_test";
    int32_t PARTITION_COUNT = 8;
    std::string CLUSTER = "cluster";
    std::string PROVIDER = "local_service";
    int64_t BALLOT = 4;

    std::string SYNC_APP_NAME = "bulk_load_failover_table";
    int32_t SYNC_APP_ID = 2;
    int32_t SYNC_PARTITION_COUNT = 4;

    std::shared_ptr<server_state> _state;
    std::unique_ptr<meta_service> _meta_svc;
    std::string _app_root;
};

/// start bulk load unit tests
TEST_F(bulk_load_service_test, start_bulk_load_with_not_existed_app)
{
    auto resp = start_bulk_load("table_not_exist");
    ASSERT_EQ(resp.err, ERR_APP_NOT_EXIST);
}

TEST_F(bulk_load_service_test, start_bulk_load_with_wrong_provider)
{
    create_app(APP_NAME);
    error_code err = check_start_bulk_load_request_params("wrong_provider", 1, PARTITION_COUNT);
    ASSERT_EQ(err, ERR_INVALID_PARAMETERS);
}

TEST_F(bulk_load_service_test, start_bulk_load_succeed)
{
    create_app(APP_NAME);
    fail::setup();
    fail::cfg("meta_check_bulk_load_request_params", "return()");
    fail::cfg("meta_bulk_load_partition_bulk_load", "return()");

    auto resp = start_bulk_load(APP_NAME);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_TRUE(app_is_bulk_loading(APP_NAME));

    fail::teardown();
}

/// query bulk load status unit tests
TEST_F(bulk_load_service_test, query_bulk_load_status_with_wrong_state)
{
    create_app(APP_NAME);
    ASSERT_EQ(query_bulk_load(APP_NAME), ERR_INVALID_STATE);
}

TEST_F(bulk_load_service_test, query_bulk_load_status_success)
{
    create_app(APP_NAME);
    std::shared_ptr<app_state> app = find_app(APP_NAME);
    app->is_bulk_loading = true;
    ASSERT_EQ(query_bulk_load(APP_NAME), ERR_OK);
}

/// control bulk load unit tests
TEST_F(bulk_load_service_test, control_bulk_load_test)
{
    create_app(APP_NAME);
    std::shared_ptr<app_state> app = find_app(APP_NAME);
    app->is_bulk_loading = true;
    mock_meta_bulk_load_context(app->app_id, app->partition_count, bulk_load_status::BLS_INVALID);

    fail::setup();
    fail::cfg("meta_update_app_status_on_remote_storage_unlock", "return()");

    struct control_test
    {
        bulk_load_control_type::type type;
        bulk_load_status::type app_status;
        error_code expected_err;
    } tests[] = {
        {bulk_load_control_type::BLC_PAUSE, bulk_load_status::BLS_DOWNLOADING, ERR_OK},
        {bulk_load_control_type::BLC_PAUSE, bulk_load_status::BLS_DOWNLOADED, ERR_INVALID_STATE},
        {bulk_load_control_type::BLC_RESTART, bulk_load_status::BLS_PAUSED, ERR_OK},
        {bulk_load_control_type::BLC_RESTART, bulk_load_status::BLS_PAUSING, ERR_INVALID_STATE},
        {bulk_load_control_type::BLC_CANCEL, bulk_load_status::BLS_DOWNLOADING, ERR_OK},
        {bulk_load_control_type::BLC_CANCEL, bulk_load_status::BLS_PAUSED, ERR_OK},
        {bulk_load_control_type::BLC_CANCEL, bulk_load_status::BLS_INGESTING, ERR_INVALID_STATE},
        {bulk_load_control_type::BLC_FORCE_CANCEL, bulk_load_status::BLS_SUCCEED, ERR_OK},
    };

    for (auto test : tests) {
        ASSERT_EQ(control_bulk_load(app->app_id, test.type, test.app_status), test.expected_err);
    }
    reset_local_bulk_load_states(app->app_id, app->app_name);

    fail::teardown();
}

/// bulk load process unit tests
class bulk_load_process_test : public bulk_load_service_test
{
public:
    bulk_load_process_test() {}

    void SetUp()
    {
        bulk_load_service_test::SetUp();
        create_app(APP_NAME);

        fail::setup();
        fail::cfg("meta_check_bulk_load_request_params", "return()");
        fail::cfg("meta_bulk_load_partition_bulk_load", "return()");

        auto resp = start_bulk_load(APP_NAME);
        ASSERT_EQ(resp.err, ERR_OK);
        std::shared_ptr<app_state> app = find_app(APP_NAME);
        _app_id = app->app_id;
        _partition_count = app->partition_count;
        ASSERT_EQ(app->is_bulk_loading, true);
    }

    void TearDown()
    {
        fail::teardown();
        bulk_load_service_test::TearDown();
    }

    void create_basic_response(error_code err, bulk_load_status::type status, int32_t pidx = 0)
    {
        _resp.app_name = APP_NAME;
        _resp.pid = gpid(_app_id, pidx);
        _resp.err = err;
        _resp.primary_bulk_load_status = status;
    }

    void mock_response_progress(error_code progress_err, bool finish_download)
    {
        create_basic_response(ERR_OK, bulk_load_status::BLS_DOWNLOADING, _pidx);
        partition_download_progress progress;
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

    void mock_response_bulk_load_metadata()
    {
        mock_response_progress(ERR_OK, false);

        file_meta f_meta;
        f_meta.name = "mock_remote_file";
        f_meta.size = 100;
        f_meta.md5 = "mock_md5";

        bulk_load_metadata metadata;
        metadata.files.emplace_back(f_meta);
        metadata.file_total_size = 100;

        _resp.__set_metadata(metadata);
    }

    void mock_response_ingestion_status(ingestion_status::type secondary_istatus)
    {
        create_basic_response(ERR_OK, bulk_load_status::BLS_INGESTING, _pidx);

        _resp.__isset.group_ingestion_status = true;
        _resp.group_ingestion_status[_primary] = ingestion_status::IS_SUCCEED;
        _resp.group_ingestion_status[rpc_address("127.0.0.1", 10085)] =
            ingestion_status::IS_SUCCEED;
        _resp.group_ingestion_status[rpc_address("127.0.0.1", 10087)] = secondary_istatus;

        _resp.__set_is_group_ingestion_finished(secondary_istatus == ingestion_status::IS_SUCCEED);
    }

    void mock_response_cleanup_flag(bool finish_cleanup, bulk_load_status::type status)
    {
        create_basic_response(ERR_OK, status, _pidx);
        _resp.__isset.is_group_bulk_load_context_cleaned = true;
        _resp.is_group_bulk_load_context_cleaned = finish_cleanup;
    }

    void mock_response_paused(bool is_group_paused)
    {
        create_basic_response(ERR_OK, bulk_load_status::BLS_PAUSED, _pidx);
        _resp.__isset.is_group_bulk_load_paused = true;
        _resp.is_group_bulk_load_paused = is_group_paused;
    }

    void test_on_partition_bulk_load_reply(int32_t in_progress_count,
                                           bulk_load_status::type status,
                                           error_code resp_err = ERR_OK)
    {
        mock_meta_bulk_load_context(_app_id, in_progress_count, status);
        auto response = _resp;
        response.err = resp_err;
        on_partition_bulk_load_reply(ERR_OK, BALLOT, response, gpid(_app_id, _pidx), _primary);
        wait_all();
    }

    void mock_ingestion_context(error_code err, int32_t rocksdb_err, int32_t in_progress_count)
    {
        mock_meta_bulk_load_context(_app_id, in_progress_count, bulk_load_status::BLS_INGESTING);
        _ingest_resp.err = err;
        _ingest_resp.rocksdb_error = rocksdb_err;
    }

    void test_on_partition_ingestion_reply(error_code rpc_err = ERR_OK)
    {
        on_partition_ingestion_reply(rpc_err, _ingest_resp, gpid(_app_id, _pidx));
        wait_all();
    }

    int32_t _app_id = 3;
    int32_t _pidx = 0;
    int32_t _partition_count = 4;
    bulk_load_response _resp;
    rpc_address _primary = rpc_address("127.0.0.1", 10086);
    ingestion_response _ingest_resp;
};

/// on_partition_bulk_load_reply unit tests

TEST_F(bulk_load_process_test, downloading_fs_error)
{
    test_on_partition_bulk_load_reply(
        _partition_count, bulk_load_status::BLS_DOWNLOADING, ERR_FS_INTERNAL);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);
}

TEST_F(bulk_load_process_test, downloading_busy)
{
    test_on_partition_bulk_load_reply(
        _partition_count, bulk_load_status::BLS_DOWNLOADING, ERR_BUSY);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
}

TEST_F(bulk_load_process_test, downloading_corrupt)
{
    mock_response_progress(ERR_CORRUPTION, false);
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);
}

TEST_F(bulk_load_process_test, normal_downloading)
{
    mock_response_progress(ERR_OK, false);
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
}

TEST_F(bulk_load_process_test, downloading_report_metadata)
{
    mock_response_bulk_load_metadata();
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_DOWNLOADING);

    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_FALSE(need_update_metadata(gpid(_app_id, _pidx)));
}

TEST_F(bulk_load_process_test, downloaded_succeed)
{
    mock_response_progress(ERR_OK, true);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADED);
}

TEST_F(bulk_load_process_test, normal_ingesting)
{
    fail::cfg("meta_bulk_load_partition_ingestion", "return()");
    mock_response_progress(ERR_OK, true);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_DOWNLOADED);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_INGESTING);
}

TEST_F(bulk_load_process_test, ingestion_running)
{
    mock_response_ingestion_status(ingestion_status::IS_RUNNING);
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_INGESTING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_INGESTING);
}

TEST_F(bulk_load_process_test, ingestion_error)
{
    mock_response_ingestion_status(ingestion_status::IS_FAILED);
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_INGESTING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);
}

TEST_F(bulk_load_process_test, normal_succeed)
{
    mock_response_ingestion_status(ingestion_status::IS_SUCCEED);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_INGESTING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_SUCCEED);
}

TEST_F(bulk_load_process_test, half_cleanup)
{
    mock_response_cleanup_flag(false, bulk_load_status::BLS_FAILED);
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_FAILED);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);
}

TEST_F(bulk_load_process_test, cleanup_succeed)
{
    mock_response_cleanup_flag(true, bulk_load_status::BLS_SUCCEED);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_SUCCEED);

    ASSERT_FALSE(app_is_bulk_loading(APP_NAME));
}

TEST_F(bulk_load_process_test, pausing)
{
    mock_response_paused(false);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_PAUSING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_PAUSING);
}

TEST_F(bulk_load_process_test, pause_succeed)
{
    mock_response_paused(true);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_PAUSING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_PAUSED);
}

TEST_F(bulk_load_process_test, rpc_error)
{
    mock_meta_bulk_load_context(_app_id, _partition_count, bulk_load_status::BLS_DOWNLOADED);
    on_partition_bulk_load_reply(ERR_TIMEOUT, BALLOT, _resp, gpid(_app_id, _pidx), _primary);
    wait_all();
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_in_process_count(_app_id), _partition_count);
}

TEST_F(bulk_load_process_test, response_invalid_state)
{
    test_on_partition_bulk_load_reply(
        _partition_count, bulk_load_status::BLS_SUCCEED, ERR_INVALID_STATE);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_in_process_count(_app_id), _partition_count);
}

TEST_F(bulk_load_process_test, response_object_not_found)
{
    test_on_partition_bulk_load_reply(
        _partition_count, bulk_load_status::BLS_CANCELED, ERR_OBJECT_NOT_FOUND);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_CANCELED);
    ASSERT_EQ(get_app_in_process_count(_app_id), _partition_count);
}

/// on_partition_ingestion_reply unit tests
TEST_F(bulk_load_process_test, ingest_rpc_error)
{
    mock_ingestion_context(ERR_OK, 1, _partition_count);
    test_on_partition_ingestion_reply(ERR_TIMEOUT);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
}

TEST_F(bulk_load_process_test, ingest_empty_write_error)
{
    fail::cfg("meta_bulk_load_partition_ingestion", "return()");
    mock_ingestion_context(ERR_TRY_AGAIN, 11, _partition_count);
    test_on_partition_ingestion_reply();
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_INGESTING);
}

TEST_F(bulk_load_process_test, ingest_wrong)
{
    mock_ingestion_context(ERR_OK, 1, _partition_count);
    on_partition_ingestion_reply(ERR_OK, _ingest_resp, gpid(_app_id, _pidx));
    wait_all();
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);
}

TEST_F(bulk_load_process_test, ingest_succeed)
{
    mock_ingestion_context(ERR_OK, 0, 1);
    test_on_partition_ingestion_reply();
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_INGESTING);
}

class bulk_load_failover_test : public bulk_load_service_test
{
public:
    bulk_load_failover_test() {}

    void SetUp()
    {
        fail::setup();
        fail::cfg("meta_bulk_load_partition_bulk_load", "return()");
        fail::cfg("meta_bulk_load_partition_ingestion", "return()");
    }

    void TearDown()
    {
        clean_up();
        fail::teardown();
        bulk_load_service_test::TearDown();
    }

    void try_to_continue_bulk_load(bulk_load_status::type app_status, bool is_bulk_loading = true)
    {
        prepare_bulk_load_structures(SYNC_APP_ID,
                                     SYNC_PARTITION_COUNT,
                                     SYNC_APP_NAME,
                                     app_status,
                                     _pstatus_map,
                                     is_bulk_loading);
        initialize_meta_server_with_mock_bulk_load(
            _app_id_set, _app_bulk_load_info_map, _partition_bulk_load_info_map, _app_info);
        bulk_svc()->initialize_bulk_load_service();
        wait_all();
    }

    void
    prepare_bulk_load_structures(int32_t app_id,
                                 int32_t partition_count,
                                 std::string &app_name,
                                 bulk_load_status::type app_status,
                                 std::unordered_map<int32_t, bulk_load_status::type> pstatus_map,
                                 bool is_bulk_loading)
    {
        _app_id_set.insert(app_id);
        mock_app_bulk_load_info(app_id, partition_count, app_name, app_status);
        mock_partition_bulk_load_info(app_id, pstatus_map);
        mock_app_info(app_id, partition_count, app_name, is_bulk_loading);
    }

    void mock_app_bulk_load_info(int32_t app_id,
                                 int32_t partition_count,
                                 std::string &app_name,
                                 bulk_load_status::type status)
    {
        app_bulk_load_info ainfo;
        ainfo.app_id = app_id;
        ainfo.app_name = app_name;
        ainfo.cluster_name = CLUSTER;
        ainfo.file_provider_type = PROVIDER;
        ainfo.partition_count = partition_count;
        ainfo.status = status;
        _app_bulk_load_info_map[app_id] = ainfo;
    }

    void
    mock_partition_bulk_load_info(int32_t app_id,
                                  std::unordered_map<int32_t, bulk_load_status::type> pstatus_map)
    {
        if (pstatus_map.size() <= 0) {
            return;
        }
        std::unordered_map<int32_t, partition_bulk_load_info> pinfo_map;
        for (auto iter = pstatus_map.begin(); iter != pstatus_map.end(); ++iter) {
            partition_bulk_load_info pinfo;
            pinfo.status = iter->second;
            pinfo_map[iter->first] = pinfo;
        }
        _partition_bulk_load_info_map[app_id] = pinfo_map;
    }

    void mock_app_info(int32_t app_id,
                       int32_t partition_count,
                       std::string &app_name,
                       bool is_bulk_loading)
    {
        app_info ainfo;
        ainfo.app_id = app_id;
        ainfo.app_name = app_name;
        ainfo.app_type = "pegasus";
        ainfo.is_stateful = true;
        ainfo.is_bulk_loading = is_bulk_loading;
        ainfo.max_replica_count = 3;
        ainfo.partition_count = partition_count;
        ainfo.status = app_status::AS_AVAILABLE;
        _app_info.emplace_back(ainfo);
    }

    void mock_pstatus_map(bulk_load_status::type status, int32_t end_index, int32_t start_index = 0)
    {
        for (auto i = start_index; i <= end_index; ++i) {
            _pstatus_map[i] = status;
        }
    }

    void clean_up()
    {
        _app_info.clear();
        _app_bulk_load_info_map.clear();
        _partition_bulk_load_info_map.clear();
        _pstatus_map.clear();
        _app_id_set.clear();
    }

    std::vector<app_info> _app_info;
    std::unordered_set<int32_t> _app_id_set;
    std::unordered_map<app_id, app_bulk_load_info> _app_bulk_load_info_map;
    std::unordered_map<app_id, std::unordered_map<int32_t, partition_bulk_load_info>>
        _partition_bulk_load_info_map;
    std::unordered_map<int32_t, bulk_load_status::type> _pstatus_map;
};

TEST_F(bulk_load_failover_test, sync_bulk_load)
{
    fail::setup();
    fail::cfg("meta_try_to_continue_bulk_load", "return()");

    // mock app downloading with partition[0~1] downloading
    std::unordered_map<int32_t, bulk_load_status::type> partition_bulk_load_status_map;
    partition_bulk_load_status_map[0] = bulk_load_status::BLS_DOWNLOADING;
    partition_bulk_load_status_map[1] = bulk_load_status::BLS_DOWNLOADING;
    prepare_bulk_load_structures(SYNC_APP_ID,
                                 SYNC_PARTITION_COUNT,
                                 SYNC_APP_NAME,
                                 bulk_load_status::BLS_DOWNLOADING,
                                 partition_bulk_load_status_map,
                                 true);

    // mock app failed with no partition existed
    partition_bulk_load_status_map.clear();
    prepare_bulk_load_structures(APP_ID,
                                 PARTITION_COUNT,
                                 APP_NAME,
                                 bulk_load_status::type::BLS_FAILED,
                                 partition_bulk_load_status_map,
                                 true);

    initialize_meta_server_with_mock_bulk_load(
        _app_id_set, _app_bulk_load_info_map, _partition_bulk_load_info_map, _app_info);
    bulk_svc()->initialize_bulk_load_service();
    wait_all();

    ASSERT_EQ(get_app_id_set_size(), 2);

    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
    ASSERT_EQ(get_app_bulk_load_status(SYNC_APP_ID), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_partition_bulk_load_info_size(SYNC_APP_ID), 2);

    ASSERT_TRUE(app_is_bulk_loading(APP_NAME));
    ASSERT_EQ(get_app_bulk_load_status(APP_ID), bulk_load_status::BLS_FAILED);
    ASSERT_EQ(get_partition_bulk_load_info_size(APP_ID), 0);

    fail::teardown();
}

/// check_app_bulk_load_consistency unit test
TEST_F(bulk_load_failover_test, status_inconsistency_wrong_app_status)
{
    // create app(is_bulk_loading=true), but no bulk load info on remote storage
    mock_app_info(SYNC_APP_ID, SYNC_PARTITION_COUNT, SYNC_APP_NAME, true);
    initialize_meta_server_with_mock_bulk_load(
        _app_id_set, _app_bulk_load_info_map, _partition_bulk_load_info_map, _app_info);
    bulk_svc()->initialize_bulk_load_service();
    wait_all();

    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

TEST_F(bulk_load_failover_test, status_inconsistency_wrong_bulk_load_dir)
{
    try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADED, false);
    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

/// try_to_continue_bulk_load unit test
TEST_F(bulk_load_failover_test, app_info_inconsistency)
{
    // bulk load partition_count = SYNC_PARTITION_COUNT, but current partition_count =
    // PARTITION_COUNT
    prepare_bulk_load_structures(SYNC_APP_ID,
                                 PARTITION_COUNT,
                                 SYNC_APP_NAME,
                                 bulk_load_status::BLS_DOWNLOADED,
                                 _pstatus_map,
                                 true);
    _app_bulk_load_info_map[SYNC_APP_ID].partition_count = SYNC_PARTITION_COUNT;
    initialize_meta_server_with_mock_bulk_load(
        _app_id_set, _app_bulk_load_info_map, _partition_bulk_load_info_map, _app_info);
    bulk_svc()->initialize_bulk_load_service();
    wait_all();

    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:downloading, partition[0,1]=downloading, partition[2,3] not existed
TEST_F(bulk_load_failover_test, lack_of_partition_with_downloading)
{
    mock_pstatus_map(bulk_load_status::BLS_DOWNLOADING, 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:download, partition[0]=downloaded, partition[1~3] not existed
TEST_F(bulk_load_failover_test, lack_of_partition_with_downloaded)
{
    _pstatus_map[0] = bulk_load_status::BLS_DOWNLOADED;
    try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADED);
    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:ingesting, all partition not exist
TEST_F(bulk_load_failover_test, lack_of_partition_with_ingestion)
{
    try_to_continue_bulk_load(bulk_load_status::BLS_INGESTING);
    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:succeed, partition[0,1]=succeed, partition[2~3] not existed
TEST_F(bulk_load_failover_test, lack_of_partition_with_succeed)
{
    mock_pstatus_map(bulk_load_status::BLS_SUCCEED, 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_SUCCEED);
    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:failed, partition[0~2]=failed, partition[3] not existed
TEST_F(bulk_load_failover_test, lack_of_partition_with_failed)
{
    mock_pstatus_map(bulk_load_status::BLS_FAILED, 2);
    try_to_continue_bulk_load(bulk_load_status::BLS_FAILED);
    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:pausing, partition[0]=pausing, partition[1~3] not existed
TEST_F(bulk_load_failover_test, lack_of_partition_with_pausing)
{
    _pstatus_map[0] = bulk_load_status::BLS_PAUSING;
    try_to_continue_bulk_load(bulk_load_status::BLS_PAUSING);
    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:paused, partition[0,1]=paused, partition[2,3] not existed
TEST_F(bulk_load_failover_test, lack_of_partition_with_paused)
{
    mock_pstatus_map(bulk_load_status::BLS_PAUSED, 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_PAUSED);
    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:cancel, partition[0~2]=pausing, partition[3] not existed
TEST_F(bulk_load_failover_test, lack_of_partition_with_cancel)
{
    mock_pstatus_map(bulk_load_status::BLS_CANCELED, 2);
    try_to_continue_bulk_load(bulk_load_status::BLS_CANCELED);
    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:downloading, partition[0~3]=downloading
TEST_F(bulk_load_failover_test, downloading_with_partition_all_downloading)
{
    mock_pstatus_map(bulk_load_status::BLS_DOWNLOADING, SYNC_PARTITION_COUNT - 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:downloading, partition[0~3]=downloaded
TEST_F(bulk_load_failover_test, downloading_with_partition_all_downloaded)
{
    mock_pstatus_map(bulk_load_status::BLS_DOWNLOADED, SYNC_PARTITION_COUNT - 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:downloading, partition[0]=downloaded, partition[1~3]=downloading
TEST_F(bulk_load_failover_test, downloading_with_partition_mixed)
{
    _pstatus_map[0] = bulk_load_status::BLS_DOWNLOADED;
    mock_pstatus_map(bulk_load_status::BLS_DOWNLOADING, SYNC_PARTITION_COUNT - 1, 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:downloading, all partition not exist
TEST_F(bulk_load_failover_test, downloading_with_partition_all_not_exist)
{
    try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:downloading, partition[0,1]=downloading, partition[2]=downloaded, partition[3] not exist
TEST_F(bulk_load_failover_test, downloading_with_partition_mix_wrong_status)
{
    mock_pstatus_map(bulk_load_status::BLS_DOWNLOADING, 1);
    _pstatus_map[2] = bulk_load_status::BLS_DOWNLOADED;
    try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADING);
    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:downloading, partition[0-3]=succeed
TEST_F(bulk_load_failover_test, downloading_with_rollback)
{
    mock_pstatus_map(bulk_load_status::BLS_SUCCEED, SYNC_PARTITION_COUNT - 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:downloaded, partition[0~3]=downloaded
TEST_F(bulk_load_failover_test, downloaded_with_partition_all_downloaded)
{
    mock_pstatus_map(bulk_load_status::BLS_DOWNLOADED, SYNC_PARTITION_COUNT - 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADED);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:downloaded, partition[0~3]=ingesting
TEST_F(bulk_load_failover_test, downloaded_with_partition_all_ingesting)
{
    mock_pstatus_map(bulk_load_status::BLS_INGESTING, SYNC_PARTITION_COUNT - 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADED);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), 0);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:downloaded, partition[0]=downloaded, partition[1~3]=ingesting
TEST_F(bulk_load_failover_test, downloaded_with_partition_mixed)
{
    _pstatus_map[0] = bulk_load_status::BLS_DOWNLOADED;
    mock_pstatus_map(bulk_load_status::BLS_INGESTING, SYNC_PARTITION_COUNT - 1, 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADED);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), 1);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:ingesting, partition[0~3]=ingesting
TEST_F(bulk_load_failover_test, ingesting_with_partition_all_ingesting)
{
    mock_pstatus_map(bulk_load_status::BLS_INGESTING, SYNC_PARTITION_COUNT - 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_INGESTING);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:ingesting, partition[0~3]=succeed
TEST_F(bulk_load_failover_test, ingesting_with_partition_all_succeed)
{
    mock_pstatus_map(bulk_load_status::BLS_SUCCEED, SYNC_PARTITION_COUNT - 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_INGESTING);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), 0);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:ingesting, partition[0]=succeed, partition[1~3]=ingesting
TEST_F(bulk_load_failover_test, ingesting_with_partition_mixed)
{
    _pstatus_map[0] = bulk_load_status::BLS_SUCCEED;
    mock_pstatus_map(bulk_load_status::BLS_INGESTING, SYNC_PARTITION_COUNT - 1, 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_INGESTING);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), 3);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:succeed, partition[0~3]=succeed
TEST_F(bulk_load_failover_test, succeed_with_partition_all_succeed)
{
    mock_pstatus_map(bulk_load_status::BLS_SUCCEED, SYNC_PARTITION_COUNT - 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_SUCCEED);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:succeed, partition[0~2]=succeed, partition[3]=failed
TEST_F(bulk_load_failover_test, succeed_with_partition_failed)
{
    mock_pstatus_map(bulk_load_status::BLS_SUCCEED, 2);
    _pstatus_map[3] = bulk_load_status::BLS_FAILED;
    try_to_continue_bulk_load(bulk_load_status::BLS_SUCCEED);
    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:failed, partition[0~3]=failed
TEST_F(bulk_load_failover_test, failed_with_partition_all_failed)
{
    mock_pstatus_map(bulk_load_status::BLS_FAILED, SYNC_PARTITION_COUNT - 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_FAILED);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:failed, partition[0,1]=downloading, partition[2]=downloaded, partition[3]=failed
TEST_F(bulk_load_failover_test, failed_with_partition_mixed)
{
    mock_pstatus_map(bulk_load_status::BLS_DOWNLOADING, 1);
    _pstatus_map[2] = bulk_load_status::BLS_DOWNLOADED;
    _pstatus_map[3] = bulk_load_status::BLS_FAILED;
    try_to_continue_bulk_load(bulk_load_status::BLS_FAILED);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:pausing, partition[0]=downloading, partition[1]=downloaded, partition[2]=pausing,
// partition[3]=paused
TEST_F(bulk_load_failover_test, pausing_with_partition_mixed)
{
    _pstatus_map[0] = bulk_load_status::BLS_DOWNLOADING;
    _pstatus_map[1] = bulk_load_status::BLS_DOWNLOADED;
    _pstatus_map[2] = bulk_load_status::BLS_PAUSING;
    _pstatus_map[3] = bulk_load_status::BLS_PAUSED;
    try_to_continue_bulk_load(bulk_load_status::BLS_PAUSING);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:pausing, partition[0~3]=pasuing
TEST_F(bulk_load_failover_test, pausing_with_all_pausing)
{
    mock_pstatus_map(bulk_load_status::BLS_PAUSING, SYNC_PARTITION_COUNT - 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_PAUSING);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:pausing, partition[0,1]=pausing, partition[2,3]=paused
TEST_F(bulk_load_failover_test, pausing_with_some_paused)
{
    mock_pstatus_map(bulk_load_status::BLS_PAUSING, 1);
    mock_pstatus_map(bulk_load_status::BLS_PAUSED, SYNC_PARTITION_COUNT - 1, 2);
    try_to_continue_bulk_load(bulk_load_status::BLS_PAUSING);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:paused, partition[0~2]=paused, partition[3]=pausing
TEST_F(bulk_load_failover_test, paused_with_mixed)
{
    mock_pstatus_map(bulk_load_status::BLS_PAUSED, 2);
    _pstatus_map[3] = bulk_load_status::BLS_PAUSING;
    try_to_continue_bulk_load(bulk_load_status::BLS_PAUSED);
    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:paused, partition[0~3]=paused
TEST_F(bulk_load_failover_test, paused_with_all_paused)
{
    mock_pstatus_map(bulk_load_status::BLS_PAUSED, SYNC_PARTITION_COUNT - 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_PAUSED);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:cancel, partition[0~3]=cancel
TEST_F(bulk_load_failover_test, cancel_with_all_cancel)
{
    mock_pstatus_map(bulk_load_status::BLS_CANCELED, SYNC_PARTITION_COUNT - 1);
    try_to_continue_bulk_load(bulk_load_status::BLS_CANCELED);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

// app:cancel, partition[0~2]=ingestion, partition[3]=downloaded
TEST_F(bulk_load_failover_test, cancel_with_mixed)
{
    mock_pstatus_map(bulk_load_status::BLS_INGESTING, 1);
    mock_pstatus_map(bulk_load_status::BLS_DOWNLOADED, SYNC_PARTITION_COUNT - 1, 2);
    try_to_continue_bulk_load(bulk_load_status::BLS_CANCELED);
    ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
}

} // namespace replication
} // namespace dsn
