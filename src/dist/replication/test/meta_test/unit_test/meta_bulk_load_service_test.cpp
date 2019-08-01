#include <gtest/gtest.h>

#include <dsn/dist/fmt_logging.h>

#include "dist/replication/meta_server/meta_server_failure_detector.h"
#include "dist/replication/meta_server/meta_service.h"
#include "meta_bulk_load_service_mock.h"

using namespace ::dsn::replication;

std::shared_ptr<meta_service> meta_service_test_app::create_bulk_load_mock_meta_svc()
{
    // create mock app
    std::shared_ptr<app_state> app = app_state::create(create_mock_app_info());

    // create meta_service
    std::shared_ptr<meta_service> meta_svc = std::make_shared<meta_service>();
    meta_svc->_meta_opts.cluster_root = "/meta_test";
    meta_svc->_meta_opts.meta_state_service_type = "meta_state_service_simple";
    meta_svc->remote_storage_initialize();
    meta_svc->set_function_level(meta_function_level::fl_steady);
    meta_svc->_failure_detector.reset(new meta_server_failure_detector(meta_svc.get()));

    // initialize server_state
    std::string apps_root = "/meta_test/apps";
    std::shared_ptr<server_state> ss = meta_svc->_state;
    ss->initialize(meta_svc.get(), apps_root);
    ss->_all_apps.emplace(std::make_pair(app->app_id, app));
    ss->sync_apps_to_remote_storage();

    // initialize bulk load service
    meta_svc->_bulk_load_svc.reset(new bulk_load_service_mock(
        meta_svc.get(),
        meta_options::concat_path_unix_style(meta_svc->_cluster_root, "bulk_load")));
    meta_svc->_bulk_load_svc->create_bulk_load_dir_on_remote_stroage();
    return meta_svc;
}

void meta_service_test_app::on_start_bulk_load_test()
{
    std::shared_ptr<meta_service> meta_svc = create_bulk_load_mock_meta_svc();

    start_bulk_load_request request;
    request.app_name = NAME;
    request.cluster_name = CLUSTER;
    request.file_provider_type = PROVIDER;

    mock_funcs["partition_bulk_load"] = nullptr;

    std::cout << "case1. wrong table name" << std::endl;
    {
        request.app_name = "table_not_exist";
        auto response =
            send_request(RPC_CM_START_BULK_LOAD, request, meta_svc, meta_svc->_bulk_load_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_APP_NOT_EXIST);
        request.app_name = NAME;
    }

    std::cout << "case2. wrong provider name" << std::endl;
    {
        request.file_provider_type = "wrong_provider_name";
        auto response =
            send_request(RPC_CM_START_BULK_LOAD, request, meta_svc, meta_svc->_bulk_load_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_INVALID_PARAMETERS);
        request.file_provider_type = PROVIDER;
    }

    // TODO(heyuchen):
    // add unit test for checking cluster_name

    std::cout << "case.3 normal case" << std::endl;
    {
        auto response =
            send_request(RPC_CM_START_BULK_LOAD, request, meta_svc, meta_svc->_bulk_load_svc.get());
        ASSERT_EQ(response.err, dsn::ERR_OK);
    }
    mock_funcs.erase("partition_bulk_load");
}

// TODO(heyuchen): start_sync_apps_bulk_load test
