#include <dsn/service_api_c.h>

#include "dist/replication/meta_server/meta_bulk_load_service.h"
#include "meta_service_test_app.h"

using namespace ::dsn::replication;

// function name -> mock function
// when mock_funcs[$function_name] exist, using mock function, otherwise original function
static std::map<std::string, void *> mock_funcs;

#define NAME "bulk_load_table"
#define PARTITION_COUNT 8
#define CLUSTER "cluster"
#define PROVIDER "local_service"

// create a fake app
inline dsn::app_info create_mock_app_info()
{
    dsn::app_info info;
    info.is_stateful = true;
    info.app_id = 1;
    info.app_type = "simple_kv";
    info.app_name = NAME;
    info.max_replica_count = 3;
    info.partition_count = PARTITION_COUNT;
    info.status = dsn::app_status::AS_CREATING;
    info.envs.clear();
    return info;
}

template <typename TRequest>
inline dsn::message_ex *create_recv_msg(dsn::task_code rpc_code, TRequest req)
{
    dsn::message_ex *binary_req = dsn::message_ex::create_request(rpc_code);
    dsn::marshall(binary_req, req);
    return create_corresponding_receive(binary_req);
}

// send start bulk load request
inline start_bulk_load_response send_request(dsn::task_code rpc_code,
                                             start_bulk_load_request request,
                                             std::shared_ptr<meta_service> meta_svc,
                                             bulk_load_service *bulk_svc)
{
    dsn::message_ex *recv_msg = create_recv_msg(rpc_code, request);
    start_bulk_load_rpc rpc(recv_msg);
    bulk_svc->on_start_bulk_load(rpc);
    meta_svc->tracker()->wait_outstanding_tasks();
    return rpc.response();
}

class bulk_load_service_mock : public bulk_load_service
{
public:
    bulk_load_service_mock(meta_service *meta_svc, const std::string &bulk_load_dir)
        : bulk_load_service(meta_svc, bulk_load_dir)
    {
    }

    void partition_bulk_load(dsn::gpid pid);
};

void bulk_load_service_mock::partition_bulk_load(dsn::gpid pid)
{
    auto iter = mock_funcs.find("partition_bulk_load");

    if (iter != mock_funcs.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void(dsn::gpid)> *)iter->second;
            (*call)(pid);
        }
    } else {
        bulk_load_service::partition_bulk_load(pid);
    }
}
