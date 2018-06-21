#include <dsn/service_api_c.h>

#include "dist/replication/lib/replica.h"
#include "dist/replication/lib/replica_stub.h"
#include "dsn/dist/replication/replication_app_base.h"
#include "dsn/dist/replication/storage_serverlet.h"

#define BALLOT 1

using namespace ::dsn::replication;

static const int partition_count = 8;
static const dsn::gpid parent_gpid(1, 1);
static const dsn::gpid child_gpid(1, 9);
// function name -> mock function
// when substitutes[$function_name] exist, using mock function, otherwise original function
static std::map<std::string, void *> substitutes;

// replication_app_base mock class
class replication_app_mock : public dsn::replication::replication_app_base
{
public:
    replication_app_mock(dsn::replication::replica *r) : dsn::replication::replication_app_base(r)
    {
    }
    virtual ~replication_app_mock() {}

    virtual dsn::error_code start(int argc, char **argv) { return dsn::ERR_OK; }
    virtual dsn::error_code stop(bool cleanup) { return dsn::ERR_OK; }
    virtual dsn::error_code sync_checkpoint() { return dsn::ERR_OK; }
    virtual dsn::error_code async_checkpoint(bool flush_memtable) { return dsn::ERR_OK; }
    virtual dsn::error_code prepare_get_checkpoint(dsn::blob &learn_req) { return dsn::ERR_OK; }
    virtual dsn::error_code get_checkpoint(int64_t learn_start,
                                           const ::dsn::blob &learn_request,
                                           /*out*/ learn_state &state)
    {
        return dsn::ERR_OK;
    }
    virtual dsn::error_code storage_apply_checkpoint(chkpt_apply_mode mode,
                                                     const learn_state &state)
    {
        return dsn::ERR_OK;
    }
    virtual dsn::error_code copy_checkpoint_to_dir(const char *checkpoint_dir, int64_t *last_decree)
    {
        return dsn::ERR_NOT_IMPLEMENTED;
    }
    virtual dsn::replication::decree last_durable_decree() const { return _last_durable_decree; }
    virtual dsn::replication::decree last_flushed_decree() const { return _last_durable_decree; }
    virtual int on_request(dsn_message_t request) { return 0; }
    virtual int on_batched_write_requests(int64_t decree,
                                          uint64_t timestamp,
                                          dsn_message_t *requests,
                                          int request_length)
    {
        return 0;
    }
    virtual std::string query_compact_state() const override { return ""; }
    virtual void update_app_envs(const std::map<std::string, std::string> &envs) {}
    virtual void query_app_envs(/*out*/ std::map<std::string, std::string> &envs) {}

private:
    int64_t _last_durable_decree;
};

// replica_stub mock class
class replica_stub_mock : public dsn::replication::replica_stub
{
public:
    std::map<dsn::gpid, dsn::replication::replica_ptr> replicas;

public:
    replica_stub_mock() : dsn::replication::replica_stub() {}

    // generate replica_split_mock instance
    replica_split_mock *generate_replica(dsn::gpid gpid, partition_status::type status);

    dsn::replication::replica_ptr get_replica(dsn::gpid pid, bool create_if_possible);
    dsn::replication::replica_ptr get_replica(dsn::gpid pid); // override
};

// replica mock class
class replica_split_mock : public dsn::replication::replica
{
public:
    replica_split_mock(dsn::replication::replica_stub *stub,
                       dsn::gpid gpid,
                       const dsn::app_info &app,
                       const char *dir,
                       bool need_restore)
        : dsn::replication::replica(stub, gpid, app, dir, need_restore)
    {
        _app.reset(new replication_app_mock(this));
    }

    // override
    void
    init_child_replica(dsn::gpid gpid_parent, dsn::rpc_address primary_address, ballot init_ballot);
    void
    prepare_copy_parent_state(const std::string &dir, dsn::gpid child_gpid, ballot child_ballot);

    void check_child_state();
    // TODO(hyc): mock rather than override it
    bool update_local_configuration_with_no_ballot_change(partition_status::type status);
};

replica_split_mock *replica_stub_mock::generate_replica(dsn::gpid gpid,
                                                        partition_status::type status)
{
    dsn::app_info info;
    info.app_type = "replica";
    info.partition_count = partition_count;

    replica_split_mock *rep = new replica_split_mock(this, gpid, std::move(info), "./", false);
    rep->_config.status = status;
    rep->_config.pid = gpid;
    rep->_config.ballot = BALLOT;

    return rep;
}

dsn::replication::replica_ptr replica_stub_mock::get_replica(dsn::gpid pid, bool create_if_possible)
{
    auto iter = replicas.find(pid);

    if (iter != replicas.end()) {
        return iter->second;
    } else if (create_if_possible) {
        auto r = this->generate_replica(pid, partition_status::PS_INACTIVE);
        replicas[pid] = r;
        return r;
    }

    return nullptr;
}

dsn::replication::replica_ptr replica_stub_mock::get_replica(dsn::gpid pid)
{
    return get_replica(pid, true);
}

bool replica_split_mock::update_local_configuration_with_no_ballot_change(
    partition_status::type status)
{
    _config.status = status;
    return true;
}

void replica_split_mock::init_child_replica(dsn::gpid gpid_parent,
                                            dsn::rpc_address primary_address,
                                            ballot init_ballot)
{
    auto iter = substitutes.find("init_child_replica");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void(dsn::gpid, dsn::rpc_address, ballot)> *)iter->second;
            (*call)(gpid_parent, primary_address, init_ballot);
        }
    } else {
        dsn::replication::replica::init_child_replica(gpid_parent, primary_address, init_ballot);
    }
}

void replica_split_mock::prepare_copy_parent_state(const std::string &dir,
                                                   dsn::gpid child_gpid,
                                                   ballot child_ballot)
{
    auto iter = substitutes.find("prepare_copy_parent_state");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void(const std::string, dsn::gpid, ballot)> *)iter->second;
            (*call)(dir, child_gpid, child_ballot);
        }
    } else {
        dsn::replication::replica::prepare_copy_parent_state(dir, child_gpid, child_ballot);
    }
}

void replica_split_mock::check_child_state()
{
    auto iter = substitutes.find("check_child_state");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void()> *)iter->second;
            (*call)();
        }
    } else {
        dsn::replication::replica::check_child_state();
    }
}
