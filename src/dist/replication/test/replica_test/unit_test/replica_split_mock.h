#include <dsn/service_api_c.h>

#include "dist/replication/lib/replica.h"
#include "dist/replication/lib/replica_stub.h"
#include "dsn/dist/replication/replication_app_base.h"
#include "dist/replication/lib/mutation_log.h"

#define BALLOT 1
#define DECREE 5

using namespace ::dsn::replication;

static const int partition_count = 8;
static const dsn::gpid parent_gpid(1, 1);
static const dsn::gpid child_gpid(1, 9);

// function name -> mock function
// when substitutes[$function_name] exist, using mock function, otherwise original function
static std::map<std::string, void *> substitutes;

class log_file_mock : public dsn::replication::log_file
{
public:
    log_file_mock(const std::string path)
        : dsn::replication::log_file(path.c_str(), nullptr, 0, 0, false)
    {
    }

    void set_file_size(int size) { _end_offset = _start_offset + size; }
};

class mutation_log_private_mock : public dsn::replication::mutation_log_private
{
public:
    mutation_log_private_mock(dsn::gpid pid, dsn::replication::replica *r)
        : dsn::replication::mutation_log_private("", 10, pid, r, 10, 10, 500)
    {
    }

    virtual ::dsn::task_ptr append(dsn::replication::mutation_ptr &mu,
                                   dsn::task_code callback_code,
                                   dsn::task_tracker *tracker,
                                   dsn::aio_handler &&callback,
                                   int hash = 0) override
    {
        mu_list.push_back(mu);
        return nullptr;
    }

    virtual bool get_mutation_in_memory(decree start_decree,
                                        ballot start_ballot,
                                        std::vector<mutation_ptr> &mutation_list) const override
    {
        int mutation_count = 0;
        for (auto &mu : mu_list) {
            ballot current_ballot =
                (start_ballot == invalid_ballot) ? invalid_ballot : mu->get_ballot();
            if ((mu->get_decree() >= start_decree && start_ballot == current_ballot) ||
                current_ballot > start_ballot) {
                mutation_list.push_back(mu);
                mutation_count++;
            }
        }
        return mutation_count > 0;
    }

    static error_code replay(std::vector<std::string> &log_files,
                             replay_callback callback,
                             /*out*/ int64_t &end_offset)
    {
        return dsn::ERR_OK;
    }

    void add_log(dsn::replication::log_file_ptr lf) { _log_files[0] = lf; }

public:
    std::vector<dsn::replication::mutation_ptr> mu_list;
};

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
        *last_decree = DECREE;
        return dsn::ERR_OK;
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

    virtual void mock_commit() { _last_committed_decree++; }

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

    void set_log(mutation_log_ptr log) { _log = log; }
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
    void copy_parent_state(dsn::error_code ec,
                           learn_state lstate,
                           std::vector<mutation_ptr> mutation_list,
                           std::vector<std::string> files,
                           prepare_list *plist);
    void apply_parent_state(dsn::error_code ec,
                            learn_state lstate,
                            std::vector<mutation_ptr> mutation_list,
                            std::vector<std::string> files,
                            decree last_committed_decree);
    dsn::error_code async_learn_mutation_private_log(std::vector<mutation_ptr> mutation_list,
                                                     std::vector<std::string> files,
                                                     decree last_committed_decree);
    void child_catch_up();
    void notify_primary_parent_finish_catch_up();

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

void replica_split_mock::copy_parent_state(dsn::error_code ec,
                                           learn_state lstate,
                                           std::vector<mutation_ptr> mutation_list,
                                           std::vector<std::string> files,
                                           prepare_list *plist)
{
    auto iter = substitutes.find("copy_parent_state");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void(dsn::error_code,
                                            learn_state,
                                            std::vector<mutation_ptr>,
                                            std::vector<std::string>,
                                            prepare_list *)> *)iter->second;
            (*call)(ec, lstate, mutation_list, files, plist);
        }
    } else {
        dsn::replication::replica::copy_parent_state(ec, lstate, mutation_list, files, plist);
    }
}

void replica_split_mock::apply_parent_state(dsn::error_code ec,
                                            learn_state lstate,
                                            std::vector<mutation_ptr> mutation_list,
                                            std::vector<std::string> files,
                                            decree last_committed_decree)
{
    auto iter = substitutes.find("apply_parent_state");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void(dsn::error_code,
                                            learn_state,
                                            std::vector<mutation_ptr>,
                                            std::vector<std::string>,
                                            decree)> *)iter->second;
            (*call)(ec, lstate, mutation_list, files, last_committed_decree);
        }
    } else {
        dsn::replication::replica::apply_parent_state(
            ec, lstate, mutation_list, files, last_committed_decree);
    }
}

dsn::error_code
replica_split_mock::async_learn_mutation_private_log(std::vector<mutation_ptr> mutation_list,
                                                     std::vector<std::string> files,
                                                     decree last_committed_decree)
{
    dsn::error_code ec = dsn::ERR_OK;
    auto iter = substitutes.find("async_learn_mutation_private_log");
    bool flag =
        substitutes.find("async_learn_mutation_private_log_mock_failure") != substitutes.end();

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<dsn::error_code(
                             std::vector<mutation_ptr>, std::vector<std::string>, decree, bool)> *)
                            iter->second;
            ec = (*call)(mutation_list, files, last_committed_decree, flag);
        }
    } else {
        ec = dsn::replication::replica::async_learn_mutation_private_log(
            mutation_list, files, last_committed_decree);
    }
    return ec;
}

void replica_split_mock::child_catch_up()
{
    auto iter = substitutes.find("child_catch_up");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void()> *)iter->second;
            (*call)();
        }
    } else {
        dsn::replication::replica::child_catch_up();
    }
}

void replica_split_mock::notify_primary_parent_finish_catch_up()
{
    auto iter = substitutes.find("notify_primary_parent_finish_catch_up");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void()> *)iter->second;
            (*call)();
        }
    } else {
        dsn::replication::replica::notify_primary_parent_finish_catch_up();
    }
}
