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
                                   int hash = 0,
                                   int64_t *pending_size = nullptr) override
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
    virtual int on_request(dsn::message_ex *request) { return 0; }
    virtual int on_batched_write_requests(int64_t decree,
                                          uint64_t timestamp,
                                          dsn::message_ex **requests,
                                          int request_length)
    {
        return 0;
    }
    virtual std::string query_compact_state() const override { return ""; }
    virtual void update_app_envs(const std::map<std::string, std::string> &envs) {}
    virtual void query_app_envs(/*out*/ std::map<std::string, std::string> &envs) {}

    virtual void set_partition_version(uint32_t partition_version) {}

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
    dsn::replication::replica_ptr
    get_replica_permit_create_new(dsn::gpid pid, dsn::app_info *app, const std::string &parent_dir);

    void set_log(mutation_log_ptr log) { _log = log; }
    void set_address(dsn::rpc_address address) { _primary_address = address; }
    void set_meta_server(dsn::rpc_address meta_address)
    {
        std::vector<dsn::rpc_address> metas;
        metas.push_back(meta_address);

        _failure_detector = new ::dsn::dist::slave_failure_detector_with_multimaster(
            metas,
            [this]() { this->on_meta_server_disconnected(); },
            [this]() { this->on_meta_server_connected(); });
    }
    void set_connected() { _state = NS_Connected; }
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
    void notify_primary_split_catch_up();
    void on_notify_primary_split_catch_up(notify_catch_up_request request,
                                          notify_cacth_up_response &response);
    void check_sync_point(decree sync_point);
    void update_group_partition_count(int new_partition_count, bool is_update_child);
    void on_update_group_partition_count(update_group_partition_count_request request,
                                         update_group_partition_count_response &response);
    void on_update_group_partition_count_reply(
        dsn::error_code ec,
        std::shared_ptr<update_group_partition_count_request> request,
        std::shared_ptr<update_group_partition_count_response> response,
        std::shared_ptr<std::set<dsn::rpc_address>> left_replicas,
        dsn::rpc_address finish_update_address,
        bool is_update_child);

    void register_child_on_meta(ballot b);
    void on_register_child_on_meta_reply(dsn::error_code ec,
                                         std::shared_ptr<register_child_request> request,
                                         std::shared_ptr<register_child_response> response);

    void check_child_state();

    void check_partition_count(int partition_count);
    void query_child_state();
    void on_query_child_state_reply(dsn::error_code ec,
                                    std::shared_ptr<query_child_state_request> request,
                                    std::shared_ptr<query_child_state_response> response);
    void on_add_child(const group_check_request &request);

    // TODO(hyc): mock rather than override it
    bool update_local_configuration_with_no_ballot_change(partition_status::type status);

    // helper functions
    replica_configuration get_config() { return _config; }
    void set_config(replica_configuration config) { _config = config; }
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

dsn::replication::replica_ptr replica_stub_mock::get_replica_permit_create_new(
    dsn::gpid pid, dsn::app_info *app, const std::string &parent_dir)
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

void replica_split_mock::notify_primary_split_catch_up()
{
    auto iter = substitutes.find("notify_primary_split_catch_up");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void()> *)iter->second;
            (*call)();
        }
    } else {
        dsn::replication::replica::notify_primary_split_catch_up();
    }
}

void replica_split_mock::on_notify_primary_split_catch_up(notify_catch_up_request request,
                                                          notify_cacth_up_response &response)
{
    auto iter = substitutes.find("on_notify_primary_split_catch_up");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void(notify_catch_up_request, notify_cacth_up_response &)> *)
                            iter->second;
            (*call)(request, response);
        }
    } else {
        dsn::replication::replica::on_notify_primary_split_catch_up(request, response);
    }
}

void replica_split_mock::check_sync_point(decree sync_point)
{
    auto iter = substitutes.find("check_sync_point");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void(decree)> *)iter->second;
            (*call)(sync_point);
        }
    } else {
        dsn::replication::replica::check_sync_point(sync_point);
    }
}

void replica_split_mock::update_group_partition_count(int new_partition_count, bool is_update_child)
{
    auto iter = substitutes.find("update_group_partition_count");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void(int, bool)> *)iter->second;
            (*call)(new_partition_count, is_update_child);
        }
    } else {
        dsn::replication::replica::update_group_partition_count(new_partition_count,
                                                                is_update_child);
    }
}

void replica_split_mock::on_update_group_partition_count(
    update_group_partition_count_request request, update_group_partition_count_response &response)
{
    auto iter = substitutes.find("on_update_group_partition_count");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call =
                (std::function<void(update_group_partition_count_request,
                                    update_group_partition_count_response &)> *)iter->second;
            (*call)(request, response);
        }
    } else {
        dsn::replication::replica::on_update_group_partition_count(request, response);
    }
}

void replica_split_mock::on_update_group_partition_count_reply(
    dsn::error_code ec,
    std::shared_ptr<update_group_partition_count_request> request,
    std::shared_ptr<update_group_partition_count_response> response,
    std::shared_ptr<std::set<dsn::rpc_address>> left_replicas,
    dsn::rpc_address finish_update_address,
    bool is_update_child)
{
    auto iter = substitutes.find("on_update_group_partition_count_reply");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void(dsn::error_code,
                                            std::shared_ptr<update_group_partition_count_request>,
                                            std::shared_ptr<update_group_partition_count_response>,
                                            std::shared_ptr<std::set<dsn::rpc_address>>,
                                            dsn::rpc_address,
                                            bool)> *)iter->second;
            (*call)(ec, request, response, left_replicas, finish_update_address, is_update_child);
        }
    } else {
        dsn::replication::replica::on_update_group_partition_count_reply(
            ec, request, response, left_replicas, finish_update_address, is_update_child);
    }
}

void replica_split_mock::register_child_on_meta(ballot b)
{
    auto iter = substitutes.find("register_child_on_meta");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void(ballot)> *)iter->second;
            (*call)(b);
        }
    } else {
        dsn::replication::replica::register_child_on_meta(b);
    }
}

void replica_split_mock::on_register_child_on_meta_reply(
    dsn::error_code ec,
    std::shared_ptr<register_child_request> request,
    std::shared_ptr<register_child_response> response)
{
    auto iter = substitutes.find("on_register_child_on_meta_reply");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call =
                (std::function<void(dsn::error_code,
                                    std::shared_ptr<register_child_request>,
                                    std::shared_ptr<register_child_response>)> *)iter->second;
            (*call)(ec, request, response);
        }
    } else {
        dsn::replication::replica::on_register_child_on_meta_reply(ec, request, response);
    }
}

void replica_split_mock::check_partition_count(int partition_count)
{
    auto iter = substitutes.find("check_partition_count");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void(int)> *)iter->second;
            (*call)(partition_count);
        }
    } else {
        dsn::replication::replica::check_partition_count(partition_count);
    }
}

void replica_split_mock::query_child_state()
{
    auto iter = substitutes.find("query_child_state");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void()> *)iter->second;
            (*call)();
        }
    } else {
        dsn::replication::replica::query_child_state();
    }
}

void replica_split_mock::on_query_child_state_reply(
    dsn::error_code ec,
    std::shared_ptr<query_child_state_request> request,
    std::shared_ptr<query_child_state_response> response)
{
    auto iter = substitutes.find("on_query_child_state_reply");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call =
                (std::function<void(dsn::error_code,
                                    std::shared_ptr<query_child_state_request>,
                                    std::shared_ptr<query_child_state_response>)> *)iter->second;
            (*call)(ec, request, response);
        }
    } else {
        dsn::replication::replica::on_query_child_state_reply(ec, request, response);
    }
}

void replica_split_mock::on_add_child(const group_check_request &request)
{
    auto iter = substitutes.find("on_add_child");

    if (iter != substitutes.end()) {
        if (iter->second != nullptr) {
            auto call = (std::function<void(const group_check_request &)> *)iter->second;
            (*call)(request);
        }
    } else {
        dsn::replication::replica::on_add_child(request);
    }
}
