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

#include "replica.h"
#include "replica_stub.h"
#include <dsn/dist/replication/replication_app_base.h>

namespace dsn {
namespace replication {

void replica::on_add_child(const group_check_request &request) // on parent
{
    if (request.config.ballot != get_ballot()) {
        dwarn_f("receive add child replica with different ballot, local ballot is {}, "
                "request ballot is {}, ignore",
                get_ballot(),
                request.config.ballot);
        return;
    }

    gpid child_gpid = request.child_gpid;
    if (_child_gpid == child_gpid) {
        dwarn_f("child replica already exist, child gpid is ({},{}), "
                "this replica {} may be spliting, ignore",
                child_gpid.get_app_id(),
                child_gpid.get_partition_index(),
                name());
        return;
    }

    if (child_gpid.get_partition_index() < _app_info.partition_count) {
        dwarn_f("{}, receive old add child replica request, child gpid is ({},{}), "
                "local partition count is {}, ignore",
                name(),
                child_gpid.get_app_id(),
                child_gpid.get_partition_index(),
                _app_info.partition_count);
        return;
    }

    _child_gpid = child_gpid;
    _child_ballot = get_ballot();

    ddebug_f("{} process add child replica({}, {}), primary is {}, ballot is {}, "
             "status is {}, last_committed_decree is {}",
             name(),
             child_gpid.get_app_id(),
             child_gpid.get_partition_index(),
             request.config.primary.to_string(),
             request.config.ballot,
             enum_to_string(request.config.status),
             request.last_committed_decree);

    tasking::enqueue(LPC_SPLIT_PARTITION,
                     tracker(),
                     std::bind(&replica_stub::add_split_replica,
                               _stub,
                               _config.primary,
                               _app_info,
                               _child_ballot,
                               _child_gpid,
                               get_gpid(),
                               _dir),
                     get_gpid().thread_hash());
}

void replica::init_child_replica(gpid parent_gpid,
                                 rpc_address primary_address,
                                 ballot init_ballot) // on child
{
    if (status() != partition_status::PS_INACTIVE) {
        dwarn_f("{} status is not PS_INACTIVE, is {}, skip split request",
                name(),
                enum_to_string(status()));

        _stub->on_exec(
            LPC_SPLIT_PARTITION, parent_gpid, [](replica_ptr r) { r->_child_gpid.set_app_id(0); });
        return;
    }

    // update replica config
    _config.ballot = init_ballot;
    _config.primary = primary_address;
    _config.status = partition_status::PS_PARTITION_SPLIT;

    // init split states
    _split_states.parent_gpid = parent_gpid;
    _split_states.is_caught_up = false;
    _split_states.is_prepare_list_copied = false;

    // heartbeat
    _split_states.check_state_task = tasking::enqueue(LPC_SPLIT_PARTITION,
                                                      tracker(),
                                                      std::bind(&replica::check_child_state, this),
                                                      get_gpid().thread_hash(),
                                                      std::chrono::seconds(5));

    ddebug_f("{}: init child replica, start ballot is {}, parent gpid is ({}.{})",
             name(),
             init_ballot,
             parent_gpid.get_app_id(),
             parent_gpid.get_partition_index());

    std::string learn_dir = _app->learn_dir();
    _stub->on_exec(LPC_SPLIT_PARTITION,
                   _split_states.parent_gpid,
                   std::bind(&replica::prepare_copy_parent_state,
                             std::placeholders::_1,
                             learn_dir,
                             get_gpid(),
                             get_ballot()),
                   std::bind(&replica::update_local_configuration_with_no_ballot_change,
                             std::placeholders::_1,
                             partition_status::PS_ERROR),
                   get_gpid());
}

void replica::check_child_state() // on child
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_f("{} status is not PS_PARTITION_SPLIT, is {}", name(), enum_to_string(status()));
        _split_states.check_state_task = nullptr;
        return;
    }

    // parent check its state
    _stub->on_exec(
        LPC_SPLIT_PARTITION,
        _split_states.parent_gpid,
        std::bind(&replica::check_parent_state, std::placeholders::_1, get_gpid(), get_ballot()),
        std::bind(&replica::update_local_configuration_with_no_ballot_change,
                  std::placeholders::_1,
                  partition_status::PS_ERROR),
        get_gpid());

    // restart check_state_task
    _split_states.check_state_task = tasking::enqueue(LPC_SPLIT_PARTITION,
                                                      tracker(),
                                                      std::bind(&replica::check_child_state, this),
                                                      get_gpid().thread_hash(),
                                                      std::chrono::seconds(5));
}

void replica::check_parent_state(gpid child_gpid, ballot child_ballot) // on parent
{
    if (child_ballot != get_ballot() || child_gpid != _child_gpid ||
        (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY &&
         (status() != partition_status::PS_INACTIVE || !_inactive_is_transient))) {

        dwarn_f("{}({}) receive out-dated split request, child config from request is ({}.{}), "
                "ballot is {}, "
                "local child config is ({}.{}), ballot is {}",
                name(),
                enum_to_string(status()),
                child_gpid.get_app_id(),
                child_gpid.get_partition_index(),
                child_ballot,
                _child_gpid.get_app_id(),
                _child_gpid.get_partition_index(),
                get_ballot());

        _stub->on_exec(LPC_SPLIT_PARTITION,
                       child_gpid,
                       std::bind(&replica::update_local_configuration_with_no_ballot_change,
                                 std::placeholders::_1,
                                 partition_status::PS_ERROR));

        _child_gpid.set_app_id(0);
    }
}

void replica::prepare_copy_parent_state(const std::string &dir,
                                        gpid child_gpid,
                                        ballot child_ballot) // on parent
{
}

} // namespace replication
} // namespace dsn
