/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#include <dsn/cpp/serialization_helper/dsn.layer2_types.h>

#include <algorithm>
#include <ostream>

#include <thrift/TToString.h>

namespace dsn {

int _kapp_statusValues[] = {app_status::AS_INVALID,
                            app_status::AS_AVAILABLE,
                            app_status::AS_CREATING,
                            app_status::AS_CREATE_FAILED,
                            app_status::AS_DROPPING,
                            app_status::AS_DROP_FAILED,
                            app_status::AS_DROPPED,
                            app_status::AS_RECALLING};
const char *_kapp_statusNames[] = {"AS_INVALID",
                                   "AS_AVAILABLE",
                                   "AS_CREATING",
                                   "AS_CREATE_FAILED",
                                   "AS_DROPPING",
                                   "AS_DROP_FAILED",
                                   "AS_DROPPED",
                                   "AS_RECALLING"};
const std::map<int, const char *> _app_status_VALUES_TO_NAMES(
    ::apache::thrift::TEnumIterator(8, _kapp_statusValues, _kapp_statusNames),
    ::apache::thrift::TEnumIterator(-1, NULL, NULL));

partition_configuration::~partition_configuration() throw() {}

void partition_configuration::__set_pid(const ::dsn::gpid &val) { this->pid = val; }

void partition_configuration::__set_ballot(const int64_t val) { this->ballot = val; }

void partition_configuration::__set_max_replica_count(const int32_t val)
{
    this->max_replica_count = val;
}

void partition_configuration::__set_primary(const ::dsn::rpc_address &val) { this->primary = val; }

void partition_configuration::__set_secondaries(const std::vector<::dsn::rpc_address> &val)
{
    this->secondaries = val;
}

void partition_configuration::__set_last_drops(const std::vector<::dsn::rpc_address> &val)
{
    this->last_drops = val;
}

void partition_configuration::__set_last_committed_decree(const int64_t val)
{
    this->last_committed_decree = val;
}

void partition_configuration::__set_partition_flags(const int32_t val)
{
    this->partition_flags = val;
}

uint32_t partition_configuration::read(::apache::thrift::protocol::TProtocol *iprot)
{

    apache::thrift::protocol::TInputRecursionTracker tracker(*iprot);
    uint32_t xfer = 0;
    std::string fname;
    ::apache::thrift::protocol::TType ftype;
    int16_t fid;

    xfer += iprot->readStructBegin(fname);

    using ::apache::thrift::protocol::TProtocolException;

    while (true) {
        xfer += iprot->readFieldBegin(fname, ftype, fid);
        if (ftype == ::apache::thrift::protocol::T_STOP) {
            break;
        }
        switch (fid) {
        case 1:
            if (ftype == ::apache::thrift::protocol::T_STRUCT) {
                xfer += this->pid.read(iprot);
                this->__isset.pid = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 2:
            if (ftype == ::apache::thrift::protocol::T_I64) {
                xfer += iprot->readI64(this->ballot);
                this->__isset.ballot = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 3:
            if (ftype == ::apache::thrift::protocol::T_I32) {
                xfer += iprot->readI32(this->max_replica_count);
                this->__isset.max_replica_count = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 4:
            if (ftype == ::apache::thrift::protocol::T_STRUCT) {
                xfer += this->primary.read(iprot);
                this->__isset.primary = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 5:
            if (ftype == ::apache::thrift::protocol::T_LIST) {
                {
                    this->secondaries.clear();
                    uint32_t _size0;
                    ::apache::thrift::protocol::TType _etype3;
                    xfer += iprot->readListBegin(_etype3, _size0);
                    this->secondaries.resize(_size0);
                    uint32_t _i4;
                    for (_i4 = 0; _i4 < _size0; ++_i4) {
                        xfer += this->secondaries[_i4].read(iprot);
                    }
                    xfer += iprot->readListEnd();
                }
                this->__isset.secondaries = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 6:
            if (ftype == ::apache::thrift::protocol::T_LIST) {
                {
                    this->last_drops.clear();
                    uint32_t _size5;
                    ::apache::thrift::protocol::TType _etype8;
                    xfer += iprot->readListBegin(_etype8, _size5);
                    this->last_drops.resize(_size5);
                    uint32_t _i9;
                    for (_i9 = 0; _i9 < _size5; ++_i9) {
                        xfer += this->last_drops[_i9].read(iprot);
                    }
                    xfer += iprot->readListEnd();
                }
                this->__isset.last_drops = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 7:
            if (ftype == ::apache::thrift::protocol::T_I64) {
                xfer += iprot->readI64(this->last_committed_decree);
                this->__isset.last_committed_decree = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 8:
            if (ftype == ::apache::thrift::protocol::T_I32) {
                xfer += iprot->readI32(this->partition_flags);
                this->__isset.partition_flags = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        default:
            xfer += iprot->skip(ftype);
            break;
        }
        xfer += iprot->readFieldEnd();
    }

    xfer += iprot->readStructEnd();

    return xfer;
}

uint32_t partition_configuration::write(::apache::thrift::protocol::TProtocol *oprot) const
{
    uint32_t xfer = 0;
    apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
    xfer += oprot->writeStructBegin("partition_configuration");

    xfer += oprot->writeFieldBegin("pid", ::apache::thrift::protocol::T_STRUCT, 1);
    xfer += this->pid.write(oprot);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("ballot", ::apache::thrift::protocol::T_I64, 2);
    xfer += oprot->writeI64(this->ballot);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("max_replica_count", ::apache::thrift::protocol::T_I32, 3);
    xfer += oprot->writeI32(this->max_replica_count);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("primary", ::apache::thrift::protocol::T_STRUCT, 4);
    xfer += this->primary.write(oprot);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("secondaries", ::apache::thrift::protocol::T_LIST, 5);
    {
        xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT,
                                      static_cast<uint32_t>(this->secondaries.size()));
        std::vector<::dsn::rpc_address>::const_iterator _iter10;
        for (_iter10 = this->secondaries.begin(); _iter10 != this->secondaries.end(); ++_iter10) {
            xfer += (*_iter10).write(oprot);
        }
        xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("last_drops", ::apache::thrift::protocol::T_LIST, 6);
    {
        xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT,
                                      static_cast<uint32_t>(this->last_drops.size()));
        std::vector<::dsn::rpc_address>::const_iterator _iter11;
        for (_iter11 = this->last_drops.begin(); _iter11 != this->last_drops.end(); ++_iter11) {
            xfer += (*_iter11).write(oprot);
        }
        xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("last_committed_decree", ::apache::thrift::protocol::T_I64, 7);
    xfer += oprot->writeI64(this->last_committed_decree);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("partition_flags", ::apache::thrift::protocol::T_I32, 8);
    xfer += oprot->writeI32(this->partition_flags);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldStop();
    xfer += oprot->writeStructEnd();
    return xfer;
}

void swap(partition_configuration &a, partition_configuration &b)
{
    using ::std::swap;
    swap(a.pid, b.pid);
    swap(a.ballot, b.ballot);
    swap(a.max_replica_count, b.max_replica_count);
    swap(a.primary, b.primary);
    swap(a.secondaries, b.secondaries);
    swap(a.last_drops, b.last_drops);
    swap(a.last_committed_decree, b.last_committed_decree);
    swap(a.partition_flags, b.partition_flags);
    swap(a.__isset, b.__isset);
}

partition_configuration::partition_configuration(const partition_configuration &other12)
{
    pid = other12.pid;
    ballot = other12.ballot;
    max_replica_count = other12.max_replica_count;
    primary = other12.primary;
    secondaries = other12.secondaries;
    last_drops = other12.last_drops;
    last_committed_decree = other12.last_committed_decree;
    partition_flags = other12.partition_flags;
    __isset = other12.__isset;
}
partition_configuration::partition_configuration(partition_configuration &&other13)
{
    pid = std::move(other13.pid);
    ballot = std::move(other13.ballot);
    max_replica_count = std::move(other13.max_replica_count);
    primary = std::move(other13.primary);
    secondaries = std::move(other13.secondaries);
    last_drops = std::move(other13.last_drops);
    last_committed_decree = std::move(other13.last_committed_decree);
    partition_flags = std::move(other13.partition_flags);
    __isset = std::move(other13.__isset);
}
partition_configuration &partition_configuration::operator=(const partition_configuration &other14)
{
    pid = other14.pid;
    ballot = other14.ballot;
    max_replica_count = other14.max_replica_count;
    primary = other14.primary;
    secondaries = other14.secondaries;
    last_drops = other14.last_drops;
    last_committed_decree = other14.last_committed_decree;
    partition_flags = other14.partition_flags;
    __isset = other14.__isset;
    return *this;
}
partition_configuration &partition_configuration::operator=(partition_configuration &&other15)
{
    pid = std::move(other15.pid);
    ballot = std::move(other15.ballot);
    max_replica_count = std::move(other15.max_replica_count);
    primary = std::move(other15.primary);
    secondaries = std::move(other15.secondaries);
    last_drops = std::move(other15.last_drops);
    last_committed_decree = std::move(other15.last_committed_decree);
    partition_flags = std::move(other15.partition_flags);
    __isset = std::move(other15.__isset);
    return *this;
}
void partition_configuration::printTo(std::ostream &out) const
{
    using ::apache::thrift::to_string;
    out << "partition_configuration(";
    out << "pid=" << to_string(pid);
    out << ", "
        << "ballot=" << to_string(ballot);
    out << ", "
        << "max_replica_count=" << to_string(max_replica_count);
    out << ", "
        << "primary=" << to_string(primary);
    out << ", "
        << "secondaries=" << to_string(secondaries);
    out << ", "
        << "last_drops=" << to_string(last_drops);
    out << ", "
        << "last_committed_decree=" << to_string(last_committed_decree);
    out << ", "
        << "partition_flags=" << to_string(partition_flags);
    out << ")";
}

configuration_query_by_index_request::~configuration_query_by_index_request() throw() {}

void configuration_query_by_index_request::__set_app_name(const std::string &val)
{
    this->app_name = val;
}

void configuration_query_by_index_request::__set_partition_indices(const std::vector<int32_t> &val)
{
    this->partition_indices = val;
}

uint32_t configuration_query_by_index_request::read(::apache::thrift::protocol::TProtocol *iprot)
{

    apache::thrift::protocol::TInputRecursionTracker tracker(*iprot);
    uint32_t xfer = 0;
    std::string fname;
    ::apache::thrift::protocol::TType ftype;
    int16_t fid;

    xfer += iprot->readStructBegin(fname);

    using ::apache::thrift::protocol::TProtocolException;

    while (true) {
        xfer += iprot->readFieldBegin(fname, ftype, fid);
        if (ftype == ::apache::thrift::protocol::T_STOP) {
            break;
        }
        switch (fid) {
        case 1:
            if (ftype == ::apache::thrift::protocol::T_STRING) {
                xfer += iprot->readString(this->app_name);
                this->__isset.app_name = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 2:
            if (ftype == ::apache::thrift::protocol::T_LIST) {
                {
                    this->partition_indices.clear();
                    uint32_t _size16;
                    ::apache::thrift::protocol::TType _etype19;
                    xfer += iprot->readListBegin(_etype19, _size16);
                    this->partition_indices.resize(_size16);
                    uint32_t _i20;
                    for (_i20 = 0; _i20 < _size16; ++_i20) {
                        xfer += iprot->readI32(this->partition_indices[_i20]);
                    }
                    xfer += iprot->readListEnd();
                }
                this->__isset.partition_indices = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        default:
            xfer += iprot->skip(ftype);
            break;
        }
        xfer += iprot->readFieldEnd();
    }

    xfer += iprot->readStructEnd();

    return xfer;
}

uint32_t
configuration_query_by_index_request::write(::apache::thrift::protocol::TProtocol *oprot) const
{
    uint32_t xfer = 0;
    apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
    xfer += oprot->writeStructBegin("configuration_query_by_index_request");

    xfer += oprot->writeFieldBegin("app_name", ::apache::thrift::protocol::T_STRING, 1);
    xfer += oprot->writeString(this->app_name);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("partition_indices", ::apache::thrift::protocol::T_LIST, 2);
    {
        xfer += oprot->writeListBegin(::apache::thrift::protocol::T_I32,
                                      static_cast<uint32_t>(this->partition_indices.size()));
        std::vector<int32_t>::const_iterator _iter21;
        for (_iter21 = this->partition_indices.begin(); _iter21 != this->partition_indices.end();
             ++_iter21) {
            xfer += oprot->writeI32((*_iter21));
        }
        xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldStop();
    xfer += oprot->writeStructEnd();
    return xfer;
}

void swap(configuration_query_by_index_request &a, configuration_query_by_index_request &b)
{
    using ::std::swap;
    swap(a.app_name, b.app_name);
    swap(a.partition_indices, b.partition_indices);
    swap(a.__isset, b.__isset);
}

configuration_query_by_index_request::configuration_query_by_index_request(
    const configuration_query_by_index_request &other22)
{
    app_name = other22.app_name;
    partition_indices = other22.partition_indices;
    __isset = other22.__isset;
}
configuration_query_by_index_request::configuration_query_by_index_request(
    configuration_query_by_index_request &&other23)
{
    app_name = std::move(other23.app_name);
    partition_indices = std::move(other23.partition_indices);
    __isset = std::move(other23.__isset);
}
configuration_query_by_index_request &configuration_query_by_index_request::
operator=(const configuration_query_by_index_request &other24)
{
    app_name = other24.app_name;
    partition_indices = other24.partition_indices;
    __isset = other24.__isset;
    return *this;
}
configuration_query_by_index_request &configuration_query_by_index_request::
operator=(configuration_query_by_index_request &&other25)
{
    app_name = std::move(other25.app_name);
    partition_indices = std::move(other25.partition_indices);
    __isset = std::move(other25.__isset);
    return *this;
}
void configuration_query_by_index_request::printTo(std::ostream &out) const
{
    using ::apache::thrift::to_string;
    out << "configuration_query_by_index_request(";
    out << "app_name=" << to_string(app_name);
    out << ", "
        << "partition_indices=" << to_string(partition_indices);
    out << ")";
}

configuration_query_by_index_response::~configuration_query_by_index_response() throw() {}

void configuration_query_by_index_response::__set_err(const ::dsn::error_code &val)
{
    this->err = val;
}

void configuration_query_by_index_response::__set_app_id(const int32_t val) { this->app_id = val; }

void configuration_query_by_index_response::__set_partition_count(const int32_t val)
{
    this->partition_count = val;
}

void configuration_query_by_index_response::__set_is_stateful(const bool val)
{
    this->is_stateful = val;
}

void configuration_query_by_index_response::__set_partitions(
    const std::vector<partition_configuration> &val)
{
    this->partitions = val;
}

uint32_t configuration_query_by_index_response::read(::apache::thrift::protocol::TProtocol *iprot)
{

    apache::thrift::protocol::TInputRecursionTracker tracker(*iprot);
    uint32_t xfer = 0;
    std::string fname;
    ::apache::thrift::protocol::TType ftype;
    int16_t fid;

    xfer += iprot->readStructBegin(fname);

    using ::apache::thrift::protocol::TProtocolException;

    while (true) {
        xfer += iprot->readFieldBegin(fname, ftype, fid);
        if (ftype == ::apache::thrift::protocol::T_STOP) {
            break;
        }
        switch (fid) {
        case 1:
            if (ftype == ::apache::thrift::protocol::T_STRUCT) {
                xfer += this->err.read(iprot);
                this->__isset.err = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 2:
            if (ftype == ::apache::thrift::protocol::T_I32) {
                xfer += iprot->readI32(this->app_id);
                this->__isset.app_id = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 3:
            if (ftype == ::apache::thrift::protocol::T_I32) {
                xfer += iprot->readI32(this->partition_count);
                this->__isset.partition_count = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 4:
            if (ftype == ::apache::thrift::protocol::T_BOOL) {
                xfer += iprot->readBool(this->is_stateful);
                this->__isset.is_stateful = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 5:
            if (ftype == ::apache::thrift::protocol::T_LIST) {
                {
                    this->partitions.clear();
                    uint32_t _size26;
                    ::apache::thrift::protocol::TType _etype29;
                    xfer += iprot->readListBegin(_etype29, _size26);
                    this->partitions.resize(_size26);
                    uint32_t _i30;
                    for (_i30 = 0; _i30 < _size26; ++_i30) {
                        xfer += this->partitions[_i30].read(iprot);
                    }
                    xfer += iprot->readListEnd();
                }
                this->__isset.partitions = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        default:
            xfer += iprot->skip(ftype);
            break;
        }
        xfer += iprot->readFieldEnd();
    }

    xfer += iprot->readStructEnd();

    return xfer;
}

uint32_t
configuration_query_by_index_response::write(::apache::thrift::protocol::TProtocol *oprot) const
{
    uint32_t xfer = 0;
    apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
    xfer += oprot->writeStructBegin("configuration_query_by_index_response");

    xfer += oprot->writeFieldBegin("err", ::apache::thrift::protocol::T_STRUCT, 1);
    xfer += this->err.write(oprot);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("app_id", ::apache::thrift::protocol::T_I32, 2);
    xfer += oprot->writeI32(this->app_id);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("partition_count", ::apache::thrift::protocol::T_I32, 3);
    xfer += oprot->writeI32(this->partition_count);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("is_stateful", ::apache::thrift::protocol::T_BOOL, 4);
    xfer += oprot->writeBool(this->is_stateful);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("partitions", ::apache::thrift::protocol::T_LIST, 5);
    {
        xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT,
                                      static_cast<uint32_t>(this->partitions.size()));
        std::vector<partition_configuration>::const_iterator _iter31;
        for (_iter31 = this->partitions.begin(); _iter31 != this->partitions.end(); ++_iter31) {
            xfer += (*_iter31).write(oprot);
        }
        xfer += oprot->writeListEnd();
    }
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldStop();
    xfer += oprot->writeStructEnd();
    return xfer;
}

void swap(configuration_query_by_index_response &a, configuration_query_by_index_response &b)
{
    using ::std::swap;
    swap(a.err, b.err);
    swap(a.app_id, b.app_id);
    swap(a.partition_count, b.partition_count);
    swap(a.is_stateful, b.is_stateful);
    swap(a.partitions, b.partitions);
    swap(a.__isset, b.__isset);
}

configuration_query_by_index_response::configuration_query_by_index_response(
    const configuration_query_by_index_response &other32)
{
    err = other32.err;
    app_id = other32.app_id;
    partition_count = other32.partition_count;
    is_stateful = other32.is_stateful;
    partitions = other32.partitions;
    __isset = other32.__isset;
}
configuration_query_by_index_response::configuration_query_by_index_response(
    configuration_query_by_index_response &&other33)
{
    err = std::move(other33.err);
    app_id = std::move(other33.app_id);
    partition_count = std::move(other33.partition_count);
    is_stateful = std::move(other33.is_stateful);
    partitions = std::move(other33.partitions);
    __isset = std::move(other33.__isset);
}
configuration_query_by_index_response &configuration_query_by_index_response::
operator=(const configuration_query_by_index_response &other34)
{
    err = other34.err;
    app_id = other34.app_id;
    partition_count = other34.partition_count;
    is_stateful = other34.is_stateful;
    partitions = other34.partitions;
    __isset = other34.__isset;
    return *this;
}
configuration_query_by_index_response &configuration_query_by_index_response::
operator=(configuration_query_by_index_response &&other35)
{
    err = std::move(other35.err);
    app_id = std::move(other35.app_id);
    partition_count = std::move(other35.partition_count);
    is_stateful = std::move(other35.is_stateful);
    partitions = std::move(other35.partitions);
    __isset = std::move(other35.__isset);
    return *this;
}
void configuration_query_by_index_response::printTo(std::ostream &out) const
{
    using ::apache::thrift::to_string;
    out << "configuration_query_by_index_response(";
    out << "err=" << to_string(err);
    out << ", "
        << "app_id=" << to_string(app_id);
    out << ", "
        << "partition_count=" << to_string(partition_count);
    out << ", "
        << "is_stateful=" << to_string(is_stateful);
    out << ", "
        << "partitions=" << to_string(partitions);
    out << ")";
}

app_info::~app_info() throw() {}

void app_info::__set_status(const app_status::type val) { this->status = val; }

void app_info::__set_app_type(const std::string &val) { this->app_type = val; }

void app_info::__set_app_name(const std::string &val) { this->app_name = val; }

void app_info::__set_app_id(const int32_t val) { this->app_id = val; }

void app_info::__set_partition_count(const int32_t val) { this->partition_count = val; }

void app_info::__set_envs(const std::map<std::string, std::string> &val) { this->envs = val; }

void app_info::__set_is_stateful(const bool val) { this->is_stateful = val; }

void app_info::__set_max_replica_count(const int32_t val) { this->max_replica_count = val; }

void app_info::__set_expire_second(const int64_t val) { this->expire_second = val; }

void app_info::__set_create_second(const int64_t val) { this->create_second = val; }

void app_info::__set_drop_second(const int64_t val) { this->drop_second = val; }

void app_info::__set_duplicating(const bool val)
{
    this->duplicating = val;
    __isset.duplicating = true;
}

void app_info::__set_init_partition_count(const int32_t val) { this->init_partition_count = val; }

void app_info::__set_is_bulk_loading(const bool val) { this->is_bulk_loading = val; }

uint32_t app_info::read(::apache::thrift::protocol::TProtocol *iprot)
{

    apache::thrift::protocol::TInputRecursionTracker tracker(*iprot);
    uint32_t xfer = 0;
    std::string fname;
    ::apache::thrift::protocol::TType ftype;
    int16_t fid;

    xfer += iprot->readStructBegin(fname);

    using ::apache::thrift::protocol::TProtocolException;

    while (true) {
        xfer += iprot->readFieldBegin(fname, ftype, fid);
        if (ftype == ::apache::thrift::protocol::T_STOP) {
            break;
        }
        switch (fid) {
        case 1:
            if (ftype == ::apache::thrift::protocol::T_I32) {
                int32_t ecast36;
                xfer += iprot->readI32(ecast36);
                this->status = (app_status::type)ecast36;
                this->__isset.status = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 2:
            if (ftype == ::apache::thrift::protocol::T_STRING) {
                xfer += iprot->readString(this->app_type);
                this->__isset.app_type = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 3:
            if (ftype == ::apache::thrift::protocol::T_STRING) {
                xfer += iprot->readString(this->app_name);
                this->__isset.app_name = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 4:
            if (ftype == ::apache::thrift::protocol::T_I32) {
                xfer += iprot->readI32(this->app_id);
                this->__isset.app_id = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 5:
            if (ftype == ::apache::thrift::protocol::T_I32) {
                xfer += iprot->readI32(this->partition_count);
                this->__isset.partition_count = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 6:
            if (ftype == ::apache::thrift::protocol::T_MAP) {
                {
                    this->envs.clear();
                    uint32_t _size37;
                    ::apache::thrift::protocol::TType _ktype38;
                    ::apache::thrift::protocol::TType _vtype39;
                    xfer += iprot->readMapBegin(_ktype38, _vtype39, _size37);
                    uint32_t _i41;
                    for (_i41 = 0; _i41 < _size37; ++_i41) {
                        std::string _key42;
                        xfer += iprot->readString(_key42);
                        std::string &_val43 = this->envs[_key42];
                        xfer += iprot->readString(_val43);
                    }
                    xfer += iprot->readMapEnd();
                }
                this->__isset.envs = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 7:
            if (ftype == ::apache::thrift::protocol::T_BOOL) {
                xfer += iprot->readBool(this->is_stateful);
                this->__isset.is_stateful = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 8:
            if (ftype == ::apache::thrift::protocol::T_I32) {
                xfer += iprot->readI32(this->max_replica_count);
                this->__isset.max_replica_count = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 9:
            if (ftype == ::apache::thrift::protocol::T_I64) {
                xfer += iprot->readI64(this->expire_second);
                this->__isset.expire_second = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 10:
            if (ftype == ::apache::thrift::protocol::T_I64) {
                xfer += iprot->readI64(this->create_second);
                this->__isset.create_second = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 11:
            if (ftype == ::apache::thrift::protocol::T_I64) {
                xfer += iprot->readI64(this->drop_second);
                this->__isset.drop_second = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 12:
            if (ftype == ::apache::thrift::protocol::T_BOOL) {
                xfer += iprot->readBool(this->duplicating);
                this->__isset.duplicating = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 13:
            if (ftype == ::apache::thrift::protocol::T_I32) {
                xfer += iprot->readI32(this->init_partition_count);
                this->__isset.init_partition_count = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        case 14:
            if (ftype == ::apache::thrift::protocol::T_BOOL) {
                xfer += iprot->readBool(this->is_bulk_loading);
                this->__isset.is_bulk_loading = true;
            } else {
                xfer += iprot->skip(ftype);
            }
            break;
        default:
            xfer += iprot->skip(ftype);
            break;
        }
        xfer += iprot->readFieldEnd();
    }

    xfer += iprot->readStructEnd();

    return xfer;
}

uint32_t app_info::write(::apache::thrift::protocol::TProtocol *oprot) const
{
    uint32_t xfer = 0;
    apache::thrift::protocol::TOutputRecursionTracker tracker(*oprot);
    xfer += oprot->writeStructBegin("app_info");

    xfer += oprot->writeFieldBegin("status", ::apache::thrift::protocol::T_I32, 1);
    xfer += oprot->writeI32((int32_t)this->status);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("app_type", ::apache::thrift::protocol::T_STRING, 2);
    xfer += oprot->writeString(this->app_type);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("app_name", ::apache::thrift::protocol::T_STRING, 3);
    xfer += oprot->writeString(this->app_name);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("app_id", ::apache::thrift::protocol::T_I32, 4);
    xfer += oprot->writeI32(this->app_id);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("partition_count", ::apache::thrift::protocol::T_I32, 5);
    xfer += oprot->writeI32(this->partition_count);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("envs", ::apache::thrift::protocol::T_MAP, 6);
    {
        xfer += oprot->writeMapBegin(::apache::thrift::protocol::T_STRING,
                                     ::apache::thrift::protocol::T_STRING,
                                     static_cast<uint32_t>(this->envs.size()));
        std::map<std::string, std::string>::const_iterator _iter44;
        for (_iter44 = this->envs.begin(); _iter44 != this->envs.end(); ++_iter44) {
            xfer += oprot->writeString(_iter44->first);
            xfer += oprot->writeString(_iter44->second);
        }
        xfer += oprot->writeMapEnd();
    }
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("is_stateful", ::apache::thrift::protocol::T_BOOL, 7);
    xfer += oprot->writeBool(this->is_stateful);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("max_replica_count", ::apache::thrift::protocol::T_I32, 8);
    xfer += oprot->writeI32(this->max_replica_count);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("expire_second", ::apache::thrift::protocol::T_I64, 9);
    xfer += oprot->writeI64(this->expire_second);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("create_second", ::apache::thrift::protocol::T_I64, 10);
    xfer += oprot->writeI64(this->create_second);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("drop_second", ::apache::thrift::protocol::T_I64, 11);
    xfer += oprot->writeI64(this->drop_second);
    xfer += oprot->writeFieldEnd();

    if (this->__isset.duplicating) {
        xfer += oprot->writeFieldBegin("duplicating", ::apache::thrift::protocol::T_BOOL, 12);
        xfer += oprot->writeBool(this->duplicating);
        xfer += oprot->writeFieldEnd();
    }
    xfer += oprot->writeFieldBegin("init_partition_count", ::apache::thrift::protocol::T_I32, 13);
    xfer += oprot->writeI32(this->init_partition_count);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldBegin("is_bulk_loading", ::apache::thrift::protocol::T_BOOL, 14);
    xfer += oprot->writeBool(this->is_bulk_loading);
    xfer += oprot->writeFieldEnd();

    xfer += oprot->writeFieldStop();
    xfer += oprot->writeStructEnd();
    return xfer;
}

void swap(app_info &a, app_info &b)
{
    using ::std::swap;
    swap(a.status, b.status);
    swap(a.app_type, b.app_type);
    swap(a.app_name, b.app_name);
    swap(a.app_id, b.app_id);
    swap(a.partition_count, b.partition_count);
    swap(a.envs, b.envs);
    swap(a.is_stateful, b.is_stateful);
    swap(a.max_replica_count, b.max_replica_count);
    swap(a.expire_second, b.expire_second);
    swap(a.create_second, b.create_second);
    swap(a.drop_second, b.drop_second);
    swap(a.duplicating, b.duplicating);
    swap(a.init_partition_count, b.init_partition_count);
    swap(a.is_bulk_loading, b.is_bulk_loading);
    swap(a.__isset, b.__isset);
}

app_info::app_info(const app_info &other45)
{
    status = other45.status;
    app_type = other45.app_type;
    app_name = other45.app_name;
    app_id = other45.app_id;
    partition_count = other45.partition_count;
    envs = other45.envs;
    is_stateful = other45.is_stateful;
    max_replica_count = other45.max_replica_count;
    expire_second = other45.expire_second;
    create_second = other45.create_second;
    drop_second = other45.drop_second;
    duplicating = other45.duplicating;
    init_partition_count = other45.init_partition_count;
    is_bulk_loading = other45.is_bulk_loading;
    __isset = other45.__isset;
}
app_info::app_info(app_info &&other46)
{
    status = std::move(other46.status);
    app_type = std::move(other46.app_type);
    app_name = std::move(other46.app_name);
    app_id = std::move(other46.app_id);
    partition_count = std::move(other46.partition_count);
    envs = std::move(other46.envs);
    is_stateful = std::move(other46.is_stateful);
    max_replica_count = std::move(other46.max_replica_count);
    expire_second = std::move(other46.expire_second);
    create_second = std::move(other46.create_second);
    drop_second = std::move(other46.drop_second);
    duplicating = std::move(other46.duplicating);
    init_partition_count = std::move(other46.init_partition_count);
    is_bulk_loading = std::move(other46.is_bulk_loading);
    __isset = std::move(other46.__isset);
}
app_info &app_info::operator=(const app_info &other47)
{
    status = other47.status;
    app_type = other47.app_type;
    app_name = other47.app_name;
    app_id = other47.app_id;
    partition_count = other47.partition_count;
    envs = other47.envs;
    is_stateful = other47.is_stateful;
    max_replica_count = other47.max_replica_count;
    expire_second = other47.expire_second;
    create_second = other47.create_second;
    drop_second = other47.drop_second;
    duplicating = other47.duplicating;
    init_partition_count = other47.init_partition_count;
    is_bulk_loading = other47.is_bulk_loading;
    __isset = other47.__isset;
    return *this;
}
app_info &app_info::operator=(app_info &&other48)
{
    status = std::move(other48.status);
    app_type = std::move(other48.app_type);
    app_name = std::move(other48.app_name);
    app_id = std::move(other48.app_id);
    partition_count = std::move(other48.partition_count);
    envs = std::move(other48.envs);
    is_stateful = std::move(other48.is_stateful);
    max_replica_count = std::move(other48.max_replica_count);
    expire_second = std::move(other48.expire_second);
    create_second = std::move(other48.create_second);
    drop_second = std::move(other48.drop_second);
    duplicating = std::move(other48.duplicating);
    init_partition_count = std::move(other48.init_partition_count);
    is_bulk_loading = std::move(other48.is_bulk_loading);
    __isset = std::move(other48.__isset);
    return *this;
}
void app_info::printTo(std::ostream &out) const
{
    using ::apache::thrift::to_string;
    out << "app_info(";
    out << "status=" << to_string(status);
    out << ", "
        << "app_type=" << to_string(app_type);
    out << ", "
        << "app_name=" << to_string(app_name);
    out << ", "
        << "app_id=" << to_string(app_id);
    out << ", "
        << "partition_count=" << to_string(partition_count);
    out << ", "
        << "envs=" << to_string(envs);
    out << ", "
        << "is_stateful=" << to_string(is_stateful);
    out << ", "
        << "max_replica_count=" << to_string(max_replica_count);
    out << ", "
        << "expire_second=" << to_string(expire_second);
    out << ", "
        << "create_second=" << to_string(create_second);
    out << ", "
        << "drop_second=" << to_string(drop_second);
    out << ", "
        << "duplicating=";
    (__isset.duplicating ? (out << to_string(duplicating)) : (out << "<null>"));
    out << ", "
        << "init_partition_count=" << to_string(init_partition_count);
    out << ", "
        << "is_bulk_loading=" << to_string(is_bulk_loading);
    out << ")";
}

} // namespace
