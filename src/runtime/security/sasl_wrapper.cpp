// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "sasl_wrapper.h"
#include "sasl_server_wrapper.h"
#include "sasl_client_wrapper.h"

#include <sasl/sasl.h>

namespace dsn {
namespace security {
const char *sasl_err_desc(int status, sasl_conn_t *conn)
{
    if (conn != nullptr) {
        return sasl_errdetail(conn);
    } else {
        return sasl_errstring(status, nullptr, nullptr);
    }
}

sasl_wrapper::~sasl_wrapper()
{
    if (nullptr != _conn) {
        sasl_dispose(&_conn);
    }
}

error_s sasl_wrapper::wrap_error(int sasl_err)
{
    error_s ret;
    switch (sasl_err) {
    case SASL_OK:
        return error_s::make(ERR_OK);
    case SASL_CONTINUE:
        return error_s::make(ERR_SASL_INCOMPLETE);
    case SASL_FAIL:      // Generic failure (encompasses missing krb5 credentials).
    case SASL_BADAUTH:   // Authentication failure.
    case SASL_BADMAC:    // Decode failure.
    case SASL_NOAUTHZ:   // Authorization failure.
    case SASL_NOUSER:    // User not found.
    case SASL_WRONGMECH: // Server doesn't support requested mechanism.
    case SASL_BADSERV: { // Server failed mutual authentication.
        ret = error_s::make(ERR_SASL_INTERNAL);
        ret << "sasl auth failed, error: " << sasl_err_desc(sasl_err, _conn);
        break;
    }
    default:
        ret = error_s::make(ERR_UNKNOWN);
        break;
    }
    return ret;
}

std::unique_ptr<sasl_wrapper> create_sasl_wrapper(bool is_client)
{
    if (is_client) {
        return make_unique<sasl_client_wrapper>();
    } else {
        return make_unique<sasl_server_wrapper>();
    }
}
} // namespace security
} // namespace dsn