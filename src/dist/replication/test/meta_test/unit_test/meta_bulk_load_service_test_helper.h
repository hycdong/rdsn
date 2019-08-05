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
