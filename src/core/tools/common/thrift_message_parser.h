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

#pragma once

#include <dsn/tool-api/message_parser.h>
#include <dsn/tool-api/rpc_message.h>
#include <dsn/utility/ports.h>

namespace dsn {
// request header (in big-endian)
struct thrift_message_header
{
    uint32_t hdr_type;    ///< must be "THFT"
    uint32_t hdr_version; ///< must be 0
    uint32_t hdr_length;  ///< must be sizeof(thrift_message_header)
    uint32_t hdr_crc32;
    uint32_t body_length;
    uint32_t body_crc32;
    int32_t app_id;
    int32_t partition_index;
    int32_t client_timeout;
    int32_t client_thread_hash;
    uint64_t client_partition_hash;
    //------------- sizeof(thrift_message_header) = 48 ----------//
};

#define THRIFT_HDR_SIG (*(uint32_t *)"THFT")

DEFINE_CUSTOMIZED_ID(network_header_format, NET_HDR_THRIFT)

// Parses request sent in rDSN thrift protocol, which is
// mainly used by our Java/GoLang/NodeJs/Python clients,
// and encodes response to them.
class thrift_message_parser final : public message_parser
{
public:
    thrift_message_parser() : _header_parsed(false) {}

    ~thrift_message_parser() {}

    void reset() override;

    message_ex *get_message_on_receive(message_reader *reader,
                                       /*out*/ int &read_next) override;

    // response format:
    //     <total_len(int32)> <thrift_string> <thrift_message_begin> <body_data(bytes)>
    //     <thrift_message_end>
    void prepare_on_send(message_ex *msg) override;

    int get_buffers_on_send(message_ex *msg, /*out*/ send_buf *buffers) override;

public:
    static void read_thrift_header(const char *buffer, /*out*/ thrift_message_header &header);
    static bool check_thrift_header(const thrift_message_header &header);

    static dsn::message_ex *parse_message(const thrift_message_header &thrift_header,
                                          dsn::blob &message_data);

private:
    thrift_message_header _thrift_header;
    bool _header_parsed;
};

} // namespace dsn
