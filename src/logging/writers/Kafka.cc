// See the file "COPYING" in the main distribution directory for copyright.
//
// This is experimental code that is not yet ready for production usage.
//


#include "config.h"

#ifdef USE_KAFKA

#include "util.h" // Needs to come first for stdint.h

#include <string>
#include <errno.h>
/*
#include <libkafka/ApiConstants.h>
#include <libkafka/Client.h>
#include <libkafka/Message.h>
#include <libkafka/MessageSet.h>
#include <libkafka/TopicNameBlock.h>
#include <libkafka/produce/ProduceMessageSet.h>
#include <libkafka/produce/ProduceRequest.h>
#include <libkafka/produce/ProduceResponsePartition.h>
#include <libkafka/produce/ProduceResponse.h>
*/

#include "Debug.h"
#include "BroString.h"
#include "NetVar.h"
#include "threading/SerialTypes.h"
#include "threading/AsciiFormatter.h"

#include "Kafka.h"

using namespace logging;
using namespace writer;
using threading::Value;
using threading::Field;

Kafka::Kafka(WriterFrontend* frontend) : WriterBackend(frontend)
{
    fd = 0;
    json_to_stdout = BifConst::LogKafka::json_to_stdout;

    hostname_len = BifConst::LogKafka::hostname->Len();
    hostname = new char[hostname_len + 1];
    memcpy(hostname, BifConst::LogKafka::hostname->Bytes(), hostname_len);
    hostname[hostname_len] = 0;

    server_port = (int) BifConst::LogKafka::server_port;

    topic_name_len = BifConst::LogKafka::topic_name->Len();
    topic_name = new char[topic_name_len + 1];
    memcpy(topic_name, BifConst::LogKafka::topic_name->Bytes(), topic_name_len);
    hostname[topic_name_len] = 0;

    buffer.Clear();
    counter = 0;

    ascii = new AsciiFormatter(this, AsciiFormatter::SeparatorInfo());
}

Kafka::~Kafka()
{
    delete [] hostname;
    delete ascii;
}

bool Kafka::DoInit(const WriterInfo& info, int num_fields, const threading::Field* const* fields)
{
    return true;
}

bool Kafka::AddValueToBuffer(ODesc* b, Value* val)
    {
    switch ( val->type )
        {
        // XXX: ES treats 0 as false and any other value as true so bool types go here.
        case TYPE_BOOL:
        case TYPE_INT:
            b->Add(val->val.int_val);
            break;

        case TYPE_COUNT:
        case TYPE_COUNTER:
            /* XXX: This is from the ES code, may not be relevant for hadoop
            {
            // Kafka doesn't seem to support unsigned 64bit ints.
            if ( val->val.uint_val >= INT64_MAX )
                {
                Error(Fmt("count value too large: %" PRIu64, val->val.uint_val));
                b->AddRaw("null", 4);
                }
            else()*/
            b->Add(val->val.uint_val);
            break;
            /*}*/

        case TYPE_PORT:
            b->Add(val->val.port_val.port);
            break;

        case TYPE_SUBNET:
            b->AddRaw("\"", 1);
            b->Add(ascii->Render(val->val.subnet_val));
            b->AddRaw("\"", 1);
            break;

        case TYPE_ADDR:
            b->AddRaw("\"", 1);
            b->Add(ascii->Render(val->val.addr_val));
            b->AddRaw("\"", 1);
            break;

        case TYPE_DOUBLE:
        case TYPE_INTERVAL:
            b->Add(val->val.double_val);
            break;

        case TYPE_TIME:
            {
            // XXX: From ES code but JSON-relevant
            // Kafka uses milliseconds for timestamps and json only
            // supports signed ints (uints can be too large).
            uint64_t ts = (uint64_t) (val->val.double_val * 1000);
            if ( ts >= INT64_MAX )
                {
                Error(Fmt("time value too large: %" PRIu64, ts));
                b->AddRaw("null", 4);
                }
            else
                b->Add(ts);
            break;
            }

        case TYPE_ENUM:
        case TYPE_STRING:
        case TYPE_FILE:
        case TYPE_FUNC:
            {
            b->AddRaw("\"", 1);
            for ( int i = 0; i < val->val.string_val.length; ++i )
                {
                char c = val->val.string_val.data[i];
                // 2byte Unicode escape special characters.
                if ( c < 32 || c > 126 || c == '\n' || c == '"' || c == '\'' || c == '\\' || c == '&' )
                    {
                    static const char hex_chars[] = "0123456789abcdef";
                    b->AddRaw("\\u00", 4);
                    b->AddRaw(&hex_chars[(c & 0xf0) >> 4], 1);
                    b->AddRaw(&hex_chars[c & 0x0f], 1);
                    }
                else
                    b->AddRaw(&c, 1);
                }
            b->AddRaw("\"", 1);
            break;
            }

        case TYPE_TABLE:
            {
            b->AddRaw("[", 1);
            for ( int j = 0; j < val->val.set_val.size; j++ )
                {
                if ( j > 0 )
                    b->AddRaw(",", 1);
                AddValueToBuffer(b, val->val.set_val.vals[j]);
                }
            b->AddRaw("]", 1);
            break;
            }

        case TYPE_VECTOR:
            {
            b->AddRaw("[", 1);
            for ( int j = 0; j < val->val.vector_val.size; j++ )
                {
                if ( j > 0 )
                    b->AddRaw(",", 1);
                AddValueToBuffer(b, val->val.vector_val.vals[j]);
                }
            b->AddRaw("]", 1);
            break;
            }

        default:
            return false;
        }
    return true;
    }

bool Kafka::AddFieldToBuffer(ODesc *b, Value* val, const Field* field)
    {
    if ( ! val->present )
        return false;

    b->AddRaw("\"", 1);
    b->Add(field->name);
    b->AddRaw("\":", 2);
    AddValueToBuffer(b, val);
    return true;
    }

bool Kafka::DoWrite(int num_fields, const Field* const * fields,
                 Value** vals)
    {
    // create JSON for Kafka message
    buffer.AddRaw("{\"", 2);
    buffer.Add(Info().path);
    buffer.AddRaw("\":\n", 4);

    buffer.AddRaw("{", 1);
    for ( int i = 0; i < num_fields; i++ )
        {
        if ( i > 0 && buffer.Bytes()[buffer.Len()] != ',' && vals[i]->present )
            buffer.AddRaw(",", 1);
        AddFieldToBuffer(&buffer, vals[i], fields[i]);
        }
    buffer.AddRaw("}\n", 2);

    }
    debug_msg("%s\n", buffer);
    //ProduceToKafka(buffer);

    return true;
    }

bool Kafka::DoSetBuf(bool enabled)
    {
    // Nothing to do.
    return true;
    }


#endif
