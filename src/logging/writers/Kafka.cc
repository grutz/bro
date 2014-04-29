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

#include "Kafka.h"

using namespace logging;
using namespace writer;
using threading::Value;
using threading::Field;

Kafka::Kafka(WriterFrontend* frontend) : WriterBackend(frontend)
{
    //json_to_stdout = BifConst::LogKafka::json_to_stdout;
    json_formatter = 0;

    server_name_len = BifConst::LogKafka::server_name->Len();
    server_name = new char[server_name_len + 1];
    memcpy(server_name, BifConst::LogKafka::server_name->Bytes(), server_name_len);
    server_name[server_name_len] = 0;

    server_port = (int) BifConst::LogKafka::server_port;

    topic_name_len = BifConst::LogKafka::topic_name->Len();
    topic_name = new char[topic_name_len + 1];
    memcpy(topic_name, BifConst::LogKafka::topic_name->Bytes(), topic_name_len);
    topic_name[topic_name_len] = 0;
    buffer.Clear();

    json_formatter = new threading::formatter::JSON(this, threading::formatter::JSON::TS_MILLIS);
}

Kafka::~Kafka()
{
    delete [] server_name;
    delete [] topic_name;
    delete json_formatter;
}

bool Kafka::DoInit(const WriterInfo& info, int num_fields, const threading::Field* const* fields)
{
    return true;
}

bool Kafka::DoWrite(int num_fields, const Field* const * fields,
                 Value** vals)
    {
    // create JSON for Kafka message
    buffer.AddRaw("{\"", 2);
    buffer.Add(Info().path);
    buffer.AddRaw("\":", 2);

    json_formatter->Describe(&buffer, num_fields, fields, vals);

    buffer.AddRaw("}\n", 2);

    ProduceToKafka();

    return true;
    }

bool Kafka::ProduceToKafka()
    {
    // TODO: Actually produce to Kafka
    const char* bytes = (const char*)buffer.Bytes();
    fprintf(stdout, "%s\n", bytes);
    return true;
    }

bool Kafka::DoSetBuf(bool enabled)
    {
    // Nothing to do.
    return true;
    }

bool Kafka::DoFlush(double network_time)
    {
    // Nothing to do.
    return true;
    }

bool Kafka::DoFinish(double network_time)
    {
    // Nothing to do.
    return true;
    }

bool Kafka::DoHeartbeat(double network_time, double current_time)
    {
    // Nothing to do.
    return true;
    }

bool Kafka::DoRotate(const char* rotated_path, double open, double close, bool terminating)
    {
    // Nothing to do.
    return true;
    }

#endif
