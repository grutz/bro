// See the file "COPYING" in the main distribution directory for copyright.
//
// This is experimental code that is not yet ready for production usage.
//


#include "config.h"

#ifdef USE_KAFKA

#include "util.h" // Needs to come first for stdint.h

#include <string>
#include <errno.h>

//#include <librdkafka/rdkafkacpp.h>

#include "Debug.h"
#include "BroString.h"
#include "NetVar.h"
#include "threading/SerialTypes.h"

#include "Kafka.h"

using namespace logging;
using namespace writer;
using threading::Value;
using threading::Field;
using namespace RdKafka;

Kafka::Kafka(WriterFrontend* frontend) : WriterBackend(frontend)
{
    //json_to_stdout = BifConst::LogKafka::json_to_stdout;
    json_formatter = 0;

    server_list_len = BifConst::LogKafka::server_list->Len();
    server_list = new char[server_list_len + 1];
    memcpy(server_list, BifConst::LogKafka::server_list->Bytes(), server_list_len);
    server_list[server_list_len] = 0;

    topic_name_len = BifConst::LogKafka::topic_name->Len();
    topic_name = new char[topic_name_len + 1];
    memcpy(topic_name, BifConst::LogKafka::topic_name->Bytes(), topic_name_len);
    topic_name[topic_name_len] = 0;

    client_id_len = BifConst::LogKafka::client_id->Len();
    client_id = new char[client_id_len + 1];
    memcpy(client_id, BifConst::LogKafka::client_id->Bytes(), client_id_len);
    client_id[client_id_len] = 0;

    compression_codec_len = BifConst::LogKafka::compression_codec->Len();
    compression_codec = new char[compression_codec_len + 1];
    memcpy(compression_codec, BifConst::LogKafka::compression_codec->Bytes(), compression_codec_len);
    compression_codec[compression_codec_len] = 0;

    buffer.Clear();
    counter = 0;
    last_send = current_time();

    json_formatter = new threading::formatter::JSON(this, threading::formatter::JSON::TS_MILLIS);
}

Kafka::~Kafka()
{
    delete [] server_list;
    delete [] topic_name;
    delete [] client_id;
    delete [] compression_codec;
    //delete [] producer;
    //delete [] topic;
    delete json_formatter;
}

bool Kafka::DoInit(const WriterInfo& info, int num_fields, const threading::Field* const* fields)
{
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    if (conf->set("metadata.broker.list", server_list, errstr) != RdKafka::Conf::CONF_OK) {
        reporter->Error("Failed to set metadata.broker.list: %s", errstr.c_str());
        return false;
    }

    conf->set("compression.codec", compression_codec, errstr);
    conf->set("client.id", client_id, errstr);

    int32_t partition = RdKafka::Topic::PARTITION_UA;

    producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        reporter->Error("Failed to create producer: %s", errstr.c_str());
        return false;
    }

    topic = RdKafka::Topic::create(producer, topic_name, tconf, errstr);
    if (!topic) {
        reporter->Error("Failed to create topic: %s", errstr.c_str());
        return false;
    }

    return true;
}
bool Kafka::BatchIndex()
    {

    const char* bytes = (const char*)buffer.Bytes();
    std::string errstr;

    RdKafka::ErrorCode resp = producer->produce(topic, partition,
                                RdKafka::Producer::MSG_COPY /* Copy payload */,
                                const_cast<char *>(bytes), strlen(bytes),
              NULL, NULL);
    if (resp != RdKafka::ERR_NO_ERROR) {
        errstr = RdKafka::err2str(resp);
        reporter->Error( "Produce failed: %s", errstr.c_str());
    }
#ifdef DEBUG
    else {
        const char* msg = Fmt("Produced message (%d bytes)", strlen(bytes));
        Debug(DBG_LOGGING, msg);
    }
#endif

    producer->poll(0);
    buffer.Clear();
    counter = 0;
    last_send = current_time();

    return true;
    }

bool Kafka::DoWrite(int num_fields, const Field* const * fields, Value** vals)
    {
    // create JSON for Kafka message
    buffer.AddRaw("{\"", 2);
    buffer.Add(Info().path);
    buffer.AddRaw("\":", 2);

    json_formatter->Describe(&buffer, num_fields, fields, vals);

    buffer.AddRaw("}", 1);

    counter++;
    if ( counter >= BifConst::LogKafka::max_batch_size ||
        uint(buffer.Len()) >= BifConst::LogKafka::max_byte_size )
        BatchIndex();

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
    RdKafka::wait_destroyed(5000);
    return true;
    }

bool Kafka::DoHeartbeat(double network_time, double current_time)
    {
    if ( last_send > 0 && buffer.Len() > 0 &&
        current_time-last_send > BifConst::LogKafka::max_batch_interval )
        {
        BatchIndex();
        }

    return true;
    }

bool Kafka::DoRotate(const char* rotated_path, double open, double close, bool terminating)
    {
    // Nothing to do.
    FinishedRotation();
    return true;
    }

#endif
