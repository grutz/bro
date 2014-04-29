// See the file "COPYING" in the main distribution directory for copyright.
//
// This is experimental code that is not yet ready for production usage.
//


#include "config.h"

#ifdef USE_KAFKA

#include "util.h" // Needs to come first for stdint.h

#include <string>
#include <errno.h>

#include <libkafka/ApiConstants.h>
#include <libkafka/Client.h>
#include <libkafka/Message.h>
#include <libkafka/MessageSet.h>
#include <libkafka/TopicNameBlock.h>
#include <libkafka/produce/ProduceMessageSet.h>
#include <libkafka/produce/ProduceRequest.h>
#include <libkafka/produce/ProduceResponsePartition.h>
#include <libkafka/produce/ProduceResponse.h>

#include "Debug.h"
#include "BroString.h"
#include "NetVar.h"
#include "threading/SerialTypes.h"

#include "Kafka.h"

using namespace logging;
using namespace writer;
using threading::Value;
using threading::Field;
using namespace LibKafka;

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

    client_id_len = BifConst::LogKafka::client_id->Len();
    client_id = new char[client_id_len + 1];
    memcpy(client_id, BifConst::LogKafka::client_id->Bytes(), client_id_len);
    client_id[client_id_len] = 0;

    buffer.Clear();

    json_formatter = new threading::formatter::JSON(this, threading::formatter::JSON::TS_MILLIS);

    LibKafka::Client *kafka_client = new Client(server_name, server_port);
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
    //fprintf(stdout, "%s\n", bytes);
    Message *m = createMessage(bytes, Info().path);
    Message **messageArray = &m;

    ProduceRequest *request = createProduceRequest(topic_name, messageArray, 1);

    // optionally set compression mode, will be automatic when messages are sent
    //request->setCompression(ApiConstants::MESSAGE_COMPRESSION_GZIP);

    ProduceResponse *response = kafka_client->sendProduceRequest(request);

    return true;
    }

Message* Kafka::createMessage(const char *value, const char *key)
{
    // these will be updated as the message is prepared for production
    const static int crc = 1001;
    const static signed char magicByte = -1;
    const static signed char attributes = 0; // last three bits must be zero to disable gzip compression

    unsigned char *v = new unsigned char[strlen(value)];
    memcpy(v, value, strlen(value));

    unsigned char *k = new unsigned char[strlen(key)];
    memcpy(k, key, strlen(key));

    return new Message(crc, magicByte, attributes, strlen(key), (unsigned char *)k, strlen(value), (unsigned char *)v, 0, true);
}

ProduceRequest* Kafka::createProduceRequest(string topic_name, Message **messageArray, int messageArraySize)
{
    const int correlationId = 212121;
    const static int requiredAcks = 1;
    const static int timeout = 20;

    int produceTopicArraySize = 1;
    produceTopicArray = new TopicNameBlock<ProduceMessageSet>*[produceTopicArraySize];
    for (int i=0; i<produceTopicArraySize; i++) {
        produceTopicArray[i] = createProduceRequestTopicNameBlock(topic_name, messageArray, messageArraySize);
    }
    return new ProduceRequest(correlationId, client_id, requiredAcks, timeout, produceTopicArraySize, produceTopicArray, true);
}

TopicNameBlock<ProduceMessageSet>* Kafka::createProduceRequestTopicNameBlock(string topic_name, Message **messageArray, int messageArraySize)
{
  const int produceMessageSetArraySize = 1;

  produceMessageSetArray = new ProduceMessageSet*[produceMessageSetArraySize];
  for (int i=0; i<produceMessageSetArraySize; i++) {
    produceMessageSetArray[i] = createProduceMessageSet(messageArray, messageArraySize);
  }
  return new TopicNameBlock<ProduceMessageSet>(topic_name, produceMessageSetArraySize, produceMessageSetArray, true);
}


ProduceMessageSet* Kafka::createProduceMessageSet(Message **messageArray, int messageArraySize)
{
  messageSet = createMessageSet(messageArray, messageArraySize);
  int messageSetSize = messageSet->getWireFormatSize(false);
  // using partition = 0
  return new ProduceMessageSet(0, messageSetSize, messageSet, true);
}

MessageSet* Kafka::createMessageSet(Message **messageArray, int messageArraySize)
{
  int messageSetSize = 0;
  messageVector.clear();

  for (int i = 0 ; i < messageArraySize ; i++)
  {
    messageVector.push_back(messageArray[i]);
    // sizeof(offset) + sizeof(messageSize) + messageSize
    messageSetSize += sizeof(long int) + sizeof(int) + messageArray[i]->getWireFormatSize(false);
  }

  return new LibKafka::MessageSet(messageSetSize, messageVector, true);
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
