// See the file "COPYING" in the main distribution directory for copyright.
//
// Log writer for writing to an Kafka (v0.8+) database using libkafka
// (https://github.com/adobe-research/libkafka)
//
// This is experimental code that is not yet ready for production usage.
//

#ifndef LOGGING_WRITER_KAFKA_H
#define LOGGING_WRITER_KAFKA_H

//#include "threading/formatters/Ascii.h"
#include "threading/formatters/JSON.h"
#include "../WriterBackend.h"
#include <libkafka/Client.h>
#include <libkafka/Message.h>
#include <libkafka/produce/ProduceRequest.h>
#include <libkafka/TopicNameBlock.h>

namespace logging { namespace writer {

class Kafka : public WriterBackend {
public:
    Kafka(WriterFrontend* frontend);
    ~Kafka();

    static WriterBackend* Instantiate(WriterFrontend* frontend)
        { return new Kafka(frontend); }
    static string LogExt();
    LibKafka::Client *kafka_client;

protected:
    // Overidden from WriterBackend.

    virtual bool DoInit(const WriterInfo& info, int num_fields,
                const threading::Field* const* fields);

    virtual bool DoWrite(int num_fields, const threading::Field* const* fields,
                 threading::Value** vals);
    virtual bool DoFinish(double network_time);
    virtual bool DoFlush(double network_time);
    virtual bool DoSetBuf(bool enabled);
    virtual bool DoHeartbeat(double network_time, double current_time);
    virtual bool DoRotate(const char* rotated_path, double open, double close, bool terminating);
    virtual LibKafka::Message* createMessage(const char * value, const char *key);
    virtual LibKafka::ProduceRequest* createProduceRequest(string topic_name, LibKafka::Message **messageArray, int messageArraySize);
    virtual LibKafka::TopicNameBlock<LibKafka::ProduceMessageSet>* createProduceRequestTopicNameBlock(string topic_name, LibKafka::Message **messageArray, int messageArraySize);
    virtual LibKafka::ProduceMessageSet* createProduceMessageSet(LibKafka::Message **messageArray, int messageArraySize);
    virtual LibKafka::MessageSet* createMessageSet(LibKafka::Message **messageArray, int messageArraySize);

private:
    bool ProduceToKafka();

    ODesc buffer;

    char* server_name;
    int server_name_len;
    int server_port;
    char* topic_name;
    int topic_name_len;
    char* client_id;
    int client_id_len;

    LibKafka::TopicNameBlock<LibKafka::ProduceMessageSet>** produceTopicArray;
    LibKafka::ProduceMessageSet** produceMessageSetArray;
    LibKafka::MessageSet* messageSet;
    vector<LibKafka::Message*> messageVector;

    threading::formatter::JSON* json_formatter;

};

}
}


#endif
