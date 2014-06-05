// See the file "COPYING" in the main distribution directory for copyright.
//
// Log writer for writing to an Kafka (v0.8+) database using librdkafka
// (https://github.com/edenhill/librdkafka)
//
// This is experimental code that is not yet ready for production usage.
//

#ifndef LOGGING_WRITER_KAFKA_H
#define LOGGING_WRITER_KAFKA_H

//#include "threading/formatters/Ascii.h"
#include "threading/formatters/JSON.h"
#include "../WriterBackend.h"
#include <librdkafka/rdkafkacpp.h>

namespace logging { namespace writer {

class Kafka : public WriterBackend {
public:
    Kafka(WriterFrontend* frontend);
    ~Kafka();

    static WriterBackend* Instantiate(WriterFrontend* frontend)
        { return new Kafka(frontend); }
    static string LogExt();

protected:
    // Overidden from WriterBackend.

    virtual bool DoInit(const WriterInfo& info, int num_fields,
                const threading::Field* const* fields);
    virtual bool BatchIndex();
    virtual bool DoWrite(int num_fields, const threading::Field* const* fields,
                 threading::Value** vals);
    virtual bool DoFinish(double network_time);
    virtual bool DoFlush(double network_time);
    virtual bool DoSetBuf(bool enabled);
    virtual bool DoHeartbeat(double network_time, double current_time);
    virtual bool DoRotate(const char* rotated_path, double open, double close, bool terminating);

private:
    bool ProduceToKafka();

    ODesc buffer;
    uint64 counter;
    double last_send;

    char* server_list;
    int server_list_len;
    char* topic_name;
    int topic_name_len;
    char* client_id;
    int client_id_len;
    char* compression_codec;
    int compression_codec_len;

    std::string errstr;
    RdKafka::Conf *conf;
    RdKafka::Conf *tconf;
    RdKafka::Producer *producer;
    RdKafka::Topic *topic;
    int32_t partition;

    threading::formatter::JSON* json_formatter;

};

}
}


#endif
