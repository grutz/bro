// See the file "COPYING" in the main distribution directory for copyright.
//
// Log writer for writing to an Kafka (v0.8+) database using libkafka
// (https://github.com/adobe-research/libkafka)
//
// This is experimental code that is not yet ready for production usage.
//

#ifndef LOGGING_WRITER_KAFKA_H
#define LOGGING_WRITER_KAFKA_H

#include "threading/formatters/Ascii.h"
#include "threading/formatters/JSON.h"
#include "../WriterBackend.h"

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

    char* server_name;
    int server_name_len;
    int server_port;
    char* topic_name;
    int topic_name_len;

    threading::formatter::JSON* json_formatter;

};

}
}


#endif
