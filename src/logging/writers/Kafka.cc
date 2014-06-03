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

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb (RdKafka::Message &message) {
    std::cout << "Message delivery for (" << message.len() << " bytes): " <<
        message.errstr() << std::endl;
  }
};

class ExampleEventCb : public RdKafka::EventCb {
 public:
  void event_cb (RdKafka::Event &event) {
    switch (event.type())
    {
      case RdKafka::Event::EVENT_ERROR:
        std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
            event.str() << std::endl;
        break;

      case RdKafka::Event::EVENT_STATS:
        std::cerr << "\"STATS\": " << event.str() << std::endl;
        break;

      case RdKafka::Event::EVENT_LOG:
        fprintf(stderr, "LOG-%i-%s: %s\n",
                event.severity(), event.fac().c_str(), event.str().c_str());
        break;

      default:
        std::cerr << "EVENT " << event.type() <<
            " (" << RdKafka::err2str(event.err()) << "): " <<
            event.str() << std::endl;
        break;
    }
  }
};

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

    json_formatter = new threading::formatter::JSON(this, threading::formatter::JSON::TS_MILLIS);

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    if (conf->set("metadata.broker.list", server_list, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set metadata.broker.list: " << errstr << std::endl;
    }
    std::cerr << "Server list: " << server_list << std::endl;

    conf->set("compression.codec", compression_codec, errstr);
    conf->set("client.id", client_id, errstr);

    fprintf(stderr, "*** Preparing Kafka\n");

    int32_t partition = RdKafka::Topic::PARTITION_UA;
    partition = 1;

    /*
    ExampleEventCb ex_event_cb;
    conf->set("event_cb", &ex_event_cb, errstr);

    ExampleDeliveryReportCb dr_cb;
    conf->set("dr_cb", &dr_cb, errstr);
    */

    /*
    int pass;

    for (pass = 0 ; pass < 2 ; pass++) {
      std::list<std::string> *dump;
      if (pass == 0) {
        dump = conf->dump();
        std::cout << "# Global config" << std::endl;
      } else {
        fprintf(stderr, "*** Dumping tconf...\n");
        dump = tconf->dump();
        std::cout << "# Topic config" << std::endl;
      }

      for (std::list<std::string>::iterator it = dump->begin();
           it != dump->end(); ) {
        std::cout << *it << " = ";
        it++;
        std::cout << *it << std::endl;
        it++;
      }
      std::cout << std::endl;
    }
    */

    /*fprintf(stderr, "*** Creating producer...\n");
    RdKafka::Producer *producer;
    producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "!!!! Failed to create producer: " << errstr << std::endl;
    }

    fprintf(stderr, "*** Creating topic...\n");
    RdKafka::Topic *topic;
    topic = RdKafka::Topic::create(producer, topic_name, tconf, errstr);
    if (!topic) {
        std::cerr << "!!!! Failed to create topic: " << errstr << std::endl;
    }*/
}

Kafka::~Kafka()
{
    delete [] server_list;
    delete [] topic_name;
    delete [] client_id;
    delete [] compression_codec;
    delete [] producer;
    delete [] topic;
    delete json_formatter;
}

bool Kafka::DoInit(const WriterInfo& info, int num_fields, const threading::Field* const* fields)
{
    return true;
}

bool Kafka::DoWrite(int num_fields, const Field* const * fields, Value** vals)
    {
    // create JSON for Kafka message
    buffer.AddRaw("{\"", 2);
    buffer.Add(Info().path);
    buffer.AddRaw("\":", 2);

    json_formatter->Describe(&buffer, num_fields, fields, vals);

    buffer.AddRaw("}\n", 2);

    const char* bytes = (const char*)buffer.Bytes();
    //fprintf(stdout, "%s\n", bytes);

    RdKafka::Conf *new_conf = conf;
    RdKafka::Conf *new_tconf = tconf;

    fprintf(stderr, "*** Creating producer...\n");
    RdKafka::Producer *producer;
    producer = RdKafka::Producer::create(new_conf, errstr);
    if (!producer) {
        std::cerr << "!!!! Failed to create producer: " << errstr << std::endl;
    }

    fprintf(stderr, "*** Creating topic...\n");
    RdKafka::Topic *topic;
    topic = RdKafka::Topic::create(producer, topic_name, new_tconf, errstr);
    if (!topic) {
        std::cerr << "!!!! Failed to create topic: " << errstr << std::endl;
    }

    if (!producer) {
        std::cerr << "!!!! No producer !!!! " << std::endl;
        return false;
    }

    if (!topic) {
        std::cerr << "!!!! No topic !!!!" << std::endl;
        return false;
    }

    fprintf(stderr, "*** Producing to Kafka...\n");
    RdKafka::ErrorCode resp = producer->produce(topic, partition,
                                RdKafka::Producer::MSG_COPY /* Copy payload */,
                                const_cast<char *>(bytes), strlen(bytes),
              NULL, NULL);
    if (resp != RdKafka::ERR_NO_ERROR)
        std::cerr << "% Produce failed: " <<
            RdKafka::err2str(resp) << std::endl;
    else
        std::cerr << "% Produced message (" << strlen(bytes) << " bytes)" << std::endl;

    producer->poll(0);

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
