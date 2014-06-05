##! Log writer for sending logs to a Kafka instance.
##!
##! Note: This module is in testing and is not yet considered stable!

module LogKafka;

export {
    ## List of Kafka instances, separated by commas
    const server_list = "127.0.0.1:9092" &redef;

    ## Name of the Kafka topic.
    const topic_name = "bro" &redef;

    ## Kafka Client ID
    const client_id = "bro" &redef;

    ## Compression codec: none, gzip, snappy
    const compression_codec = "none" &redef;


    ## The batch size is the number of messages that will be queued up before
    ## they are sent to Kafka.
    const max_batch_size = 1000 &redef;

    ## The maximum amount of wall-clock time that is allowed to pass without
    ## finishing a bulk log send.  This represents the maximum delay you
    ## would like to have with your logs before they are sent to Kafka.
    const max_batch_interval = 1min &redef;

    ## The maximum byte size for a buffered JSON string to send to the bulk
    ## insert API.
    const max_byte_size = 1024 * 1024 &redef;

    ## Format of timestamps when writing out JSON. By default, the JSON formatter will
    ## use double values for timestamps which represent the number of seconds from the
    ## UNIX epoch.
    const json_timestamps: JSON::TimestampFormat = JSON::TS_EPOCH &redef;
}

