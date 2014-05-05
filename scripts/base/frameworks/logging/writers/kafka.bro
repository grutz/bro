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

    ## Format of timestamps when writing out JSON. By default, the JSON formatter will
    ## use double values for timestamps which represent the number of seconds from the
    ## UNIX epoch.
    const json_timestamps: JSON::TimestampFormat = JSON::TS_EPOCH &redef;
}

