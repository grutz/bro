##! Log writer for sending logs to a Kafka instance.
##!
##! Note: This module is in testing and is not yet considered stable!

module LogKafka;

export {
    ## Name of the Kafka instance.
    const server_name = "kafka" &redef;

    ## Kafka port.
    const server_port = 2181 &redef;

    ## Name of the Kafka topic.
    const topic_name = "bro" &redef;

    ## Format of timestamps when writing out JSON. By default, the JSON formatter will
    ## use double values for timestamps which represent the number of seconds from the
    ## UNIX epoch.
    const json_timestamps: JSON::TimestampFormat = JSON::TS_EPOCH &redef;
}

