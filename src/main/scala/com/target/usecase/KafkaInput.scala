package com.target.usecase;

import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import com.cisco.pnda.model._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext

class KafkaInput extends Serializable {
    def readFromKafka (ssc: StreamingContext) = {
        val props = AppConfig.loadProperties();
        val topicsSet = props.getProperty("kafka.topic").split(",").toSet;
        val kafkaParams = collection.mutable.Map[String, String]("metadata.broker.list" -> props.getProperty("kafka.brokers"))
        if (props.getProperty("kafka.consume_from_beginning").toBoolean)
        {
            kafkaParams.put("auto.offset.reset", "smallest");
        }
        val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams.toMap, topicsSet).repartition(Integer.parseInt(props.getProperty("app.processing_parallelism")));

        // Decode avro container format
        val avroSchemaString = StaticHelpers.loadResourceFile("viewshipdata-raw.avsc");
        val rawMessages = messages.map(x => {
        val eventDecoder = new DataPlatformEventDecoder(avroSchemaString);
        val payload = x._2;
        val dataPlatformEvent = eventDecoder.decode(payload);
        dataPlatformEvent;
        });
        rawMessages;
    };
}
