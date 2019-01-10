package com.target.usecase;

import com.target.usecase._
import com.target.usecase.model._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.log4j.Logger;

class KafkaPipeline extends Serializable {

    object Holder extends Serializable {
       @transient lazy val logger = Logger.getLogger(getClass.getName)
    }

    def create() = {

        val parseMessages = (messagesDstream: DStream[DataPlatformEvent]) => {

            val parsedMessages = messagesDstream.flatMap(dataPlatformEvent => {
            val parsed = dataPlatformEvent.getRawdata();
            Some(parsed);
            });
            parsedMessages
        }: DStream[String];

        val props = AppConfig.loadProperties();
        val checkpointDirectory = props.getProperty("app.checkpoint_path");
        val batchSizeSeconds = Integer.parseInt(props.getProperty("app.batch_size_seconds"));

        val sparkConf = new SparkConf();
        Holder.logger.info("Creating new spark context with checkpoint directory: " + checkpointDirectory)
        val ssc = new StreamingContext(sparkConf, Seconds(batchSizeSeconds));

        if (checkpointDirectory.length() > 0) {
            ssc.checkpoint(checkpointDirectory);
        }

        val inputStream = new KafkaInput().readFromKafka(ssc);
        val parsedStream = parseMessages(inputStream);
        val writeCounts: DStream[Integer] =
        
        new OpenTSDBOutput().putOpentsdb(
            props.getProperty("openssdb.ip"),
            parsedStream);

        val viewperlocation = writeCounts.group(location).reduce(_ + _);
        ssc;
    }: StreamingContext
}
