package com.target.usecase;

import com.cisco.pnda._
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Logger;

object Consumer {
  
  private[this] val logger = Logger.getLogger(getClass().getName());

  def main(args: Array[String]) {

    val props = AppConfig.loadProperties();
    val loggerUrl = props.getProperty("environment.metric_logger_url")
    val appName = props.getProperty("component.application")
    val checkpointDirectory = props.getProperty("app.checkpoint_path");
    val batchSizeSeconds = Integer.parseInt(props.getProperty("app.batch_size_seconds"));

    val pipeline = new KafkaPipeline()
    // Create the streaming context, or load a saved one from disk
    val ssc = if (checkpointDirectory.length() > 0) StreamingContext.getOrCreate(checkpointDirectory, pipeline.create) else pipeline.create();

    sys.ShutdownHookThread {
        logger.info("Gracefully stopping Spark Streaming Application")
        ssc.stop(true, true)
        logger.info("Application stopped")
    }

    logger.info("Starting spark streaming execution")
    logger.info("Logger url: " + loggerUrl)
    ssc.addStreamingListener(new StatReporter(appName, loggerUrl))
    ssc.start()
    ssc.awaitTermination()
  }
}

