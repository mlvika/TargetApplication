package com.target.usecase

import scala.util.control.NonFatal
import java.io.StringWriter
import java.io.PrintWriter
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import org.apache.log4j.Logger
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient

class StatReporter(appName: String, metricsUrl: String) extends StreamingListener {

    private[this] val logger = Logger.getLogger(getClass().getName())

    override def onBatchCompleted(batch: StreamingListenerBatchCompleted) = {
        def doSend(metricName: String, metricValue: String) = {
            try {
                val httpClient = new DefaultHttpClient()
                val post = new HttpPost(metricsUrl)
                post.setHeader("Content-type", "application/json")
                val ts = java.lang.System.currentTimeMillis()
                val body = f"""{
                    |    "data": [{
                    |        "source": "application.$appName",
                    |        "metric": "application.kpi.$appName.$metricName",
                    |        "value": "$metricValue",
                    |        "timestamp": $ts%d
                    |    }],
                    |    "timestamp": $ts%d
                    |}""".stripMargin

                logger.debug(body)
                post.setEntity(new StringEntity(body))
                val response = httpClient.execute(post)
                if (response.getStatusLine.getStatusCode() != 200) {
                    logger.error("POST failed: " + metricsUrl + " response:" + response.getStatusLine.getStatusCode())
                }

            } catch {
                case NonFatal(t) => {
                    logger.error("POST failed: " + metricsUrl)
                    val sw = new StringWriter
                    t.printStackTrace(new PrintWriter(sw))
                    logger.error(sw.toString)
                }
            }
        }
        doSend("processing-delay", batch.batchInfo.processingDelay.get.toString())
        doSend("scheduling-delay", batch.batchInfo.schedulingDelay.get.toString())
    }
}
