package com.target.usecase;

import org.joda.time.DateTime
import scala.util.control.NonFatal
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

class OpenTSDBOutput extends Serializable {


  def putOpentsdb[T](opentsdbIP: String,
      stream: DStream[String]) = {
    stream.mapPartitions(partition => {
      var count = 0;
      partition.foreach(rowData =>
        {

          val json = parse(rowData.replace("'", "\""))
          val host = compact(render((json \\ "host"))).replace("\"", "")
          val timestampStr = compact(render((json \\ "timestamp"))).replace("\"", "")
          val location = compact(render((json \\ "timestamp"))).replace("\"", "")
          val viewership = compact(render((json \\ "timestamp"))).replace("\"", "")
          val collectd_type = compact(render((json \\ "collectd_type"))).replace("\"", "")
          var metric:String = "trgt.viewership"
          metric = metric.concat("." + collectd_type)
          val timestamp = new DateTime(timestampStr).getMillis
          val body = f"""{
                    |        "metric": "$metric",
                    |        "location": "$location",
                    |        "viewership": "$viewership",
                    |        "timestamp": $timestamp,
                    |        "tags": {"host": "$host"}
                    |}""".stripMargin

          var openTSDBUrl = "http://" + opentsdbIP + "/api/put"
          try {
                val httpClient = new DefaultHttpClient()
                val post = new HttpPost(openTSDBUrl)
                post.setHeader("Content-type", "application/json")
                post.setEntity(new StringEntity(body))
                httpClient.execute(post)

            } catch {
                case NonFatal(t) => {
                    
                }
            }

          count += 1
        });
      Iterator[Integer](count)
    });
  }
}