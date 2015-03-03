/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alertlogic.alss

import scala.reflect.ClassTag

import org.json4s._
import org.json4s.jackson.JsonMethods._

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.PutRecordRequest

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kinesis.KinesisUtils

import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Estimate clusters on one stream of data and make predictions
 * on another stream, where the data streams arrive as data
 * into two different streams.
 *
 * Usage:
 *   KMeans <streamRegion> <trainingStream> <testStream> <numClusters>
 *
 * As you add data to `trainingStream` the clusters will continuously update.
 * Anytime you add data to `testStream`, you'll see predicted labels using the current model.
 *
 * Example:
 *    $ export AWS_ACCESS_KEY_ID=<your-access-key>
 *    $ export AWS_SECRET_KEY=<your-secret-key>
 *    $ $SPARK_HOME/bin/run-example \
 *        com.alertlogic.alss.KMeans \
 *        us-east-1 trainingStream testStream 5
 *
 * The access and secret keys will be picked up from EC2 metadata first if possible.
 *
 */
object KMeans {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: KMeans " +
          "<sparkMaster> <streamRegion> <trainingStream> <testStream> <numClusters>")
      System.exit(1)
    }

    val master = args(0)
    val streamRegion = args(1)
    val trainingStreamName = args(2)
    val testStreamName = args(3)
    val numClusters = args(4).toInt
    val endpointURL = "https://kinesis." + streamRegion + ".amazonaws.com"
    
    val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
    kinesisClient.setEndpoint(endpointURL)

    StreamingLogs.setLogLevels()

    val batchInterval = Milliseconds(2000)
    val conf = new SparkConf().setAppName("KMeans")
    val ssc = new StreamingContext(conf, batchInterval)

    val checkpointInterval = batchInterval

    val trainingStream = makeUnifiedStream(kinesisClient, endpointURL, ssc, checkpointInterval, trainingStreamName)
    val trainingData = trainingStream.flatMap(json => parse(json))

    val testStream = makeUnifiedStream(kinesisClient, endpointURL, ssc, checkpointInterval, testStreamName)
    val testData = testStream.flatMap(json => parse(json))

    val numDimensions = 1

    val model = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0)

    model.trainOn(trainingData)
    //model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
  }
  
  def makeUnifiedStream[T: ClassTag](kinesisClient: AmazonKinesisClient,
                                     endpointURL : String,
                                     ssc: StreamingContext,
                                     checkpointInterval: Duration,
                                     streamName: String) : DStream[T] = {
    val numShards =
        kinesisClient.describeStream(streamName).getStreamDescription().getShards().size()
    val streams = (0 until numShards).map
        {
            i => KinesisUtils.createStream(ssc,
                                           streamName,
                                           endpointURL,
                                           checkpointInterval,
                                           InitialPositionInStream.LATEST,
                                           StorageLevel.MEMORY_AND_DISK_2)
        }
    return ssc.union(trainingStreams)
  }
    
}

private object StreamingLogs extends Logging {

  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for KMeans." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
