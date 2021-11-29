/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkWordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Crypto data processor"))
    val threshold = args(1).toInt

    // Input trade data
    val rddFromFile = sc.textFile("projects/spark/input/BNBUSDT-trades-2019-01.csv")
    val rdd = rddFromFile.map(f=>{
        f.split(",")
    })


    // Process UNIX milisecond-timestamps to human-readable dates
    val df1 = df.withColumn("_c4", date_format(to_utc_timestamp(from_unixtime(col("_c4")/1000), "EST"), "MM-dd-yyyy")).withColumn("_c2", $"_c2".cast(FloatType)).withColumn("_c3", $"_c3".cast(FloatType))

    val totalQuantityDf = df1.groupBy("_c4").sum("_c2").withColumnRenamed("sum(_c2)", "totalQuantityPerDay")
    val totalExchangeDf = df1.groupBy("_c4").sum("_c3").withColumnRenamed("sum(_c3)", "totalExchangePerDay").withColumnRenamed("_c4", "date")

    // Output final data
    val finalDf = totalQuantityDf.join(totalExchangeDf, $"_c4" === $"date").withColumn("pricePerDay", $"totalExchangePerDay" / $"totalQuantityPerDay").drop("_c4")
    
   // Write DataFrame data to CSV file
   finalDf.write.csv("projects/spark/output/BNBUSDT-trades-2019-01.csv") 
  }
}
