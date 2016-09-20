/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import static org.apache.geode.spark.connector.javaapi.GeodeJavaUtil.*;


/**
 * This Spark application demonstrates how to get region data from Geode using Geode
 * OQL Java API. The result is a Spark DataFrame.
 * <p>
 * In order to run it, you will need to start a Geode cluster, and run demo PairRDDSaveJavaDemo
 * first to create some data in the region.
 * <p>
 * Once you compile and package the demo, the jar file basic-demos_2.10-0.5.0.jar
 * should be generated under geode-spark-demos/basic-demos/target/scala-2.10/.
 * Then run the following command to start a Spark job:
 * <pre>
 *   <path to spark>/bin/spark-submit --master=local[2] --class demo.OQLJavaDemo \
 *       <path to>/basic-demos_2.10-0.5.0.jar <locator host>:<port>
 * </pre>
 */
public class OQLJavaDemo {

  public static void main(String[] argv) {

    if (argv.length != 1) {
      System.err.printf("Usage: OQLJavaDemo <locators>\n");
      return;
    }

    SparkConf conf = new SparkConf().setAppName("OQLJavaDemo");
    conf.set(GeodeLocatorPropKey, argv[0]); // "192.168.1.47[10335]"
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
    DataFrame df = javaFunctions(sqlContext).geodeOQL("select * from /str_str_region");
    System.out.println("======= DataFrame =======\n");
    df.show();
    sc.stop();
  }
}
