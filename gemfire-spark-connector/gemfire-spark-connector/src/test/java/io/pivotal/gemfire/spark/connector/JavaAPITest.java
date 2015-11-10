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
package io.pivotal.gemfire.spark.connector;

import io.pivotal.gemfire.spark.connector.javaapi.*;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.DStream;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.mutable.LinkedList;
import scala.reflect.ClassTag;

import static org.junit.Assert.*;
import static io.pivotal.gemfire.spark.connector.javaapi.GemFireJavaUtil.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class JavaAPITest extends JUnitSuite {

  @SuppressWarnings( "unchecked" )
  public Tuple3<SparkContext, GemFireConnectionConf, GemFireConnection> createCommonMocks() {
    SparkContext mockSparkContext = mock(SparkContext.class);
    GemFireConnectionConf mockConnConf = mock(GemFireConnectionConf.class);
    GemFireConnection mockConnection = mock(GemFireConnection.class);
    when(mockConnConf.getConnection()).thenReturn(mockConnection);
    when(mockConnConf.locators()).thenReturn(new LinkedList());
    return new Tuple3<>(mockSparkContext, mockConnConf, mockConnection);
  }
  
  @Test
  public void testSparkContextFunction() throws Exception {
    Tuple3<SparkContext, GemFireConnectionConf, GemFireConnection> tuple3 = createCommonMocks();
    GemFireJavaSparkContextFunctions wrapper = javaFunctions(tuple3._1());
    assertTrue(tuple3._1() == wrapper.sc);
    String regionPath = "testregion";
    JavaPairRDD<String, String> rdd = wrapper.gemfireRegion(regionPath, tuple3._2());
    verify(tuple3._3()).validateRegion(regionPath);
  }

  @Test
  public void testJavaSparkContextFunctions() throws Exception {
    SparkContext mockSparkContext = mock(SparkContext.class);
    JavaSparkContext mockJavaSparkContext = mock(JavaSparkContext.class);
    when(mockJavaSparkContext.sc()).thenReturn(mockSparkContext);
    GemFireJavaSparkContextFunctions wrapper = javaFunctions(mockJavaSparkContext);
    assertTrue(mockSparkContext == wrapper.sc);
  }
  
  @Test
  @SuppressWarnings( "unchecked" )
  public void testJavaPairRDDFunctions() throws Exception {
    JavaPairRDD<String, Integer> mockPairRDD = mock(JavaPairRDD.class);
    RDD<Tuple2<String, Integer>> mockTuple2RDD = mock(RDD.class);
    when(mockPairRDD.rdd()).thenReturn(mockTuple2RDD);
    GemFireJavaPairRDDFunctions wrapper = javaFunctions(mockPairRDD);
    assertTrue(mockTuple2RDD == wrapper.rddf.rdd());

    Tuple3<SparkContext, GemFireConnectionConf, GemFireConnection> tuple3 = createCommonMocks();
    when(mockTuple2RDD.sparkContext()).thenReturn(tuple3._1());
    String regionPath = "testregion";
    wrapper.saveToGemfire(regionPath, tuple3._2());
    verify(mockTuple2RDD, times(1)).sparkContext();
    verify(tuple3._1(), times(1)).runJob(eq(mockTuple2RDD), any(Function2.class), any(ClassTag.class));
  }

  @Test
  @SuppressWarnings( "unchecked" )
  public void testJavaRDDFunctions() throws Exception {
    JavaRDD<String> mockJavaRDD = mock(JavaRDD.class);
    RDD<String> mockRDD = mock(RDD.class);
    when(mockJavaRDD.rdd()).thenReturn(mockRDD);
    GemFireJavaRDDFunctions wrapper = javaFunctions(mockJavaRDD);
    assertTrue(mockRDD == wrapper.rddf.rdd());

    Tuple3<SparkContext, GemFireConnectionConf, GemFireConnection> tuple3 = createCommonMocks();
    when(mockRDD.sparkContext()).thenReturn(tuple3._1());
    PairFunction<String, String, Integer> mockPairFunc = mock(PairFunction.class);
    String regionPath = "testregion";
    wrapper.saveToGemfire(regionPath, mockPairFunc, tuple3._2());
    verify(mockRDD, times(1)).sparkContext();
    verify(tuple3._1(), times(1)).runJob(eq(mockRDD), any(Function2.class), any(ClassTag.class));
  }

  @Test
  @SuppressWarnings( "unchecked" )
  public void testJavaPairDStreamFunctions() throws Exception {
    JavaPairDStream<String, String> mockJavaDStream = mock(JavaPairDStream.class);
    DStream<Tuple2<String, String>> mockDStream = mock(DStream.class);
    when(mockJavaDStream.dstream()).thenReturn(mockDStream);
    GemFireJavaPairDStreamFunctions wrapper = javaFunctions(mockJavaDStream);
    assertTrue(mockDStream == wrapper.dsf.dstream());

    Tuple3<SparkContext, GemFireConnectionConf, GemFireConnection> tuple3 = createCommonMocks();
    String regionPath = "testregion";
    wrapper.saveToGemfire(regionPath, tuple3._2());
    verify(tuple3._2()).getConnection();
    verify(tuple3._3()).validateRegion(regionPath);
    verify(mockDStream).foreachRDD(any(Function1.class));
  }

  @Test
  @SuppressWarnings( "unchecked" )
  public void testJavaPairDStreamFunctionsWithTuple2DStream() throws Exception {
    JavaDStream<Tuple2<String, String>> mockJavaDStream = mock(JavaDStream.class);
    DStream<Tuple2<String, String>> mockDStream = mock(DStream.class);
    when(mockJavaDStream.dstream()).thenReturn(mockDStream);
    GemFireJavaPairDStreamFunctions wrapper = javaFunctions(toJavaPairDStream(mockJavaDStream));
    assertTrue(mockDStream == wrapper.dsf.dstream());
  }

  @Test
  @SuppressWarnings( "unchecked" )
  public void testJavaDStreamFunctions() throws Exception {
    JavaDStream<String> mockJavaDStream = mock(JavaDStream.class);
    DStream<String> mockDStream = mock(DStream.class);
    when(mockJavaDStream.dstream()).thenReturn(mockDStream);
    GemFireJavaDStreamFunctions wrapper = javaFunctions(mockJavaDStream);
    assertTrue(mockDStream == wrapper.dsf.dstream());

    Tuple3<SparkContext, GemFireConnectionConf, GemFireConnection> tuple3 = createCommonMocks();
    PairFunction<String, String, Integer> mockPairFunc = mock(PairFunction.class);
    String regionPath = "testregion";
    wrapper.saveToGemfire(regionPath, mockPairFunc, tuple3._2());
    verify(tuple3._2()).getConnection();
    verify(tuple3._3()).validateRegion(regionPath);
    verify(mockDStream).foreachRDD(any(Function1.class));
  }

  @Test
  public void testSQLContextFunction() throws Exception {
    SQLContext mockSQLContext = mock(SQLContext.class);
    GemFireJavaSQLContextFunctions wrapper = javaFunctions(mockSQLContext);
    assertTrue(wrapper.scf.getClass() == GemFireSQLContextFunctions.class);
  }

}
