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
package io.pivotal.gemfire.spark.connector.javaapi;

import io.pivotal.gemfire.spark.connector.GemFireConnectionConf;
import io.pivotal.gemfire.spark.connector.GemFireSQLContextFunctions;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Java API wrapper over {@link org.apache.spark.sql.SQLContext} to provide GemFire
 * OQL functionality.
 *
 * <p></p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * io.pivotal.gemfire.spark.connector.javaapi.GemFireJavaUtil} class.</p>
 */
public class GemFireJavaSQLContextFunctions {

  public final GemFireSQLContextFunctions scf;

  public GemFireJavaSQLContextFunctions(SQLContext sqlContext) {
    scf = new GemFireSQLContextFunctions(sqlContext);
  }

  public <T> DataFrame gemfireOQL(String query) {
    DataFrame df = scf.gemfireOQL(query, scf.defaultConnectionConf());
    return df;
  }

  public <T> DataFrame gemfireOQL(String query, GemFireConnectionConf connConf) {
    DataFrame df = scf.gemfireOQL(query, connConf);
    return df;
  }

}
