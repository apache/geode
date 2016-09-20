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
package org.apache.geode.spark.connector

/**
 * GeodeConnectionFactory provide an common interface that manages Geode
 * connections, and it's serializable. Each factory instance will handle
 * connection instance creation and connection pool management.
 */
trait GeodeConnectionManager extends Serializable {

  /** get connection for the given connector */
  def getConnection(connConf: GeodeConnectionConf): GeodeConnection

  /** close the connection */
  def closeConnection(connConf: GeodeConnectionConf): Unit
}
