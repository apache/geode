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
package org.apache.geode.cache.server;


/**
 * Metrics about the resource usage for a cache server.
 * These metrics are provided to the {@link ServerLoadProbe} for
 * use in calculating the load on the server.
 * @since GemFire 5.7
 *
 */
public interface ServerMetrics {
  /**
   * Get the number of open connections
   * for this cache server.
   */
  int getConnectionCount();
  
  /** Get the number of clients connected to this
   * cache server.
   */ 
  int getClientCount();
  
  /**
   * Get the number of client subscription connections hosted on this
   * cache server.
   */
  int getSubscriptionConnectionCount();
  
  /**
   * Get the max connections for this cache server.
   */
  int getMaxConnections();
  
}
