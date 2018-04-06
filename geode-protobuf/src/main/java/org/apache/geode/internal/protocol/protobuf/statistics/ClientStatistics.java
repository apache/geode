/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.protocol.protobuf.statistics;

public interface ClientStatistics {
  default String getStatsName() {
    return "ClientProtocolStats";
  }

  void clientConnected();

  void clientDisconnected();

  void messageReceived(int bytes);

  void messageSent(int bytes);

  void incAuthorizationViolations();

  void incAuthenticationFailures();

  /**
   * Invoke this at the start of an operation and then invoke endOperation in a finally block
   */
  long startOperation();

  /**
   * record the end of an operation. The parameter value should be from startOperation and
   * endOperation must be invoked in the same thread as startOperation because we're using
   * System.nanoTime()
   */
  void endOperation(long startOperationTime);
}
