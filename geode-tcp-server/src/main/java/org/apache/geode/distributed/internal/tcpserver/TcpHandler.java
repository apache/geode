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
package org.apache.geode.distributed.internal.tcpserver;

import java.io.IOException;

/**
 * A TcpHandler is the handler of messages not directly processed by TcpServer itself.
 * The messages handled by TcpServer are those in this package.
 */
public interface TcpHandler {
  /**
   * Process a request and return a response
   *
   * @return the response, or null if there is no reponse
   */
  Object processRequest(Object request) throws IOException;

  void endRequest(Object request, long startTime);

  void endResponse(Object request, long startTime);

  /**
   * Perform any shutdown code in the handler after the TCP server has closed the socket.
   */
  void shutDown();

  /**
   * Initialize the handler with the TcpServer. Called before the TcpServer starts accepting
   * connections.
   */
  void init(TcpServer tcpServer);
}
