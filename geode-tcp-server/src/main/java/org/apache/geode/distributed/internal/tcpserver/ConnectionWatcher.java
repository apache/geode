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

import java.net.Socket;

/**
 * ConnectionWatcher is used to observe tcp/ip connection formation in socket-creator
 * implementations.
 *
 * @see AdvancedSocketCreator#connect(HostAndPort, int, ConnectionWatcher, boolean, int, boolean)
 */
public interface ConnectionWatcher {
  /**
   * this is invoked with the connecting socket just prior to issuing a connect() call. It can be
   * used to start another thread or task to monitor the connection attempt.
   */
  void beforeConnect(Socket socket);

  /**
   * this is invoked after the connection attempt has finished. It can be used to cancel the task
   * started by beforeConnect
   */
  void afterConnect(Socket socket);
}
