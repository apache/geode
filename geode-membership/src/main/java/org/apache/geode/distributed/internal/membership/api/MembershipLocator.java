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

package org.apache.geode.distributed.internal.membership.api;

import java.io.IOException;
import java.net.SocketAddress;

import org.apache.geode.distributed.internal.tcpserver.TcpHandler;

public interface MembershipLocator<ID extends MemberIdentifier> {

  int start() throws IOException;

  void stop();

  boolean isAlive();

  int getPort();

  boolean isShuttingDown();

  void waitToShutdown(long waitTime) throws InterruptedException;

  void waitToShutdown() throws InterruptedException;

  void restarting() throws IOException;

  SocketAddress getSocketAddress();

  void setMembership(Membership<ID> membership);

  void addHandler(Class<?> clazz, TcpHandler handler);

  boolean isHandled(Class<?> clazz);
}
