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

package org.apache.geode.internal.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.geode.internal.cache.tier.sockets.GenericProtocolServerConnection;
import org.apache.geode.internal.cache.tier.sockets.ServerConnectionFactory;


/**
 * This is an interface that other modules can implement to hook into
 * {@link GenericProtocolServerConnection} to handle messages sent to Geode.
 *
 * Currently, only one {@link ClientProtocolMessageHandler} at a time can be used in a Geode
 * instance. It gets wired into {@link ServerConnectionFactory} to create all instances of
 * {@link GenericProtocolServerConnection}.
 *
 * Implementors of this interface are expected to be able to be used for any number of connections
 * at a time (stateless except for the statistics).
 */
public interface ClientProtocolMessageHandler {
  void receiveMessage(InputStream inputStream, OutputStream outputStream,
      MessageExecutionContext executionContext) throws IOException;
}
