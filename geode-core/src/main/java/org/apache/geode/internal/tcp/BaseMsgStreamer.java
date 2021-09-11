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

package org.apache.geode.internal.tcp;

import java.io.IOException;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Base interface for {@link MsgStreamer} and {@link MsgStreamerList} to send a message over a list
 * of connections to one or more peers.
 *
 * @since GemFire 7.1
 */
public interface BaseMsgStreamer {

  /**
   * set connections to be "in use" and schedule alert tasks
   */
  void reserveConnections(long startTime, long ackTimeout, long ackSDTimeout);

  /**
   * Returns a list of the Connections that the message was sent to. Call this after
   * {@link #writeMessage}.
   */
  @NotNull
  List<@NotNull Connection> getSentConnections();

  /**
   * Returns an exception the describes which cons the message was not sent to. Call this after
   * {@link #writeMessage}.
   */
  @Nullable
  ConnectExceptions getConnectExceptions();

  /**
   * Writes the message to the connected streams and returns the number of bytes written.
   *
   * @throws IOException if serialization failure
   */
  int writeMessage() throws IOException;

  /**
   * Close this streamer.
   *
   * @throws IOException on exception
   */
  void close() throws IOException;
}
