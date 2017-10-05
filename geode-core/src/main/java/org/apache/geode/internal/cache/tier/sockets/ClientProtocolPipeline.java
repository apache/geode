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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.geode.cache.IncompatibleVersionException;

/**
 * An interface representing the message handling part of a protocol. This may contain multiple
 * parts, and is expected to take care of handshaking, auth, whatever is required. It does not
 * manage the socket, and apart from the statistics, is expected to be effectively stateless.
 */
public interface ClientProtocolPipeline extends AutoCloseable {
  void processMessage(InputStream inputStream, OutputStream outputStream)
      throws IOException, IncompatibleVersionException;

  /**
   * Close the pipeline, incrementing stats and releasing any resources.
   *
   * This declaration narrows the exception type to be IOException.
   */
  @Override
  void close();
}
