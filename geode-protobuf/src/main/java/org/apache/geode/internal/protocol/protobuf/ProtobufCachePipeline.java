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

package org.apache.geode.internal.protocol.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolProcessor;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.security.Authenticator;
import org.apache.geode.internal.protocol.security.Authorizer;
import org.apache.geode.internal.protocol.security.SecurityProcessor;
import org.apache.geode.internal.protocol.statistics.ProtocolClientStatistics;


@Experimental
public final class ProtobufCachePipeline implements ClientProtocolProcessor {
  private final ProtocolClientStatistics statistics;
  private final ProtobufStreamProcessor streamProcessor;

  private final MessageExecutionContext messageExecutionContext;

  ProtobufCachePipeline(ProtobufStreamProcessor protobufStreamProcessor,
      ProtocolClientStatistics statistics, Cache cache, Authenticator authenticator,
      Authorizer authorizer, SecurityProcessor securityProcessor) {
    this.streamProcessor = protobufStreamProcessor;
    this.statistics = statistics;
    this.statistics.clientConnected();
    this.messageExecutionContext = new MessageExecutionContext(cache, authenticator, authorizer,
        null, statistics, securityProcessor);
  }

  @Override
  public void processMessage(InputStream inputStream, OutputStream outputStream)
      throws IOException {
    streamProcessor.receiveMessage(inputStream, outputStream, messageExecutionContext);
  }

  @Override
  public void close() {
    this.statistics.clientDisconnected();
  }
}
