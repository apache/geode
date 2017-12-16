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

package org.apache.geode.internal.protocol.protobuf.v1;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolProcessor;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionHandshakeStateProcessor;
import org.apache.geode.internal.protocol.state.ConnectionStateProcessor;
import org.apache.geode.internal.protocol.statistics.ProtocolClientStatistics;
import org.apache.geode.internal.security.SecurityService;


@Experimental
public final class ProtobufCachePipeline implements ClientProtocolProcessor {
  private final ProtocolClientStatistics statistics;
  private final ProtobufStreamProcessor streamProcessor;
  private final ConnectionStateProcessor initialCacheConnectionStateProcessor;
  private final MessageExecutionContext messageExecutionContext;

  ProtobufCachePipeline(ProtobufStreamProcessor protobufStreamProcessor,
      ProtocolClientStatistics statistics, Cache cache, SecurityService securityService) {
    this.streamProcessor = protobufStreamProcessor;
    this.statistics = statistics;
    this.statistics.clientConnected();
    this.initialCacheConnectionStateProcessor =
        new ProtobufConnectionHandshakeStateProcessor(securityService);
    this.messageExecutionContext =
        new MessageExecutionContext(cache, statistics, initialCacheConnectionStateProcessor);
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

  @Override
  public boolean socketProcessingIsFinished() {
    return messageExecutionContext.getConnectionStateProcessor().socketProcessingIsFinished();
  }
}
