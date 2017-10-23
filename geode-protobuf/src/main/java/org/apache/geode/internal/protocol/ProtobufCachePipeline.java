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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.IncompatibleVersionException;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolProcessor;
import org.apache.geode.internal.cache.tier.sockets.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.ProtobufStreamProcessor;
import org.apache.geode.internal.protocol.protobuf.security.Authenticator;
import org.apache.geode.internal.protocol.protobuf.security.Authorizer;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatistics;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationFailedException;


@Experimental
public final class ProtobufCachePipeline implements ClientProtocolProcessor {
  private final ProtobufClientStatistics statistics;
  private final Cache cache;
  private final Authorizer authorizer;
  private final SecurityService securityService;
  private final ProtobufStreamProcessor streamProcessor;
  private final Authenticator authenticator;

  private Object authenticatorToken;

  ProtobufCachePipeline(ProtobufStreamProcessor protobufStreamProcessor,
      ProtobufClientStatistics statistics, Cache cache, Authenticator authenticator,
      Authorizer authorizer, SecurityService securityService) {
    this.streamProcessor = protobufStreamProcessor;
    this.statistics = statistics;
    this.cache = cache;
    this.authenticator = authenticator;
    this.authorizer = authorizer;
    this.securityService = securityService;
    this.statistics.clientConnected();
  }

  @Override
  public void processMessage(InputStream inputStream, OutputStream outputStream)
      throws IOException, IncompatibleVersionException {
    if (authenticatorToken == null) {
      try {
        authenticatorToken = authenticator.authenticate(inputStream, outputStream, securityService);
      } catch (AuthenticationFailedException ex) {
        statistics.incAuthenticationFailures();
        throw new IOException(ex);
      }
    } else {
      streamProcessor.receiveMessage(inputStream, outputStream,
          new MessageExecutionContext(cache, authorizer, authenticatorToken, statistics));
    }
  }

  @Override
  public void close() {
    this.statistics.clientDisconnected();
  }
}
