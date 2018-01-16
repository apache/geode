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

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolProcessor;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolService;
import org.apache.geode.internal.protocol.LocatorMessageExecutionContext;
import org.apache.geode.internal.protocol.ServerMessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.ProtocolVersion;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatisticsImpl;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionHandshakeStateProcessor;
import org.apache.geode.internal.protocol.statistics.NoOpStatistics;
import org.apache.geode.internal.protocol.statistics.ProtocolClientStatistics;
import org.apache.geode.internal.security.SecurityService;

public class ProtobufProtocolService implements ClientProtocolService {
  private volatile ProtocolClientStatistics statistics;
  private final ProtobufStreamProcessor protobufStreamProcessor = new ProtobufStreamProcessor();

  @Override
  public synchronized void initializeStatistics(String statisticsName, StatisticsFactory factory) {
    if (statistics == null) {
      statistics = new ProtobufClientStatisticsImpl(factory, statisticsName);
    }
  }

  @Override
  public ClientProtocolProcessor createProcessorForCache(Cache cache,
      SecurityService securityService) {
    assert (statistics != null);

    ProtobufConnectionHandshakeStateProcessor connectionStateProcessor =
        new ProtobufConnectionHandshakeStateProcessor(securityService);
    return new ProtobufCachePipeline(protobufStreamProcessor,
        new ServerMessageExecutionContext(cache, statistics, connectionStateProcessor));
  }

  /**
   * For internal use. This is necessary because the statistics may get initialized in another
   * thread.
   */
  ProtocolClientStatistics getStatistics() {
    if (statistics == null) {
      return new NoOpStatistics();
    }
    return statistics;
  }

  @Override
  public ClientProtocolProcessor createProcessorForLocator(InternalLocator locator,
      SecurityService securityService) {
    ProtobufConnectionHandshakeStateProcessor connectionStateProcessor =
        new ProtobufConnectionHandshakeStateProcessor(securityService);
    return new ProtobufCachePipeline(protobufStreamProcessor,
        new LocatorMessageExecutionContext(locator, statistics, connectionStateProcessor));
  }

  @Override
  public int getServiceProtocolVersion() {
    return ProtocolVersion.MajorVersions.CURRENT_MAJOR_VERSION_VALUE;
  }
}
