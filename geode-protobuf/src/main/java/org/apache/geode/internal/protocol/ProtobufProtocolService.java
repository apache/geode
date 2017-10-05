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

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolPipeline;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolService;
import org.apache.geode.internal.protocol.protobuf.ProtobufStreamProcessor;
import org.apache.geode.internal.protocol.protobuf.statistics.NoOpStatistics;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatistics;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatisticsImpl;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.server.Authenticator;

public class ProtobufProtocolService implements ClientProtocolService {
  private volatile ProtobufClientStatistics statistics;
  private final ProtobufStreamProcessor protobufStreamProcessor = new ProtobufStreamProcessor();

  @Override
  public synchronized void initializeStatistics(String statisticsName, StatisticsFactory factory) {
    statistics = new ProtobufClientStatisticsImpl(factory, statisticsName,
        ProtobufClientStatistics.PROTOBUF_STATS_NAME);
  }

  /**
   * For internal use. This is necessary because the statistics may get initialized in another
   * thread.
   */
  private ProtobufClientStatistics getStatistics() {
    if (statistics == null) {
      return new NoOpStatistics();
    }
    return statistics;
  }

  @Override
  public ClientProtocolPipeline createCachePipeline(Cache cache, Authenticator authenticator,
      SecurityService securityService) {
    return new ProtobufCachePipeline(protobufStreamProcessor, getStatistics(), cache, authenticator,
        securityService);
  }

  @Override
  public ClientProtocolPipeline createLocatorPipeline(InternalLocator locator) {
    return new ProtobufLocatorPipeline(protobufStreamProcessor, getStatistics(), locator);
  }
}
