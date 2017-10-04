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

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.server.Authenticator;

/**
 * Provides a convenient location for a client protocol service to be loaded into the system.
 */
public interface ClientProtocolService {
  void initializeStatistics(String statisticsName, StatisticsFactory factory);

  ClientProtocolStatistics getStatistics();

  /**
   *
   * The pipeline MUST use an available authenticator for authentication of all operations once the
   * handshake has happened.
   *
   * @param availableAuthenticators A list of valid authenticators for the current system.
   */
  ClientProtocolPipeline createCachePipeline(Cache cache, Authenticator availableAuthenticators,
      SecurityService securityService);

  /**
   * In this case, the locator calls this to serve a single message at a time. The locator closes
   * the socket after this is done.
   *
   * Statistics saved on the service are used if initialized.
   */
  void serveLocatorMessage(InputStream inputStream, OutputStream outputStream,
      InternalLocator locator) throws IOException;
}
