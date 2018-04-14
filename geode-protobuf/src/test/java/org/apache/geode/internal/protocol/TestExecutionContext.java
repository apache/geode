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

import static org.mockito.Mockito.mock;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.protocol.protobuf.statistics.NoOpStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.LocatorMessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ServerMessageExecutionContext;
import org.apache.geode.internal.security.SecurityService;

public class TestExecutionContext {
  public static MessageExecutionContext getNoAuthCacheExecutionContext(InternalCache cache) {
    return new ServerMessageExecutionContext(cache, new NoOpStatistics(),
        mock(SecurityService.class));
  }

  public static MessageExecutionContext getLocatorExecutionContext(InternalLocator locator) {
    return new LocatorMessageExecutionContext(locator, new NoOpStatistics(),
        mock(SecurityService.class));
  }
}
