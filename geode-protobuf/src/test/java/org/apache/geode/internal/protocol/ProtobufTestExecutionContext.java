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

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.protocol.security.NoOpAuthenticator;
import org.apache.geode.internal.protocol.security.NoOpAuthorizer;
import org.apache.geode.internal.protocol.security.processors.NoAuthenticationSecurityProcessor;
import org.apache.geode.internal.protocol.statistics.NoOpStatistics;

public class ProtobufTestExecutionContext {
  public static MessageExecutionContext getNoAuthCacheExecutionContext(Cache cache) {
    return new MessageExecutionContext(cache, new NoOpAuthenticator(), new NoOpAuthorizer(),
        new Object(), new NoOpStatistics(), new NoAuthenticationSecurityProcessor());
  }

  public static MessageExecutionContext getLocatorExecutionContext(InternalLocator locator) {
    return new MessageExecutionContext(locator, new NoOpStatistics());
  }
}
