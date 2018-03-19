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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.protobuf.statistics.ClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.Authorizer;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.AuthorizingCache;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.AuthorizingCacheImpl;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.AuthorizingLocator;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionStateProcessor;

@Experimental
public class ServerMessageExecutionContext extends MessageExecutionContext {
  private final InternalCache cache;
  private AuthorizingCache authorizingCache;

  public ServerMessageExecutionContext(InternalCache cache, ClientStatistics statistics,
      ProtobufConnectionStateProcessor initialConnectionStateProcessor, Authorizer authorizer) {
    super(statistics, initialConnectionStateProcessor);
    this.cache = cache;
    this.authorizingCache = new AuthorizingCacheImpl(cache, authorizer);
  }

  @Override
  public AuthorizingCache getAuthorizingCache() {
    return this.authorizingCache;
  }

  @Override
  public AuthorizingLocator getAuthorizingLocator() throws InvalidExecutionContextException {
    throw new InvalidExecutionContextException(
        "Operations on the server should not to try to operate on a locator");
  }

  @Override
  public void setAuthorizor(Authorizer authorizer) {
    this.authorizingCache = new AuthorizingCacheImpl(cache, authorizer);
  }
}
