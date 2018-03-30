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
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.protobuf.statistics.ClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.Authorizer;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.AuthorizingCache;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.AuthorizingLocator;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.AuthorizingLocatorImpl;
import org.apache.geode.internal.protocol.protobuf.v1.state.ConnectionState;
import org.apache.geode.internal.protocol.protobuf.v1.state.TerminateConnection;

@Experimental
public class LocatorMessageExecutionContext extends MessageExecutionContext {
  private final Locator locator;
  private AuthorizingLocator authorizingLocator;

  public LocatorMessageExecutionContext(Locator locator, ClientStatistics statistics,
      ConnectionState initialConnectionState, Authorizer authorizer) {
    super(statistics, initialConnectionState);
    this.locator = locator;
    this.authorizingLocator = new AuthorizingLocatorImpl(locator, authorizer);
  }

  @Override
  public AuthorizingCache getAuthorizingCache() throws InvalidExecutionContextException {
    setConnectionStateProcessor(new TerminateConnection());
    throw new InvalidExecutionContextException(
        "Operations on the locator should not to try to operate on a server");
  }

  @Override
  public AuthorizingLocator getAuthorizingLocator() throws InvalidExecutionContextException {
    return authorizingLocator;
  }

  @Override
  public void setAuthorizer(Authorizer authorizer) {
    this.authorizingLocator = new AuthorizingLocatorImpl(locator, authorizer);
  }
}
