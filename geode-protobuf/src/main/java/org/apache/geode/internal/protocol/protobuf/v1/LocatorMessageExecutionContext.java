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

import java.util.Properties;

import org.apache.shiro.subject.Subject;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.protobuf.statistics.ClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.Authorizer;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.AuthorizingCache;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.AuthorizingLocator;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.AuthorizingLocatorImpl;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.NoSecurityAuthorizer;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.NotLoggedInAuthorizer;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.ShiroAuthorizer;
import org.apache.geode.internal.protocol.protobuf.v1.state.TerminateConnection;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.protocol.serialization.ValueSerializer;

@Experimental
public class LocatorMessageExecutionContext extends MessageExecutionContext {
  private final Locator locator;
  private AuthorizingLocator authorizingLocator;

  public LocatorMessageExecutionContext(Locator locator, ClientStatistics statistics,
      SecurityService securityService) {
    super(statistics, securityService);
    this.locator = locator;
    Authorizer authorizer = securityService.isIntegratedSecurity() ? new NotLoggedInAuthorizer()
        : new NoSecurityAuthorizer();
    this.authorizingLocator = new AuthorizingLocatorImpl(locator, authorizer);
  }

  @Override
  public AuthorizingCache getAuthorizingCache() throws InvalidExecutionContextException {
    setState(new TerminateConnection());
    throw new InvalidExecutionContextException(
        "Operations on the locator should not to try to operate on a server");
  }

  @Override
  public AuthorizingLocator getAuthorizingLocator() throws InvalidExecutionContextException {
    return authorizingLocator;
  }

  @Override
  public void authenticate(Properties properties) {
    Subject subject = securityService.login(properties);
    this.authorizingLocator =
        new AuthorizingLocatorImpl(locator, new ShiroAuthorizer(securityService, subject));
  }

  @Override
  public void setValueSerializer(ValueSerializer valueSerializer) {
    // Do nothing, locator messages don't need a value serializer
  }
}
