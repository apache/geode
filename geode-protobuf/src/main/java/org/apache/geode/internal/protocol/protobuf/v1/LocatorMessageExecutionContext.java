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
import org.apache.geode.internal.protocol.protobuf.security.NoSecurity;
import org.apache.geode.internal.protocol.protobuf.security.NotLoggedInSecurity;
import org.apache.geode.internal.protocol.protobuf.security.SecureCache;
import org.apache.geode.internal.protocol.protobuf.security.SecureLocator;
import org.apache.geode.internal.protocol.protobuf.security.SecureLocatorImpl;
import org.apache.geode.internal.protocol.protobuf.security.Security;
import org.apache.geode.internal.protocol.protobuf.security.ShiroSecurity;
import org.apache.geode.internal.protocol.protobuf.statistics.ClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.state.TerminateConnection;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.protocol.serialization.ValueSerializer;

@Experimental
public class LocatorMessageExecutionContext extends MessageExecutionContext {
  private final Locator locator;
  private SecureLocator secureLocator;

  public LocatorMessageExecutionContext(Locator locator, ClientStatistics statistics,
      SecurityService securityService) {
    super(statistics, securityService);
    this.locator = locator;
    Security security =
        securityService.isIntegratedSecurity() ? new NotLoggedInSecurity() : new NoSecurity();
    this.secureLocator = new SecureLocatorImpl(locator, security);
  }

  @Override
  public SecureCache getSecureCache() throws InvalidExecutionContextException {
    setState(new TerminateConnection());
    throw new InvalidExecutionContextException(
        "Operations on the locator should not to try to operate on a server");
  }

  @Override
  public SecureLocator getSecureLocator() throws InvalidExecutionContextException {
    return secureLocator;
  }

  @Override
  public void authenticate(Properties properties) {
    Subject subject = securityService.login(properties);
    this.secureLocator =
        new SecureLocatorImpl(locator, new ShiroSecurity(securityService, subject));
  }

  @Override
  public void setValueSerializer(ValueSerializer valueSerializer) {
    // Do nothing, locator messages don't need a value serializer
  }
}
