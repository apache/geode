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
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.protobuf.security.NoSecurity;
import org.apache.geode.internal.protocol.protobuf.security.NotLoggedInSecurity;
import org.apache.geode.internal.protocol.protobuf.security.SecureCache;
import org.apache.geode.internal.protocol.protobuf.security.SecureCacheImpl;
import org.apache.geode.internal.protocol.protobuf.security.SecureLocator;
import org.apache.geode.internal.protocol.protobuf.security.Security;
import org.apache.geode.internal.protocol.protobuf.security.ShiroSecurity;
import org.apache.geode.internal.protocol.protobuf.statistics.ClientStatistics;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.protocol.serialization.ValueSerializer;

@Experimental
public class ServerMessageExecutionContext extends MessageExecutionContext {
  private final InternalCache cache;
  private SecureCache secureCache;

  public ServerMessageExecutionContext(InternalCache cache, ClientStatistics statistics,
      SecurityService securityService) {
    super(statistics, securityService);
    this.cache = cache.getCacheForProcessingClientRequests();
    Security security =
        securityService.isIntegratedSecurity() ? new NotLoggedInSecurity() : new NoSecurity();
    this.secureCache = new SecureCacheImpl(this.cache, security);
  }

  @Override
  public SecureCache getSecureCache() {
    return this.secureCache;
  }

  @Override
  public SecureLocator getSecureLocator() throws InvalidExecutionContextException {
    throw new InvalidExecutionContextException(
        "Operations on the server should not to try to operate on a locator");
  }

  @Override
  public void authenticate(Properties properties) {
    Subject subject = securityService.login(properties);
    this.secureCache = new SecureCacheImpl(cache, new ShiroSecurity(securityService, subject));
  }

  @Override
  public void setValueSerializer(ValueSerializer valueSerializer) {
    valueSerializer.init(cache);
    this.serializationService = new ProtobufSerializationService(valueSerializer);
  }
}
