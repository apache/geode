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
package org.apache.geode.internal.protocol.protobuf.security;

import org.apache.geode.internal.protocol.protobuf.security.processors.AuthenticationSecurityProcessor;
import org.apache.geode.internal.protocol.security.Authenticator;
import org.apache.geode.internal.protocol.security.Authorizer;
import org.apache.geode.internal.protocol.security.InvalidConfigAuthenticator;
import org.apache.geode.internal.protocol.security.NoOpAuthenticator;
import org.apache.geode.internal.protocol.security.NoOpAuthorizer;
import org.apache.geode.internal.protocol.security.SecurityProcessor;
import org.apache.geode.internal.protocol.security.processors.NoAuthenticationSecurityProcessor;
import org.apache.geode.internal.security.SecurityService;

public class ProtobufSecurityLookupService {
  private final Authenticator[] authenticators = new Authenticator[3];
  private final Authorizer[] authorizers = new Authorizer[2];
  private final SecurityProcessor[] securityProcessors = new SecurityProcessor[2];

  public ProtobufSecurityLookupService() {
    initializeAuthenticators();
    initializeAuthortizers();
    initializeSecurityProcessors();
  }

  private void initializeSecurityProcessors() {
    securityProcessors[0] = new NoAuthenticationSecurityProcessor();
    securityProcessors[1] = new AuthenticationSecurityProcessor();
  }

  private void initializeAuthenticators() {
    authenticators[0] = new NoOpAuthenticator();
    authenticators[1] = new InvalidConfigAuthenticator();
  }

  private void initializeAuthortizers() {
    authorizers[0] = new NoOpAuthorizer();
  }

  public SecurityProcessor lookupProcessor(SecurityService securityService) {
    return securityProcessors[isSecurityEnabled(securityService) ? 1 : 0];
  }

  public Authenticator lookupAuthenticator(SecurityService securityService) {
    if (securityService.isIntegratedSecurity()) {
      // no need to care about thread safety, eventually there will only be one authenticator
      if (authenticators[2] == null) {
        authenticators[2] = new ProtobufShiroAuthenticator(securityService);
      }
      // Simple authenticator...normal shiro
      return authenticators[2];
    }
    if (isLegacySecurity(securityService)) {
      // Failing authentication...legacy security
      return authenticators[1];
    } else {
      // Noop authenticator...no security
      return authenticators[0];
    }
  }

  public Authorizer lookupAuthorizer(SecurityService securityService) {
    if (securityService.isIntegratedSecurity()) {
      // Simple authenticator...normal shiro
      if (authorizers[1] == null) {
        authorizers[1] = new ProtobufShiroAuthorizer(securityService);
      }
      // Simple authenticator...normal shiro
      return authorizers[1];
    }
    if (isLegacySecurity(securityService)) {
      // Failing authentication...legacy security
      // This should never be called.
      return null;
    } else {
      // Noop authenticator...no security
      return authorizers[0];
    }
  }

  private boolean isLegacySecurity(SecurityService securityService) {
    return securityService.isPeerSecurityRequired() || securityService.isClientSecurityRequired();
  }

  private boolean isSecurityEnabled(SecurityService securityService) {
    return securityService.isIntegratedSecurity() || isLegacySecurity(securityService);
  }
}
