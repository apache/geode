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


import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.security.processors.AuthorizationSecurityProcessor;
import org.apache.geode.internal.protocol.security.processors.NoAuthenticationSecurityProcessor;
import org.apache.geode.internal.protocol.security.Authenticator;
import org.apache.geode.internal.protocol.security.NoOpAuthenticator;
import org.apache.geode.internal.protocol.security.SecurityProcessor;
import org.apache.geode.internal.protocol.statistics.ProtocolClientStatistics;
import org.apache.geode.internal.protocol.security.Authorizer;
import org.apache.geode.internal.protocol.security.NoOpAuthorizer;

@Experimental
public class MessageExecutionContext {
  private final Cache cache;
  private final Locator locator;
  private final Authorizer authorizer;
  private Object authenticatedToken;
  private final ProtocolClientStatistics statistics;
  private SecurityProcessor securityProcessor;
  private final Authenticator authenticator;


  public MessageExecutionContext(Cache cache, Authenticator authenticator,
      Authorizer streamAuthorizer, Object authenticatedToken, ProtocolClientStatistics statistics,
      SecurityProcessor securityProcessor) {
    this.cache = cache;
    this.locator = null;
    this.authorizer = streamAuthorizer;
    this.authenticatedToken = authenticatedToken;
    this.statistics = statistics;
    this.securityProcessor = securityProcessor;
    this.authenticator = authenticator;
  }

  public MessageExecutionContext(InternalLocator locator, ProtocolClientStatistics statistics) {
    this.locator = locator;
    this.cache = null;
    // set a no-op authorizer until such time as locators implement authentication
    // and authorization checks
    this.authorizer = new NoOpAuthorizer();
    this.authenticator = new NoOpAuthenticator();
    this.statistics = statistics;
    this.securityProcessor = new NoAuthenticationSecurityProcessor();
  }

  /**
   * Returns the cache associated with this execution
   * <p>
   * 
   * @throws InvalidExecutionContextException if there is no cache available
   */
  public Cache getCache() throws InvalidExecutionContextException {
    if (cache != null) {
      return cache;
    }
    throw new InvalidExecutionContextException(
        "Operations on the locator should not to try to operate on a cache");
  }

  /**
   * Returns the locator associated with this execution
   * <p>
   * 
   * @throws InvalidExecutionContextException if there is no locator available
   */
  public Locator getLocator() throws InvalidExecutionContextException {
    if (locator != null) {
      return locator;
    }
    throw new InvalidExecutionContextException(
        "Operations on the server should not to try to operate on a locator");
  }

  /**
   * Return the authorizer associated with this execution
   */
  public Authorizer getAuthorizer() {
    return authorizer;
  }

  /**
   * Returns the authentication token associated with this execution
   */
  public Object getAuthenticationToken() {
    return authenticatedToken;
  }


  /**
   * Returns the statistics for recording operation stats. In a unit test environment this may not
   * be a protocol-specific statistics implementation.
   */
  public ProtocolClientStatistics getStatistics() {
    return statistics;
  }

  public Authenticator getAuthenticator() {
    return authenticator;
  }

  public SecurityProcessor getSecurityProcessor() {
    return securityProcessor;
  }

  public void setSecurityProcessor(AuthorizationSecurityProcessor securityProcessor) {
    this.securityProcessor = securityProcessor;
  }

  public void setAuthenticationToken(Object authenticationToken) {
    this.authenticatedToken = authenticationToken;
  }
}
