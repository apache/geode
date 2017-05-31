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
package org.apache.geode.internal.security;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;

public class FakeSecurityManager implements SecurityManager {

  private final AtomicInteger initInvocations = new AtomicInteger(0);
  private final AtomicInteger authenticateInvocations = new AtomicInteger(0);
  private final AtomicInteger authorizeInvocations = new AtomicInteger(0);
  private final AtomicInteger closeInvocations = new AtomicInteger(0);

  private final AtomicReference<Properties> securityPropsRef = new AtomicReference<>();
  private final AtomicReference<Properties> credentialsRef = new AtomicReference<>();
  private final AtomicReference<AuthorizeArguments> processAuthorizeArgumentsRef =
      new AtomicReference<>();

  @Override
  public void init(final Properties securityProps) {
    this.initInvocations.incrementAndGet();
    this.securityPropsRef.set(securityProps);
  }

  @Override
  public Object authenticate(final Properties credentials) throws AuthenticationFailedException {
    this.authenticateInvocations.incrementAndGet();
    this.credentialsRef.set(credentials);
    return credentials;
  }

  @Override
  public boolean authorize(final Object principal, final ResourcePermission permission) {
    this.authorizeInvocations.incrementAndGet();
    this.processAuthorizeArgumentsRef.set(new AuthorizeArguments(principal, permission));
    return true;
  }

  @Override
  public void close() {
    this.closeInvocations.incrementAndGet();
  }

  public int getInitInvocations() {
    return this.initInvocations.get();
  }

  public int getAuthenticateInvocations() {
    return this.authenticateInvocations.get();
  }

  public int getAuthorizeInvocations() {
    return this.authorizeInvocations.get();
  }

  public int getCloseInvocations() {
    return this.closeInvocations.get();
  }

  public Properties getSecurityProps() {
    return this.securityPropsRef.get();
  }

  public AuthorizeArguments getAuthorizeArguments() {
    return this.processAuthorizeArgumentsRef.get();
  }

  public static class AuthorizeArguments {
    private final Object principal;
    private final ResourcePermission permission;

    public AuthorizeArguments(final Object principal, final ResourcePermission permission) {
      this.principal = principal;
      this.permission = permission;
    }

    public Object getPrincipal() {
      return this.principal;
    }

    public ResourcePermission getPermission() {
      return this.permission;
    }
  }
}
