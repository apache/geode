/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */package com.gemstone.gemfire.distributed.internal.membership.gms.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.security.Principal;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.ServiceConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.security.SecurityService;
import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;

import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractGMSAuthenticatorTestCase {

  protected SecurityService securityService;
  protected Properties props;
  protected Properties securityProps;
  protected Services services;
  protected GMSAuthenticator authenticator;
  protected DistributedMember member;
  protected Subject subject;

  @Before
  public void setUp() throws Exception {
    clearStatics();

    props = new Properties();
    securityProps = new Properties();

    subject = mock(Subject.class);

    securityService = mock(SecurityService.class);
    when(securityService.isIntegratedSecurity(isA(Properties.class))).thenReturn(isIntegratedSecurity());
    when(securityService.isPeerSecurityRequired(isA(Properties.class))).thenReturn(true);
    when(securityService.login(anyString(), anyString())).thenReturn(subject);

    authenticator = new GMSAuthenticator(securityService);

    InternalLogWriter securityLog = mock(InternalLogWriter.class);
    DistributionConfig distributionConfig = mock(DistributionConfig.class);
    when(distributionConfig.getSecurityProps()).thenReturn(securityProps);

    ServiceConfig serviceConfig = mock(ServiceConfig.class);
    when(serviceConfig.getDistributionConfig()).thenReturn(distributionConfig);

    services = mock(Services.class);
    when(services.getSecurityLogWriter()).thenReturn(securityLog);
    when(services.getConfig()).thenReturn(serviceConfig);

    authenticator.init(services);

    member = mock(DistributedMember.class);
  }

  protected abstract boolean isIntegratedSecurity();

  private static void clearStatics() {
    SpyAuthInit.clear();
    SpyAuthenticator.clear();
  }

  protected static final class AuthInitCreateReturnsNull implements AuthInitialize {

    public static AuthInitialize create() {
      return null;
    }

    @Override
    public void init(LogWriter systemLogger, LogWriter securityLogger) throws AuthenticationFailedException {
    }

    @Override
    public Properties getCredentials(Properties props, DistributedMember server, boolean isPeer) throws AuthenticationFailedException {
      throw new AuthenticationFailedException("expected get credential error");
    }

    @Override
    public void close() {
    }
  }

  protected static final class SpyAuthInit implements AuthInitialize {

    private static SpyAuthInit instance = null;
    private static int createCount = 0;

    boolean closed = false;

    static void clear() {
      instance = null;
      createCount = 0;
    }

    public static void setAuthInitialize(SpyAuthInit auth) {
      instance = auth;
    }

    public static AuthInitialize create() {
      createCount++;
      return instance;
    }

    @Override
    public Properties getCredentials(Properties props, DistributedMember server, boolean isPeer) throws AuthenticationFailedException {
      return props;
    }

    @Override
    public void close() {
      closed = true;
    }

    public boolean isClosed() {
      return closed;
    }

    public static int getCreateCount() {
      return createCount;
    }

    @Override
    public void init(LogWriter systemLogger, LogWriter securityLogger) throws AuthenticationFailedException {
    }
  }

  protected static final class AuthInitGetCredentialsAndInitThrow implements AuthInitialize {

    public static AuthInitialize create() {
      return new AuthInitGetCredentialsAndInitThrow();
    }

    @Override
    public void init(LogWriter systemLogger, LogWriter securityLogger) throws AuthenticationFailedException {
      throw new AuthenticationFailedException("expected init error");
    }

    @Override
    public Properties getCredentials(Properties props, DistributedMember server, boolean isPeer) throws AuthenticationFailedException {
      throw new AuthenticationFailedException("expected get credential error");
    }

    @Override
    public void close() {
    }
  }

  protected static final class AuthInitGetCredentialsThrows implements AuthInitialize {

    public static AuthInitialize create() {
      return new AuthInitGetCredentialsThrows();
    }

    @Override
    public void init(LogWriter systemLogger, LogWriter securityLogger) throws AuthenticationFailedException {
    }

    @Override
    public Properties getCredentials(Properties props, DistributedMember server, boolean isPeer) throws AuthenticationFailedException {
      throw new AuthenticationFailedException("expected get credential error");
    }

    @Override
    public void close() {
    }
  }

  protected static final class AuthenticatorReturnsNulls implements Authenticator {

    public static Authenticator create() {
      return null;
    }

    @Override
    public void init(Properties securityProps, LogWriter systemLogger, LogWriter securityLogger) throws AuthenticationFailedException {
    }

    @Override
    public Principal authenticate(Properties props, DistributedMember member) throws AuthenticationFailedException {
      return null;
    }

    @Override
    public void close() {
    }
  }

  protected static final class AuthenticatorInitThrows implements Authenticator {

    public static Authenticator create() {
      return new AuthenticatorInitThrows();
    }

    @Override
    public void init(Properties securityProps, LogWriter systemLogger, LogWriter securityLogger) throws AuthenticationFailedException {
      throw new AuthenticationFailedException("expected init error");
    }

    @Override
    public Principal authenticate(Properties props, DistributedMember member) throws AuthenticationFailedException {
      return null;
    }

    @Override
    public void close() {
    }
  }

  protected static final class AuthenticatorAuthenticateThrows implements Authenticator {

    public static Authenticator create() {
      return new AuthenticatorAuthenticateThrows();
    }

    @Override
    public Principal authenticate(Properties props, DistributedMember member) throws AuthenticationFailedException {
      throw new AuthenticationFailedException("expected authenticate error");
    }

    @Override
    public void init(Properties securityProps, LogWriter systemLogger, LogWriter securityLogger) throws AuthenticationFailedException {
    }

    @Override
    public void close() {
    }
  }

  protected static final class SpyAuthenticator implements Authenticator {

    private static Authenticator instance = null;
    private static int createCount = 0;

    private boolean closed = false;

    static void clear() {
      instance = null;
      createCount = 0;
    }

    public static void setAuthenticator(Authenticator auth) {
      instance = auth;
    }

    public static Authenticator create() {
      createCount++;
      return instance;
    }

    @Override
    public Principal authenticate(Properties props, DistributedMember member) throws AuthenticationFailedException {
      return null;
    }

    @Override
    public void close() {
      closed = true;
    }

    public boolean isClosed() {
      return closed;
    }

    public static int getCreateCount() {
      return createCount;
    }

    @Override
    public void init(Properties securityProps, LogWriter systemLogger, LogWriter securityLogger) throws AuthenticationFailedException {
    }
  }
}
