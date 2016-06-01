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
 */
package com.gemstone.gemfire.distributed.internal.membership.gms.auth;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;
import com.gemstone.gemfire.security.GemFireSecurityException;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import java.security.Principal;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

@Category({ UnitTest.class, SecurityTest.class })
public class GMSAuthenticatorJUnitTest {

  private String prefix;
  private Properties props;
  private Services services;
  private GMSAuthenticator authenticator;
  private DistributedMember member;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    prefix = getClass().getName() + "$";
    props = new Properties();
    authenticator = new GMSAuthenticator();

    services = mock(Services.class);
    InternalLogWriter securityLog = mock(InternalLogWriter.class);
    when(services.getSecurityLogWriter()).thenReturn(mock(InternalLogWriter.class));

    authenticator.init(services);

    member = mock(DistributedMember.class);
  }

  @Test
  public void testGetSecurityProps() throws Exception {
    props.setProperty(DistributionConfig.GEMFIRE_PREFIX + "sys."+SECURITY_PEER_AUTH_INIT, "dummy1");
    props.setProperty(DistributionConfig.GEMFIRE_PREFIX + "sys."+SECURITY_PEER_AUTHENTICATOR, "dummy2");
    props.setProperty("security-auth-init", "dummy3");
    System.setProperties(props);
    Properties secProps = authenticator.getSecurityProps();
    assertEquals("wrong size", 2, secProps.size());
    assertEquals("wrong value", "dummy1", secProps.getProperty(SECURITY_PEER_AUTH_INIT));
    assertEquals("wrong value", "dummy2", secProps.getProperty(SECURITY_PEER_AUTHENTICATOR));
  }

  @Test
  public void testGetCredentialNormal() throws Exception {
    props.setProperty(SECURITY_PEER_AUTH_INIT, prefix + "TestAuthInit2.create");
    TestAuthInit2 auth = new TestAuthInit2();
    assertFalse(auth.isClosed());
    TestAuthInit2.setAuthInitialize(auth);
    Properties credential = authenticator.getCredentials(member, props);
    assertTrue(props == credential);
    assertTrue(auth.isClosed());
    assertTrue(TestAuthInit2.getCreateCount() == 1);
  }

  @Test
  public void testGetCredentialWithNoAuth() throws Exception {
    Properties credential = authenticator.getCredentials(member, props);
    assertNull(credential);
  }

  @Test
  public void testGetCredentialWithEmptyAuth() throws Exception {
    props.setProperty(SECURITY_PEER_AUTH_INIT, "");
    Properties credential = authenticator.getCredentials(member, props);
    assertNull(credential);
  }

  @Test
  public void testGetCredentialWithNotExistAuth() throws Exception {
    props.setProperty(SECURITY_PEER_AUTH_INIT, prefix + "NotExistAuth.create");
    verifyNegativeGetCredential(props, "Failed to acquire AuthInitialize method");
  }

  @Test
  public void testGetCredentialWithNullAuth() throws Exception {
    props.setProperty(SECURITY_PEER_AUTH_INIT, prefix + "TestAuthInit1.create");
    verifyNegativeGetCredential(props, "AuthInitialize instance could not be obtained");
  }

  @Test
  public void testGetCredentialWithInitError() throws Exception {
    props.setProperty(SECURITY_PEER_AUTH_INIT, prefix + "TestAuthInit3.create");
    verifyNegativeGetCredential(props, "expected init error");
  }

  @Test
  public void testGetCredentialWithError() throws Exception {
    props.setProperty(SECURITY_PEER_AUTH_INIT, prefix + "TestAuthInit4.create");
    verifyNegativeGetCredential(props, "expected get credential error");
  }

  private void verifyNegativeGetCredential(Properties props, String expectedError) throws Exception {
    try {
      authenticator.getCredentials(member, props);
      fail("should catch: " + expectedError);
    } catch (GemFireSecurityException expected) {
      assertTrue(expected.getMessage().startsWith(expectedError));
    }
  }

  @Test
  public void testAuthenticatorNormal() throws Exception {
    props.setProperty(SECURITY_PEER_AUTHENTICATOR, prefix + "TestAuthenticator4.create");
    TestAuthenticator4 auth = new TestAuthenticator4();
    assertFalse(auth.isClosed());
    TestAuthenticator4.setAuthenticator(auth);
    String result = authenticator.authenticate(member, props, props, member);
    assertNull(result);
    assertTrue(auth.isClosed());
    assertTrue(TestAuthenticator4.getCreateCount() == 1);
  }

  @Test
  public void testAuthenticatorWithNoAuth() throws Exception {
    String result = authenticator.authenticate(member, props, props, member);
    assertNull(result);
  }

  @Test
  public void testAuthenticatorWithEmptyAuth() throws Exception {
    props.setProperty(DistributionConfig.SECURITY_PEER_AUTHENTICATOR, "");
    String result = authenticator.authenticate(member, props, props, member);
    assertNull(result);
  }

  @Test
  public void testAuthenticatorWithNotExistAuth() throws Exception {
    props.setProperty(DistributionConfig.SECURITY_PEER_AUTHENTICATOR, prefix + "NotExistAuth.create");
    verifyNegativeAuthenticate(props, props, "Authentication failed. See coordinator");
  }

  @Test
  public void testAuthenticatorWithNullAuth() throws Exception {
    props.setProperty(DistributionConfig.SECURITY_PEER_AUTHENTICATOR, prefix + "TestAuthenticator1.create");
    verifyNegativeAuthenticate(props, props, "Authentication failed. See coordinator");
  }

  @Test
  public void testAuthenticatorWithNullCredential() throws Exception {
    props.setProperty(DistributionConfig.SECURITY_PEER_AUTHENTICATOR, prefix + "TestAuthenticator1.create");
    verifyNegativeAuthenticate(null, props, "Failed to find credentials from");
  }

  @Test
  public void testAuthenticatorWithAuthInitFailure() throws Exception {
    props.setProperty(DistributionConfig.SECURITY_PEER_AUTHENTICATOR, prefix + "TestAuthenticator2.create");
    verifyNegativeAuthenticate(props, props, "Authentication failed. See coordinator");
  }

  @Test
  public void testAuthenticatorWithAuthFailure() throws Exception {
    props.setProperty(DistributionConfig.SECURITY_PEER_AUTHENTICATOR, prefix + "TestAuthenticator3.create");
    verifyNegativeAuthenticate(props, props, "Authentication failed. See coordinator");
  }

  void verifyNegativeAuthenticate(Object credential, Properties props, String expectedError) throws Exception {
    String result = authenticator.authenticate(member, credential, props, member);
    assertTrue(result, result.startsWith(expectedError));
  }

  private static class TestAuthInit1 implements AuthInitialize {
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

  private static class TestAuthInit2 extends TestAuthInit1 {

    private static TestAuthInit2 instance = null;
    private static int createCount = 0;

    boolean closed = false;

    public static void setAuthInitialize(TestAuthInit2 auth) {
      instance = auth;
    }
    public static AuthInitialize create() {
      createCount ++;
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
  }

  // used by reflection by test
  private static class TestAuthInit3 extends TestAuthInit1 {
    public static AuthInitialize create() {
      return new TestAuthInit3();
    }
    @Override
    public void init(LogWriter systemLogger, LogWriter securityLogger) throws AuthenticationFailedException {
      throw new AuthenticationFailedException("expected init error");
    }
  }

  private static class TestAuthInit4 extends TestAuthInit1 {
    public static AuthInitialize create() {
      return new TestAuthInit4();
    }
  }

  private static class TestAuthenticator1 implements Authenticator {
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

  private static class TestAuthenticator2 extends TestAuthenticator1 {
    public static Authenticator create() {
      return new TestAuthenticator2();
    }
    @Override
    public void init(Properties securityProps, LogWriter systemLogger, LogWriter securityLogger) throws AuthenticationFailedException {
      throw new AuthenticationFailedException("expected init error");
    }
  }

  private static class TestAuthenticator3 extends TestAuthenticator1 {
    public static Authenticator create() {
      return new TestAuthenticator3();
    }
    @Override
    public Principal authenticate(Properties props, DistributedMember member) throws AuthenticationFailedException {
      throw new AuthenticationFailedException("expected authenticate error");
    }
  }

  private static class TestAuthenticator4 extends TestAuthenticator1 {

    private static Authenticator instance = null;
    private static int createCount = 0;

    private boolean closed = false;

    public static void setAuthenticator(Authenticator auth) {
      instance = auth;
    }
    public static Authenticator create() {
      createCount ++;
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
  }
}
