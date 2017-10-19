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

package org.apache.geode.internal.protocol.protobuf;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.protocol.protobuf.security.ProtobufShiroAuthenticator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ProtobufShiroAuthenticatorJUnitTest {
  private static final String TEST_USERNAME = "user1";
  private static final String TEST_PASSWORD = "hunter2";
  // initialized with an incoming request in setUp.
  private ByteArrayInputStream byteArrayInputStream;
  private ByteArrayOutputStream byteArrayOutputStream;
  private ProtobufShiroAuthenticator protobufShiroAuthenticator;
  private SecurityService mockSecurityService;
  private Subject mockSecuritySubject;
  private Properties expectedAuthProperties;

  @Before
  public void setUp() throws IOException {
    ClientProtocol.Message basicAuthenticationRequest = ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setAuthenticationRequest(AuthenticationAPI.AuthenticationRequest.newBuilder()
                .putCredentials(ResourceConstants.USER_NAME, TEST_USERNAME)
                .putCredentials(ResourceConstants.PASSWORD, TEST_PASSWORD)))
        .build();

    expectedAuthProperties = new Properties();
    expectedAuthProperties.setProperty(ResourceConstants.USER_NAME, TEST_USERNAME);
    expectedAuthProperties.setProperty(ResourceConstants.PASSWORD, TEST_PASSWORD);

    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();
    basicAuthenticationRequest.writeDelimitedTo(messageStream);
    byteArrayInputStream = new ByteArrayInputStream(messageStream.toByteArray());
    byteArrayOutputStream = new ByteArrayOutputStream();

    mockSecuritySubject = mock(Subject.class);
    mockSecurityService = mock(SecurityService.class);
    when(mockSecurityService.login(expectedAuthProperties)).thenReturn(mockSecuritySubject);

    protobufShiroAuthenticator = new ProtobufShiroAuthenticator(mockSecurityService);
  }

  @Test
  public void successfulAuthentication() throws IOException {

    Properties properties = new Properties();
    properties.setProperty(ResourceConstants.USER_NAME, TEST_USERNAME);
    properties.setProperty(ResourceConstants.PASSWORD, TEST_PASSWORD);

    Subject authenticate = protobufShiroAuthenticator.authenticate(properties);

    assertNotNull(authenticate);
  }

  @Test(expected = AuthenticationFailedException.class)
  public void failedAuthentication() throws IOException {
    when(mockSecurityService.login(expectedAuthProperties))
        .thenThrow(new AuthenticationFailedException("BOOM!"));

    Properties properties = new Properties();
    properties.setProperty(ResourceConstants.USER_NAME, TEST_USERNAME);
    properties.setProperty(ResourceConstants.PASSWORD, TEST_PASSWORD);

    protobufShiroAuthenticator.authenticate(properties);
  }

  @Test
  public void authenticationRequestedWithNoCacheSecurity() throws IOException {
    when(mockSecurityService.isIntegratedSecurity()).thenReturn(false);
    when(mockSecurityService.isClientSecurityRequired()).thenReturn(false);
    when(mockSecurityService.isPeerSecurityRequired()).thenReturn(false);

    Properties properties = new Properties();
    properties.setProperty(ResourceConstants.USER_NAME, TEST_USERNAME);
    properties.setProperty(ResourceConstants.PASSWORD, TEST_PASSWORD);

    Subject authenticate = protobufShiroAuthenticator.authenticate(properties);

    assertNotNull(authenticate);
  }
}
