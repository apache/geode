/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.security;

import java.util.Collection;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * Test for authentication from client to server. This tests for both valid and invalid
 * credentials/modules. It also checks for authentication success/failure in case of failover and
 * for the notification channel.
 *
 * @since GemFire 5.5
 */
@Category({SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ClientAuthenticationDUnitTest extends ClientAuthenticationTestCase {
  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersions();
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    } else {
      System.out.println("running against these versions: " + result);
    }
    return result;
  }

  public ClientAuthenticationDUnitTest(String version) {
    super();
    clientVersion = version;
  }

  @Test
  public void testValidCredentials() throws Exception {
    doTestValidCredentials(false);
  }

  @Test
  public void testNoCredentials() throws Exception {
    doTestNoCredentials(false);
  }

  @Test
  public void testInvalidCredentials() throws Exception {
    doTestInvalidCredentials(false);
  }

  @Test
  public void testInvalidAuthInit() throws Exception {
    doTestInvalidAuthInit(false);
  }

  @Test
  public void testNoAuthInitWithCredentials() throws Exception {
    doTestNoAuthInitWithCredentials(false);
  }

  @Test
  public void testInvalidAuthenticator() throws Exception {
    doTestInvalidAuthenticator(false);
  }

  @Test
  public void testNoAuthenticatorWithCredentials() throws Exception {
    doTestNoAuthenticatorWithCredentials(false);
  }

  @Test
  public void testCredentialsWithFailover() throws Exception {
    doTestCredentialsWithFailover(false);
  }

  @Test
  public void testCredentialsForNotifications() throws Exception {
    doTestCredentialsForNotifications(false);
  }

  @Ignore("Disabled for unknown reason")
  @Test
  public void testValidCredentialsForMultipleUsers() throws Exception {
    doTestValidCredentials(true);
  }
}
