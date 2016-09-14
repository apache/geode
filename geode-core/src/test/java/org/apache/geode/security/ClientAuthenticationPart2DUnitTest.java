/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.geode.security;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * this class contains test methods that used to be in its superclass but
 * that test started taking too long and caused dunit runs to hang
 */
@Category({ DistributedTest.class, SecurityTest.class })
public class ClientAuthenticationPart2DUnitTest extends ClientAuthenticationTestCase {

  @Test
  public void testNoCredentialsForMultipleUsers() throws Exception {
    doTestNoCredentials(true);
  }

  @Test
  public void testInvalidCredentialsForMultipleUsers() throws Exception {
    doTestInvalidCredentials(true);
  }

  @Test
  public void testInvalidAuthInitForMultipleUsers() throws Exception {
    doTestInvalidAuthInit(true);
  }

  @Test
  public void testNoAuthInitWithCredentialsForMultipleUsers() throws Exception {
    doTestNoAuthInitWithCredentials(true);
  }

  @Test
  public void testInvalidAuthenitcatorForMultipleUsers() throws Exception {
    doTestInvalidAuthenticator(true);
  }

  @Test
  public void testNoAuthenticatorWithCredentialsForMultipleUsers() throws Exception {
    doTestNoAuthenticatorWithCredentials(true);
  }

  @Ignore("Disabled for unknown reason")
  @Test
  public void testCredentialsWithFailoverForMultipleUsers() throws Exception {
    doTestCredentialsWithFailover(true);
  }

  @Ignore("Disabled for unknown reason")
  @Test
  public void testCredentialsForNotificationsForMultipleUsers() throws Exception {
    doTestCredentialsForNotifications(true);
  }
}
