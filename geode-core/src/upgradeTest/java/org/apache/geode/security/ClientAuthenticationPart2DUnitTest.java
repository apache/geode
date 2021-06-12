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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.VersionManager;

/**
 * this class contains test methods that used to be in its superclass but that test started taking
 * too long and caused dunit runs to hang
 */
@Category({SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ClientAuthenticationPart2DUnitTest extends ClientAuthenticationTestCase {
  @Parameterized.Parameters
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersions();
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    } else {
      System.out.println("running against these versions: " + result);
    }
    return result;
  }

  public ClientAuthenticationPart2DUnitTest(String version) {
    super();
    clientVersion = version;
  }

  @Test
  public void testNoCredentialsForMultipleUsers() throws Exception {
    doTestNoCredentials(true);
  }

  // GEODE-3249
  @Test
  public void testNoCredentialsForMultipleUsersCantRegisterMetadata() throws Exception {
    doTestNoCredentialsCantRegisterMetadata(true);
  }

  @Test
  public void testServerConnectionAcceptsOldInternalMessagesIfAllowed() throws Exception {

    ServerConnection serverConnection = mock(ServerConnection.class);
    when(serverConnection.isInternalMessage(any(Message.class), any(Boolean.class)))
        .thenCallRealMethod();

    int[] oldInternalMessages = new int[] {MessageType.ADD_PDX_TYPE, MessageType.ADD_PDX_ENUM,
        MessageType.REGISTER_INSTANTIATORS, MessageType.REGISTER_DATASERIALIZERS};

    for (int i = 0; i < oldInternalMessages.length; i++) {
      Message message = mock(Message.class);
      when(message.getMessageType()).thenReturn(oldInternalMessages[i]);

      Assert.assertFalse(serverConnection.isInternalMessage(message, false));
      Assert.assertTrue(serverConnection.isInternalMessage(message, true));
    }
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
