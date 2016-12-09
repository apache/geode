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
 *
 */

package org.apache.geode.internal.cache.tier.sockets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.DataOutputStream;
import java.io.IOException;

@Category(UnitTest.class)
public class MessageIdExtractorTest {
  @Mock
  Message requestMessage;

  @Mock
  HandShake handshake;

  private MessageIdExtractor messageIdExtractor;

  private Long connectionId = 123L;
  private Long uniqueId = 234L;
  private byte[] decryptedBytes;

  @Before
  public void before() throws Exception {
    this.messageIdExtractor = new MessageIdExtractor();
    decryptedBytes = byteArrayFromIds(connectionId, uniqueId);

    MockitoAnnotations.initMocks(this);

    when(handshake.decryptBytes(any())).thenReturn(decryptedBytes);
  }

  @Test
  public void getUniqueIdFromMessage() throws Exception {
    assertThat(messageIdExtractor.getUniqueIdFromMessage(requestMessage, handshake, connectionId))
        .isEqualTo(uniqueId);
  }

  @Test
  public void throwsWhenConnectionIdsDoNotMatch() throws Exception {
    long otherConnectionId = 789L;

    assertThatThrownBy(() -> messageIdExtractor.getUniqueIdFromMessage(requestMessage, handshake,
        otherConnectionId)).isInstanceOf(AuthenticationRequiredException.class);
  }

  private byte[] byteArrayFromIds(Long connectionId, Long uniqueId) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dis = new DataOutputStream(byteArrayOutputStream);
    dis.writeLong(connectionId);
    dis.writeLong(uniqueId);
    dis.flush();
    return byteArrayOutputStream.toByteArray();
  }

}
