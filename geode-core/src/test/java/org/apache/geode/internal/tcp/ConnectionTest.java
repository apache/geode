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
package org.apache.geode.internal.tcp;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ConnectionTest {

  @Test
  public void shouldBeMockable() throws Exception {
    Connection mockConnection = mock(Connection.class);
    SocketChannel channel = null;
    ByteBuffer buffer = null;
    boolean forceAsync = true;
    DistributionMessage mockDistributionMessage = mock(DistributionMessage.class);

    mockConnection.nioWriteFully(channel, buffer, forceAsync, mockDistributionMessage);

    verify(mockConnection, times(1)).nioWriteFully(channel, buffer, forceAsync,
        mockDistributionMessage);
  }
}
