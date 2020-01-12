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
package org.apache.geode.cache.client.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.internal.pooling.ConnectionDestroyedException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class QueueConnectionImplJUnitTest {

  @Test
  public void testInternalDestroyFollowedByInternalClose() {
    // Mock ClientUpdater and Connection
    ClientUpdater updater = mock(ClientUpdater.class);
    Connection connection = mock(Connection.class);

    // Create a QueueConnectionImpl on the mocks
    QueueConnectionImpl qci = new QueueConnectionImpl(null, connection, updater, null);

    // Invoke internalDestroy (which sets the updater to null)
    qci.internalDestroy();

    // Assert that invoking internalClose throws a ConnectionDestroyedException and not a
    // NullPointerException
    assertThatThrownBy(() -> qci.internalClose(false))
        .isInstanceOf(ConnectionDestroyedException.class);
  }
}
