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
package org.apache.geode.cache.client.internal.pooling;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category({IntegrationTest.class, ClientServerTest.class})
public class PooledConnectionJUnitTest {

  @Test
  public void internalCloseMustCloseTheInputStream() throws Exception {
    Connection connection = mock(Connection.class);
    ConnectionManagerImpl connectionManager = mock(ConnectionManagerImpl.class);
    InputStream inputStream = mock(InputStream.class);
    when(connection.getInputStream()).thenReturn(inputStream);
    PooledConnection pooledConnection = new PooledConnection(connectionManager, connection);
    doNothing().when(connection).close(false);
    pooledConnection.internalClose(false);
    verify(inputStream, times(1)).close();
  }
}
