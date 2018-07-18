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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.distributed.PoolCancelledException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.net.SocketCreatorFactory;

public class ConnectionFactoryJUnitTest {

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {
    SocketCreatorFactory.close();
  }

  @Test(expected = CancelException.class)
  public void connectionFactoryThrowsCancelException() throws CancelException, IOException {
    ServerLocation serverLocation = mock(ServerLocation.class);
    doReturn(false).when(serverLocation).getRequiresCredentials();

    ConnectionConnector connector = mock(ConnectionConnector.class);
    doReturn(mock(ConnectionImpl.class)).when(connector).connectClientToServer(serverLocation,
        false);

    ConnectionSource connectionSource = mock(ConnectionSource.class);
    doReturn(serverLocation).when(connectionSource).findServer(any(Set.class));

    // mocks don't seem to work well with CancelCriterion so let's create a real one
    CancelCriterion cancelCriterion = new CancelCriterion() {
      @Override
      public String cancelInProgress() {
        return "shutting down for test";
      }

      @Override
      public RuntimeException generateCancelledException(Throwable throwable) {
        return new PoolCancelledException(cancelInProgress(), throwable);
      }
    };

    ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(connector, connectionSource,
        60000, mock(PoolImpl.class), cancelCriterion);

    connectionFactory.createClientToServerConnection(Collections.emptySet());
  }
}
