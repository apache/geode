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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class QueueManagerImplTest {
  private final InternalPool pool = mock(InternalPool.class, RETURNS_DEEP_STUBS);
  private final Endpoint endpoint = mock(Endpoint.class);
  private final Endpoint backupEndpoint = mock(Endpoint.class);
  private final QueueConnectionImpl primary = mock(QueueConnectionImpl.class);
  private final QueueConnectionImpl backup = mock(QueueConnectionImpl.class);
  private final ClientUpdater clientUpdater = mock(ClientUpdater.class);
  private QueueManagerImpl queueManager;

  @Before
  public void setup() {
    queueManager = new QueueManagerImpl(pool, null, null, null, 1, 1, null, null);
    when(primary.getEndpoint()).thenReturn(endpoint);
    when(primary.getUpdater()).thenReturn(clientUpdater);
    when(primary.isDestroyed()).thenReturn(false);
    when(clientUpdater.isAlive()).thenReturn(true);
    when(clientUpdater.isProcessing()).thenReturn(true);
    when(endpoint.isClosed()).thenReturn(false);
    when(backup.getEndpoint()).thenReturn(backupEndpoint);
    when(backup.getUpdater()).thenReturn(clientUpdater);
    when(backupEndpoint.isClosed()).thenReturn(false);
  }

  @Test
  public void addNoClientUpdaterConnectionToConnectionListReturnsFalse() {
    when(primary.getUpdater()).thenReturn(null);

    assertThat(queueManager.addToConnectionList(primary, true)).isFalse();
    QueueManager.QueueConnections connectionList = queueManager.getAllConnectionsNoWait();
    assertThat(connectionList.getPrimary()).isNull();
  }

  @Test
  public void addNotAliveClientUpdaterConnectionToConnectionListReturnsFalse() {
    when(clientUpdater.isAlive()).thenReturn(false);

    assertThat(queueManager.addToConnectionList(primary, true)).isFalse();
    QueueManager.QueueConnections connectionList = queueManager.getAllConnectionsNoWait();
    assertThat(connectionList.getPrimary()).isNull();
  }

  @Test
  public void addNotProcessingClientUpdaterConnectionToConnectionListReturnsFalse() {
    when(clientUpdater.isProcessing()).thenReturn(false);

    assertThat(queueManager.addToConnectionList(primary, true)).isFalse();
    QueueManager.QueueConnections connectionList = queueManager.getAllConnectionsNoWait();
    assertThat(connectionList.getPrimary()).isNull();
  }

  @Test
  public void addClosedEndpointConnectionToConnectionListReturnsFalse() throws Exception {
    when(endpoint.isClosed()).thenReturn(true);

    assertThat(queueManager.addToConnectionList(primary, true)).isFalse();
    QueueManager.QueueConnections connectionList = queueManager.getAllConnectionsNoWait();
    assertThat(connectionList.getPrimary()).isNull();
    verify(primary).internalClose(true);
  }

  @Test
  public void addDestroyedConnectionToConnectionListReturnsFalse() throws Exception {
    when(primary.isDestroyed()).thenReturn(true);

    assertThat(queueManager.addToConnectionList(primary, true)).isFalse();
    QueueManager.QueueConnections connectionList = queueManager.getAllConnectionsNoWait();
    assertThat(connectionList.getPrimary()).isNull();
    verify(primary).internalClose(true);
  }

  @Test
  public void addConnectionToConnectionListWhenCancelInProgressReturnsFalse() throws Exception {
    when(pool.getPoolOrCacheCancelInProgress()).thenReturn("cache closed");

    assertThat(queueManager.addToConnectionList(primary, true)).isFalse();
    QueueManager.QueueConnections connectionList = queueManager.getAllConnectionsNoWait();
    assertThat(connectionList.getPrimary()).isNull();
    verify(primary).internalClose(true);
  }

  @Test
  public void addConnectionToConnectionListCanSetPrimary() throws Exception {
    assertThat(queueManager.addToConnectionList(primary, true)).isTrue();
    QueueManager.QueueConnections connectionList = queueManager.getAllConnectionsNoWait();
    assertThat(connectionList.getPrimary()).isEqualTo(primary);
    verify(primary, never()).internalClose(true);
  }

  @Test
  public void addConnectionToConnectionListCanAddBackups() throws Exception {
    queueManager.addToConnectionList(primary, true);

    assertThat(queueManager.addToConnectionList(backup, false)).isTrue();
    QueueManager.QueueConnections connectionList = queueManager.getAllConnectionsNoWait();
    assertThat(connectionList.getPrimary()).isEqualTo(primary);
    assertThat(connectionList.getBackups()).contains(backup);
    verify(backup, never()).internalClose(true);
  }

  @Test
  public void endpointCrashedScheduleRedundancySatisfierAfterConnectionDestroyed() {
    addConnections();
    QueueManagerImpl spy = spy(queueManager);
    InOrder inOrder = inOrder(primary, spy);
    doNothing().when(spy).scheduleRedundancySatisfierIfNeeded(0);

    spy.endpointCrashed(endpoint);

    inOrder.verify(primary).internalDestroy();
    inOrder.verify(spy).scheduleRedundancySatisfierIfNeeded(0);
    QueueManager.QueueConnections connectionList = spy.getAllConnectionsNoWait();
    assertThat(connectionList.getPrimary()).isNull();
  }

  private void addConnections() {
    queueManager.addToConnectionList(primary, true);
    queueManager.addToConnectionList(backup, false);
  }

  @Test
  public void recoverPrimaryDoesNotPromoteBackupToPrimaryIfPrimaryExists() {
    addConnections();
    QueueManagerImpl spy = spy(queueManager);

    spy.recoverPrimary(null);

    verify(spy, never()).promoteBackupToPrimary(anyList());
  }

  @Test
  public void recoverPrimaryPromoteBackupToPrimaryIfNoPrimary() {
    QueueManagerImpl spy = spy(queueManager);
    spy.addToConnectionList(backup, false);
    doReturn(backup).when(spy).promoteBackupToPrimary(anyList());

    spy.recoverPrimary(null);

    verify(spy).promoteBackupToPrimary(anyList());
    verifyQueueConnectionsAfterRecoverPrimary(spy);
  }

  private void verifyQueueConnectionsAfterRecoverPrimary(QueueManagerImpl spy) {
    QueueManager.QueueConnections connectionList = spy.getAllConnectionsNoWait();
    assertThat(connectionList.getPrimary()).isEqualTo(backup);
    assertThat(connectionList.getBackups()).isEmpty();
  }

  @Test
  public void recoverPrimaryPromoteBackupToPrimaryIfPrimaryConnectionIsDestroyed() {
    addConnections();
    QueueManagerImpl spy = spy(queueManager);
    doReturn(backup).when(spy).promoteBackupToPrimary(anyList());
    when(primary.isDestroyed()).thenReturn(true);

    spy.recoverPrimary(null);

    verify(spy).promoteBackupToPrimary(anyList());
    verifyQueueConnectionsAfterRecoverPrimary(spy);
  }
}
