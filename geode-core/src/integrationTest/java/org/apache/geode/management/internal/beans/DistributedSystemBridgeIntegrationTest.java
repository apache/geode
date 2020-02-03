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
package org.apache.geode.management.internal.beans;

import static org.apache.geode.management.internal.ManagementConstants.OBJECTNAME__GATEWAYSENDER_MXBEAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.text.MessageFormat;
import java.util.Map;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.backup.AbortBackupRequest;
import org.apache.geode.internal.cache.backup.BackupLockService;
import org.apache.geode.internal.cache.backup.BackupService;
import org.apache.geode.internal.cache.backup.FinishBackupRequest;
import org.apache.geode.internal.cache.backup.PrepareBackupRequest;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.internal.FederationComponent;
import org.apache.geode.test.fake.Fakes;

public class DistributedSystemBridgeIntegrationTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private GemFireCacheImpl cache;
  private BackupService backupService;
  private DistributedSystemBridge bridge;
  private GatewaySenderMXBean bean1, bean2, bean3, bean4;

  @Before
  public void createCache() throws MalformedObjectNameException {
    cache = Fakes.cache();
    PersistentMemberManager memberManager = mock(PersistentMemberManager.class);
    backupService = mock(BackupService.class);
    when(cache.getBackupService()).thenReturn(backupService);
    when(cache.getPersistentMemberManager()).thenReturn(memberManager);
    when(cache.getBackupService()).thenReturn(backupService);

    DLockService dlock = mock(DLockService.class);
    when(dlock.lock(any(), anyLong(), anyLong())).thenReturn(true);

    DLockService.addLockServiceForTests(BackupLockService.LOCK_SERVICE_NAME, dlock);

    ObjectName name1 = ObjectName
        .getInstance(MessageFormat.format(OBJECTNAME__GATEWAYSENDER_MXBEAN, "sender1", "server1"));
    ObjectName name2 = ObjectName
        .getInstance(MessageFormat.format(OBJECTNAME__GATEWAYSENDER_MXBEAN, "sender2", "server2"));
    ObjectName name3 = ObjectName
        .getInstance(MessageFormat.format(OBJECTNAME__GATEWAYSENDER_MXBEAN, "sender3", "server3"));
    ObjectName name4 = ObjectName
        .getInstance(MessageFormat.format(OBJECTNAME__GATEWAYSENDER_MXBEAN, "sender4", "server4"));

    bean1 = mock(GatewaySenderMXBean.class);
    bean2 = mock(GatewaySenderMXBean.class);
    bean3 = mock(GatewaySenderMXBean.class);
    bean4 = mock(GatewaySenderMXBean.class);

    doReturn(2).when(bean1).getRemoteDSId();
    doReturn(2).when(bean2).getRemoteDSId();
    doReturn(3).when(bean3).getRemoteDSId();
    doReturn(3).when(bean4).getRemoteDSId();

    bridge = new DistributedSystemBridge(null, cache);
    bridge.addGatewaySenderToSystem(name1, bean1, mock(FederationComponent.class));
    bridge.addGatewaySenderToSystem(name2, bean2, mock(FederationComponent.class));
    bridge.addGatewaySenderToSystem(name3, bean3, mock(FederationComponent.class));
    bridge.addGatewaySenderToSystem(name4, bean4, mock(FederationComponent.class));
  }

  @After
  public void clearCache() {
    DLockService.removeLockServiceForTests(BackupLockService.LOCK_SERVICE_NAME);
  }

  @Test
  public void testSuccessfulBackup() throws Exception {
    DistributionManager dm = cache.getDistributionManager();

    bridge.backupAllMembers(temporaryFolder.getRoot().getAbsolutePath(), null);

    InOrder inOrder = inOrder(dm, backupService);
    inOrder.verify(dm).putOutgoing(isA(PrepareBackupRequest.class));
    inOrder.verify(backupService).prepareBackup(any(), any());
    inOrder.verify(dm).putOutgoing(isA(FinishBackupRequest.class));
    inOrder.verify(backupService).doBackup();
  }

  @Test
  public void testPrepareErrorAbortsBackup() {
    DistributionManager dm = cache.getDistributionManager();
    PersistentMemberManager memberManager = mock(PersistentMemberManager.class);
    BackupService backupService = mock(BackupService.class);
    when(cache.getBackupService()).thenReturn(backupService);
    when(cache.getPersistentMemberManager()).thenReturn(memberManager);
    when(cache.getBackupService()).thenReturn(backupService);
    when(dm.putOutgoing(isA(PrepareBackupRequest.class)))
        .thenThrow(new RuntimeException("Fail the prepare"));
    assertThatThrownBy(
        () -> bridge.backupAllMembers(temporaryFolder.getRoot().getAbsolutePath(), null))
            .isInstanceOf(RuntimeException.class).hasMessage("Fail the prepare");

    verify(dm).putOutgoing(isA(AbortBackupRequest.class));
    verify(backupService).abortBackup();
  }

  @Test
  public void viewClusterStatusShouldBeTrueIfAllParallelSendersAreRunning() {
    // parallel senders for dsid = 2
    doReturn(true).when(bean1).isParallel();
    doReturn(true).when(bean1).isRunning();
    doReturn(true).when(bean2).isParallel();
    doReturn(true).when(bean2).isRunning();

    // parallel senders for dsid = 3
    doReturn(true).when(bean3).isParallel();
    doReturn(true).when(bean3).isRunning();
    doReturn(true).when(bean4).isParallel();
    doReturn(true).when(bean4).isRunning();

    Map<String, Boolean> status = bridge.viewRemoteClusterStatus();
    assertThat(status.keySet()).hasSize(2);
    assertThat(status.keySet()).contains("2", "3");
    assertThat(status.values()).contains(true, true);
  }

  @Test
  public void viewClusterStatusShouldBeFalseIfAnyParallelSendersIsNotRunning() {
    // parallel senders for dsid = 2
    doReturn(true).when(bean1).isParallel();
    doReturn(true).when(bean1).isRunning();
    doReturn(true).when(bean2).isParallel();
    doReturn(true).when(bean2).isRunning();

    // parallel senders for dsid = 3
    doReturn(true).when(bean3).isParallel();
    doReturn(true).when(bean3).isRunning();
    doReturn(true).when(bean4).isParallel();
    doReturn(false).when(bean4).isRunning();

    Map<String, Boolean> status = bridge.viewRemoteClusterStatus();
    assertThat(status.keySet()).hasSize(2);
    assertThat(status.keySet()).contains("2", "3");
    assertThat(status.values()).contains(true, false);
  }

  @Test
  public void viewClusterStatusShouldBeTrueIfASerialPrimaryIsRunning() {
    // serial primary for dsid = 2
    doReturn(false).when(bean1).isParallel();
    doReturn(true).when(bean1).isPrimary();
    doReturn(true).when(bean1).isRunning();

    // serial secondary for dsid = 2
    doReturn(false).when(bean2).isParallel();
    doReturn(false).when(bean2).isPrimary();
    doReturn(false).when(bean2).isRunning();

    // serial primary for dsid = 3
    doReturn(false).when(bean3).isParallel();
    doReturn(true).when(bean3).isPrimary();
    doReturn(true).when(bean3).isRunning();

    // serial secondary for dsid = 3
    doReturn(false).when(bean4).isParallel();
    doReturn(false).when(bean4).isPrimary();
    doReturn(false).when(bean4).isRunning();

    Map<String, Boolean> status = bridge.viewRemoteClusterStatus();
    assertThat(status.keySet()).hasSize(2);
    assertThat(status.keySet()).contains("2", "3");
    assertThat(status.values()).contains(true, true);
  }

  @Test
  public void viewClusterStatusShouldBeFalseIfASerialPrimaryIsNotRunning() {
    // serial primary for dsid = 2
    doReturn(false).when(bean1).isParallel();
    doReturn(true).when(bean1).isPrimary();
    doReturn(true).when(bean1).isRunning();

    // serial secondary for dsid = 2
    doReturn(false).when(bean2).isParallel();
    doReturn(false).when(bean2).isPrimary();
    doReturn(false).when(bean2).isRunning();

    // serial primary for dsid = 3
    doReturn(false).when(bean3).isParallel();
    doReturn(true).when(bean3).isPrimary();
    doReturn(false).when(bean3).isRunning();

    // serial secondary for dsid = 3
    doReturn(false).when(bean4).isParallel();
    doReturn(false).when(bean4).isPrimary();
    doReturn(true).when(bean4).isRunning();

    Map<String, Boolean> status = bridge.viewRemoteClusterStatus();
    assertThat(status.keySet()).hasSize(2);
    assertThat(status.keySet()).contains("2", "3");
    assertThat(status.values()).contains(true, false);
  }
}
