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

package org.apache.geode.internal.cache.locks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock;

public class TXLockServiceImplTest {
  private TXLockServiceImpl txLockService;
  private InternalDistributedSystem internalDistributedSystem;
  private DLockService dlock;
  private List distLocks;
  private Set otherMembers;
  private DistributionManager distributionManager;
  private InternalDistributedMember distributedMember;
  private StoppableReentrantReadWriteLock recoverylock;

  @Before
  public void setUp() {
    internalDistributedSystem = mock(InternalDistributedSystem.class);
    dlock = mock(DLockService.class);
    distributionManager = mock(DistributionManager.class);
    distributedMember = mock(InternalDistributedMember.class);
    recoverylock = mock(StoppableReentrantReadWriteLock.class);
  }

  @Test
  public void testTxLockService() {
    distLocks = new ArrayList();
    txLockService = new TXLockServiceImpl(internalDistributedSystem, recoverylock, dlock);

    when(dlock.getDistributionManager()).thenReturn(distributionManager);
    when(dlock.getDistributionManager().getId()).thenReturn(distributedMember);

    assertThat((txLockService).getTxLockIdList()).isEqualTo(0);

    assertThrows(CommitConflictException.class,
        () -> txLockService.txLock(distLocks, otherMembers));

    assertThat((txLockService).getTxLockIdList()).isEqualTo(0);
  }
}
