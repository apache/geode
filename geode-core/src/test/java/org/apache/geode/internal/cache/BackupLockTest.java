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
package org.apache.geode.internal.cache;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class BackupLockTest {

  private BackupLock backupLock;
  private ExecutorService executor;

  @Before
  public void setUp() throws Exception {
    backupLock = new BackupLock();
    executor = Executors.newSingleThreadExecutor();
  }

  @Test
  public void lockShouldBlockUntilLockForBackup() throws Exception {
    backupLock.lockForBackup();
    backupLock.setBackupThread();

    AtomicBoolean beforeLock = new AtomicBoolean();
    AtomicBoolean afterLock = new AtomicBoolean();

    backupLock.setBackupLockTestHook(() -> beforeLock.set(true));

    executor.submit(() -> {
      backupLock.lock(); // beforeLock is set inside lock() method
      afterLock.set(true);
    });

    await().atMost(10, SECONDS).until(() -> assertThat(beforeLock).isTrue());
    assertThat(afterLock).isFalse();

    backupLock.unlockForBackup();
    await().atMost(10, SECONDS).until(() -> assertThat(afterLock).isTrue());
  }

  @Test
  public void otherThreadShouldBeAbleToUnlockForBackup() throws Exception {
    backupLock.lockForBackup();
    backupLock.setBackupThread();

    await().atMost(10, SECONDS).until(() -> assertThat(backupLock.isBackingUp()).isTrue());
    assertThat(backupLock.isCurrentThreadDoingBackup()).isTrue();

    executor.submit(() -> {
      assertThat(backupLock.isCurrentThreadDoingBackup()).isFalse();
      backupLock.unlockForBackup();
    });

    await().atMost(10, SECONDS).until(() -> assertThat(backupLock.isBackingUp()).isFalse());
  }

  @Test
  public void isCurrentThreadDoingBackupShouldBeSetAndUnset() throws Exception {
    backupLock.lockForBackup();
    backupLock.setBackupThread();

    assertThat(backupLock.isCurrentThreadDoingBackup()).isTrue();

    backupLock.unlockForBackup();

    assertThat(backupLock.isCurrentThreadDoingBackup()).isFalse();
  }

  @Test
  public void threadLocalShouldNotLeak() throws Exception {
    backupLock.lockForBackup();
    backupLock.setBackupThread();

    assertThat(backupLock.hasThreadLocal()).isTrue();

    backupLock.unlockForBackup();

    assertThat(backupLock.hasThreadLocal()).isFalse();
  }

}
