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
package org.apache.geode.internal.cache.backup;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A lock used for the backup process. This is a reentrant lock that provides a "backup" mode, where
 * the lock is held by a "backup thread" which can be assigned later than the time we lock.
 *
 * <p>
 * We need this because our backup process is two phase. In the first phase we acquire the lock and
 * in the second phase we actually do the backup. During the second phase we need to reenter the
 * lock and release it with a different thread.
 */
public class BackupLock extends ReentrantLock {

  private final ThreadLocal<Boolean> isBackupThread = new ThreadLocal<>();
  private boolean isBackingUp;
  private Condition backupDone = super.newCondition();

  // test hook
  private final AtomicReference<BackupLockTestHook> hook = new AtomicReference<>();

  public interface BackupLockTestHook {
    /**
     * Test hook called before the wait for backup to complete
     */
    void beforeWaitForBackupCompletion();
  }

  public void setBackupLockTestHook(BackupLockTestHook testHook) {
    hook.set(testHook);
  }

  public void lockForBackup() {
    super.lock();
    isBackingUp = true;
    super.unlock();
  }

  void setBackupThread() {
    isBackupThread.set(true);
  }

  public void unlockForBackup() {
    super.lock();
    isBackingUp = false;
    isBackupThread.remove();
    backupDone.signalAll();
    super.unlock();
  }

  boolean isCurrentThreadDoingBackup() {
    Boolean result = isBackupThread.get();
    return (result != null) && result;
  }

  /**
   * For testing only
   */
  boolean isBackingUp() {
    return isBackingUp;
  }

  /**
   * For testing only
   */
  boolean hasThreadLocal() {
    return isBackupThread.get() != null;
  }

  @Override
  public void unlock() {
    // The backup thread does not need to unlock this lock since it never gets the lock. It is the
    // only thread that has permission to modify disk files during backup.
    if (!isCurrentThreadDoingBackup()) {
      super.unlock();
    }
  }

  /**
   * Acquire this lock, waiting for a backup to finish the first phase.
   */
  @Override
  public void lock() {
    // The backup thread is a noop; it does not need to get the lock since it is the only thread
    // with permission to modify disk files during backup
    if (!isCurrentThreadDoingBackup()) {
      super.lock();
      while (isBackingUp) {
        BackupLockTestHook testHook = hook.get();
        if (testHook != null) {
          testHook.beforeWaitForBackupCompletion();
        }
        backupDone.awaitUninterruptibly();
      }
    }
  }
}
