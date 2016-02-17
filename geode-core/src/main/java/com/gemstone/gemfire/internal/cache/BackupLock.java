/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A lock used for the backup process. This
 * is a reentrant lock that provides a "backup"
 * mode, where the lock is held by a "backup thread" which
 * can be assigned later than the time we lock.
 * 
 * We need this because our backup process
 * is two phase. In the first phase we aquire the lock
 * and in the second phase we actually do the backup. During
 * the second phase we need to reenter the lock and release
 * it with a different thread.
 *
 */
public class BackupLock extends ReentrantLock {
  
  private Thread backupThread;
  boolean isBackingUp;
  Condition backupDone = super.newCondition();
  
  public void lockForBackup() {
    super.lock();
    isBackingUp = true;
    super.unlock();
  }
  
  public void setBackupThread(Thread thread) {
    super.lock();
    backupThread = thread;
    super.unlock();
  }
  
  public void unlockForBackup() {
    super.lock();
    isBackingUp = false;
    backupThread = null;
    backupDone.signalAll();
    super.unlock();
  }

  /**
   * Acquire this lock, waiting for an in progress backup
   * if one is in progress.
   */
  @Override
  public void lock() {
    lock(true);
  }
  
  /**
   * Acquire this lock, Optionally waiting for a backup to finish the first
   * phase. Any operations that update metadata related to the distributed
   * system state should pass true for this flag, because we need to make sure
   * we get a point in time snapshot of the init files across members to for
   * metadata consistentency.
   * 
   * Updates which update only record changes to the local state on this
   * member(eg, switching oplogs), do not need to wait for the backup.
   * 
   * @param waitForBackup
   *          if true, we will wait for an in progress backup before acquiring
   *          this lock.
   */
  public void lock(boolean waitForBackup) {
    super.lock();
    while(isBackingUp && waitForBackup && !(Thread.currentThread() == backupThread)) {
      backupDone.awaitUninterruptibly();
    }
  }
}
