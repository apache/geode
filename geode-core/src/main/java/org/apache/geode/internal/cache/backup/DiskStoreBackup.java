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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.internal.cache.Oplog;

/**
 * This class manages the state of the backup of an individual disk store. It holds the list of
 * oplogs that still need to be backed up, along with the lists of oplog files that should be
 * deleted when the oplog is backed up. See
 */
public class DiskStoreBackup {

  private final Set<Oplog> pendingBackup;
  private final Set<Oplog> deferredCrfDeletes = new HashSet<>();
  private final Set<Oplog> deferredDrfDeletes = new HashSet<>();

  DiskStoreBackup(Oplog[] allOplogs) {
    pendingBackup = new HashSet<>(Arrays.asList(allOplogs));
  }

  /**
   * Add the oplog to the list of deferred deletes.
   *
   * @return true if the delete has been deferred. False if this oplog should be deleted
   *         immediately.
   */
  synchronized boolean deferCrfDelete(Oplog oplog) {
    if (pendingBackup.contains(oplog)) {
      deferredCrfDeletes.add(oplog);
      return true;
    }

    return false;
  }

  /**
   * Add the oplog to the list of deferred deletes.
   *
   * @return true if the delete has been deferred. False if this oplog should be deleted
   *         immediately.
   */
  synchronized boolean deferDrfDelete(Oplog oplog) {
    if (pendingBackup.contains(oplog)) {
      deferredDrfDeletes.add(oplog);
      return true;
    }

    return false;
  }

  synchronized Set<Oplog> getPendingBackup() {
    return new HashSet<>(pendingBackup);
  }

  synchronized void backupFinished(Oplog oplog) {
    pendingBackup.remove(oplog);
    if (deferredCrfDeletes.remove(oplog)) {
      oplog.deleteCRFFileOnly();
    }
    if (deferredDrfDeletes.remove(oplog)) {
      oplog.deleteDRFFileOnly();
    }
  }

  synchronized void cleanup() {
    for (Oplog oplog : getPendingBackup()) {
      backupFinished(oplog);
    }
  }
}
