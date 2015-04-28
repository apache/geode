/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.internal.cache.persistence.BackupInspector;

/**
 * This class manages the state of the backup of an individual disk store. It
 * holds the list of oplogs that still need to be backed up, along with the
 * lists of oplog files that should be deleted when the oplog is backed up. See
 * {@link DiskStoreImpl#startBackup(File, BackupInspector, com.gemstone.gemfire.internal.cache.persistence.RestoreScript)}
 * 
 * @author dsmith
 * 
 */
public class DiskStoreBackup {
  
  private final Set<Oplog> pendingBackup;
  private final Set<Oplog> deferredCrfDeletes = new HashSet<Oplog>();
  private final Set<Oplog> deferredDrfDeletes = new HashSet<Oplog>();
  private final File targetDir;
  
  public DiskStoreBackup(Oplog[] allOplogs, File targetDir) {
    this.pendingBackup = new HashSet<Oplog>(Arrays.asList(allOplogs));
    this.targetDir = targetDir;
  }
  
  /**
   * Add the oplog to the list of deferred deletes.
   * @return true if the delete has been deferred. False if this
   * oplog should be deleted immediately.
   */
  public synchronized boolean deferCrfDelete(Oplog oplog) {
    if(pendingBackup.contains(oplog)) {
      deferredCrfDeletes.add(oplog);
      return true;
    }
    
    return false;
  }

  /**
   * Add the oplog to the list of deferred deletes.
   * @return true if the delete has been deferred. False if this
   * oplog should be deleted immediately.
   */
  public synchronized boolean deferDrfDelete(Oplog oplog) {
    if(pendingBackup.contains(oplog)) {
      deferredDrfDeletes.add(oplog);
      return true;
    }
    
    return false;
  }
  
  public synchronized Set<Oplog> getPendingBackup() {
    return new HashSet<Oplog>(pendingBackup);
  }
  
  public synchronized void backupFinished(Oplog oplog) {
    pendingBackup.remove(oplog);
    if(deferredCrfDeletes.remove(oplog)) {
      oplog.deleteCRFFileOnly();
    }
    if(deferredDrfDeletes.remove(oplog)) {
      oplog.deleteDRFFileOnly();
    }
  }
  
  public File getTargetDir() {
    return targetDir;
  }

  public synchronized void cleanup() {
    for(Oplog oplog: getPendingBackup()) {
      backupFinished(oplog);
    }
  }
}
