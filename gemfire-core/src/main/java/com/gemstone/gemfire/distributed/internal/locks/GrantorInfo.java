/*=========================================================================
 * Copyright (c) 2003-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.distributed.internal.locks;

import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Used to provide information on a grantor request made to the elder.
 *
 * @since 4.0
 * @author Darrel Schneider
 */
public class GrantorInfo {
  private final InternalDistributedMember id;
  private final boolean needsRecovery;
  private final long versionId;
  private boolean initiatingTransfer;
  private final int serialNumber;
    
  public GrantorInfo(InternalDistributedMember id, long versionId, int serialNumber, boolean needsRecovery) {
    this.id = id;
    this.needsRecovery = needsRecovery;
    this.versionId = versionId;
    this.serialNumber = serialNumber;
  }
  
  /** Caller is sync'ed on ElderState  */
  public final void setInitiatingTransfer(boolean initiatingTransfer) {
    this.initiatingTransfer = initiatingTransfer;
  }
  
  /** Caller is sync'ed on ElderState  */
  public final boolean isInitiatingTransfer() {
    return this.initiatingTransfer;
  }
  
  /**
   * Gets the member id of this grantor.
   */
  public final InternalDistributedMember getId() {
    return this.id;
  }
  /**
   * Returns true if the current grantor needs to do lock recovery.
   */
  public final boolean needsRecovery() {
    return this.needsRecovery;
  }
  /**
   * Returns the elder version id of this grantor.
   */
  public final long getVersionId() {
    return this.versionId;
  }
  /**
   * Returns the DLockService serial number of this grantor.
   */
  public final int getSerialNumber() {
    return this.serialNumber;
  }
  
  /** Returns human readable String version of this object. */
  @Override
  public String toString() {
    return "<GrantorInfo id=" + this.id + 
           " versionId=" + this.versionId +
           " serialNumber=" + this.serialNumber +
           " needsRecovery=" + this.needsRecovery + 
           " initiatingTransfer=" + this.initiatingTransfer + ">";
  }
  
}
