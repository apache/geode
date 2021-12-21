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

package org.apache.geode.distributed.internal.locks;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * Used to provide information on a grantor request made to the elder.
 *
 * @since GemFire 4.0
 */
public class GrantorInfo {
  private final InternalDistributedMember id;
  private final boolean needsRecovery;
  private final long versionId;
  private boolean initiatingTransfer;
  private final int serialNumber;

  public GrantorInfo(InternalDistributedMember id, long versionId, int serialNumber,
      boolean needsRecovery) {
    this.id = id;
    this.needsRecovery = needsRecovery;
    this.versionId = versionId;
    this.serialNumber = serialNumber;
  }

  /** Caller is sync'ed on ElderState */
  public void setInitiatingTransfer(boolean initiatingTransfer) {
    this.initiatingTransfer = initiatingTransfer;
  }

  /** Caller is sync'ed on ElderState */
  public boolean isInitiatingTransfer() {
    return initiatingTransfer;
  }

  /**
   * Gets the member id of this grantor.
   */
  public InternalDistributedMember getId() {
    return id;
  }

  /**
   * Returns true if the current grantor needs to do lock recovery.
   */
  public boolean needsRecovery() {
    return needsRecovery;
  }

  /**
   * Returns the elder version id of this grantor.
   */
  public long getVersionId() {
    return versionId;
  }

  /**
   * Returns the DLockService serial number of this grantor.
   */
  public int getSerialNumber() {
    return serialNumber;
  }

  /** Returns human readable String version of this object. */
  @Override
  public String toString() {
    return "<GrantorInfo id=" + id + " versionId=" + versionId + " serialNumber="
        + serialNumber + " needsRecovery=" + needsRecovery + " initiatingTransfer="
        + initiatingTransfer + ">";
  }

}
