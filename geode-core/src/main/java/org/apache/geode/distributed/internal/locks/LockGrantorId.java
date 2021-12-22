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

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Identifies specific lock grantor member and version.
 *
 * @since GemFire 5.1
 */
public class LockGrantorId {

  public static final int ROLLOVER_MARGIN = Integer
      .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "DLockService.LockGrantorId.rolloverMargin",
          10000);

  private final DistributionManager dm;
  private final InternalDistributedMember lockGrantorMember;
  private final long lockGrantorVersion;
  private final int lockGrantorSerialNumber;

  /**
   * Constructs a new instance to identify a specific lock grantor member and version.
   *
   * @param dm the distribution manager which is used by {@link #isLocal()}
   * @param lockGrantorMember the non-null member hosting the grantor
   * @param lockGrantorVersion the long grantor version number
   */
  public LockGrantorId(DistributionManager dm, InternalDistributedMember lockGrantorMember,
      long lockGrantorVersion, int lockGrantorSerialNumber) {
    if (lockGrantorMember == null) {
      throw new NullPointerException(
          "lockGrantorMember is null");
    }
    this.dm = dm;
    this.lockGrantorMember = lockGrantorMember;
    this.lockGrantorVersion = lockGrantorVersion;
    this.lockGrantorSerialNumber = lockGrantorSerialNumber;
  }

  /**
   * Returns the non-null member hosting the grantor.
   *
   * @return the member hosting the grantor
   */
  public InternalDistributedMember getLockGrantorMember() {
    return lockGrantorMember;
  }

  /**
   * Returns the long grantor version number. A given member may host grantor several times during
   * its life and this version number will be greater for later grantor instances.
   *
   * @return the long grantor version number
   */
  public long getLockGrantorVersion() {
    return lockGrantorVersion;
  }

  /**
   * Returns the DLS serial number of the lock service that is hosting the grantor.
   *
   * @return the grantor's DLS serial number
   */
  public int getLockGrantorSerialNumber() {
    return lockGrantorSerialNumber;
  }

  /**
   * Returns true if the grantor version number is positive.
   *
   * @return true if the grantor version number is positive
   */
  public boolean hasLockGrantorVersion() {
    return lockGrantorVersion > -1;
  }

  /**
   * Returns true if <code>otherLockGrantorId</code> is same as this instance. Returns false if
   * different or if <code>otherLockGrantorId</code> is null.
   *
   * @param otherLockGrantorId the other instance to compare this instance to
   * @return true if <code>otherLockGrantorId</code> is same
   */
  public boolean sameAs(LockGrantorId otherLockGrantorId) {
    if (otherLockGrantorId == null) {
      return false;
    }
    return sameAs(otherLockGrantorId.lockGrantorMember, otherLockGrantorId.lockGrantorVersion,
        otherLockGrantorId.lockGrantorSerialNumber);
  }

  /**
   * Returns true if this instance represents a newer lock grantor version than
   * <code>otherLockGrantorId</code>. Returns true if <code>otherLockGrantorId</code> is null.
   *
   * @param otherLockGrantorId the other lock grantor id to compare to
   * @return true if this instance represents a newer lock grantor version
   */
  public boolean isNewerThan(LockGrantorId otherLockGrantorId) {
    if (otherLockGrantorId == null) {
      return true;
    }
    boolean isNewer = lockGrantorVersion > otherLockGrantorId.getLockGrantorVersion();
    if (!isNewer && lockGrantorMember.equals(otherLockGrantorId.getLockGrantorMember())) {
      int otherGrantorSerialNumber = otherLockGrantorId.getLockGrantorSerialNumber();
      boolean serialRolled =
          lockGrantorSerialNumber > ROLLOVER_MARGIN && otherGrantorSerialNumber < 0;
      isNewer = serialRolled || lockGrantorSerialNumber > otherGrantorSerialNumber;
    }

    return isNewer;
  }

  /**
   * Returns true if this instance represents the same lock grantor member and version
   *
   * @param someLockGrantorMember the lock grantor member
   * @param someLockGrantorVersion the lock grantor version
   * @return true if <code>otherLockGrantorId</code> is same
   */
  public boolean sameAs(InternalDistributedMember someLockGrantorMember,
      long someLockGrantorVersion, int someLockGrantorSerialNumber) {
    if (someLockGrantorMember == null) {
      throw new IllegalStateException(
          "someLockGrantorId must not be null");
    }
    return someLockGrantorMember.equals(lockGrantorMember)
        && someLockGrantorVersion == lockGrantorVersion
        && someLockGrantorSerialNumber == lockGrantorSerialNumber;
  }

  /**
   * Returns true if this instance represents a local lock grantor.
   *
   * @return true if this instance represents a local lock grantor
   */
  public boolean isLocal() {
    return dm.getId().equals(lockGrantorMember);
  }

  /**
   * Returns true if this instance represents a local lock grantor with the specified DLS serial
   * number
   *
   * @return ture if local grantor with matching serial number
   */
  public boolean isLocal(int dlsSerialNumber) {
    return lockGrantorSerialNumber == dlsSerialNumber
        && dm.getId().equals(lockGrantorMember);
  }

  /**
   * Returns true if this instance represents a remote lock grantor.
   *
   * @return true if this instance represents a remote lock grantor
   */
  public boolean isRemote() {
    return !dm.getId().equals(lockGrantorMember);
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("[LockGrantorId: ");
    sb.append("lockGrantorMember=").append(lockGrantorMember);
    sb.append(", lockGrantorVersion=").append(lockGrantorVersion);
    sb.append(", lockGrantorSerialNumber=").append(lockGrantorSerialNumber);
    sb.append("]");
    return sb.toString();
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * @param other the reference object with which to compare.
   * @return true if this object is the same as the obj argument; false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (!(other instanceof LockGrantorId)) {
      return false;
    }
    final LockGrantorId that = (LockGrantorId) other;

    if (lockGrantorMember != that.lockGrantorMember && !(lockGrantorMember != null
        && lockGrantorMember.equals(that.lockGrantorMember))) {
      return false;
    }
    if (lockGrantorVersion != that.lockGrantorVersion) {
      return false;
    }
    return lockGrantorSerialNumber == that.lockGrantorSerialNumber;
  }

  /**
   * Returns a hash code for the object. This method is supported for the benefit of hashtables such
   * as those provided by java.util.Hashtable.
   *
   * @return the integer 0 if description is null; otherwise a unique integer.
   */
  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;

    result =
        mult * result + (lockGrantorMember == null ? 0 : lockGrantorMember.hashCode());
    result = mult * result + (int) (lockGrantorVersion ^ (lockGrantorVersion >>> 32));
    result = mult * result + lockGrantorSerialNumber;

    return result;
  }
}
