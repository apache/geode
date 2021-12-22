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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Represents a held lock in a member for use when initializing a new grantor. All currently held
 * locks are represented as DLockRemoteTokens and provided in response to a
 * DLockRecoverGrantorMessage.
 *
 */
public class DLockRemoteToken implements DataSerializableFixedID {

  /**
   * Lock name for this lock.
   */
  private final Object name;

  /**
   * The reply processor id is used to identify the distinct lease which the lessee has used to
   * lease this lock.
   */
  private final int leaseId;

  /**
   * The absolute time at which the current lease on this lock will expire. -1 represents a lease
   * which will not expire until explicitly released.
   */
  private final long leaseExpireTime;

  /**
   * Serializable identity of thread currently leasing this lock.
   */
  private final RemoteThread lesseeThread;

  /**
   * Creates a new immutable instance of DLockRemoteToken representing the current lease tracked by
   * DLockToken. Synchronizes on the provided DLockToken to read data from it.
   *
   * @param token the lock token to gather lease information from
   * @return new immutable instance of DLockRemoteToken
   */
  public static DLockRemoteToken createFromDLockToken(DLockToken token) {
    synchronized (token) {
      return new DLockRemoteToken(token.getName(), token.getLesseeThread(), token.getLeaseId(),
          token.getLeaseExpireTime());
    }
  }

  /**
   * Creates a new immutable instance of DLockRemoteToken from the provided DataInput.
   *
   * @param in the input stream to gather state from
   * @return new immutable instance of DLockRemoteToken
   * @throws IOException if DataSerializer failed to read object from input stream
   * @throws ClassNotFoundException if DataSerializer failed to find class to read object from input
   */
  public static DLockRemoteToken createFromDataInput(DataInput in)
      throws IOException, ClassNotFoundException {
    Object name = DataSerializer.readObject(in);
    RemoteThread lesseeThread = null;
    InternalDistributedMember lessee = DataSerializer.readObject(in);
    lesseeThread = new RemoteThread(lessee, in.readInt());
    int leaseId = in.readInt();
    long leaseExpireTime = in.readLong();
    return new DLockRemoteToken(name, lesseeThread, leaseId, leaseExpireTime);
  }

  public static DLockRemoteToken create(Object name, RemoteThread lesseeThread, int leaseId,
      long leaseExpireTime) {
    return new DLockRemoteToken(name, lesseeThread, leaseId, leaseExpireTime);
  }

  /**
   * Unused no-arg constructor for Serializable. Instead use DataSerializable and
   * {@link DLockRemoteToken#createFromDataInput(DataInput)}.
   */
  public DLockRemoteToken() {
    throw new UnsupportedOperationException(
        "Use DLockRemoteToken#createFromDataInput(DataInput) instead.");
  }

  /**
   * Instantiates an immutable DLockRemoteToken.
   *
   * @param name the name of the lock
   * @param lesseeThread the remotable thread identity of the lease holder
   * @param leaseId used to identify the distinct lease used by the lease holder
   * @param leaseExpireTime the absolute time when this lease will expire
   */
  private DLockRemoteToken(Object name, RemoteThread lesseeThread, int leaseId,
      long leaseExpireTime) {
    this.name = name;
    this.lesseeThread = lesseeThread;
    this.leaseId = leaseId;
    this.leaseExpireTime = leaseExpireTime;
  }

  /**
   * Returns the identifying name of this lock.
   *
   * @return the identifying name of this lock
   */
  public Object getName() {
    return name;
  }

  /**
   * Returns the serializable identity of the thread currently leasing this lock or null if no
   * thread currently holds this lock.
   *
   * @return identity of the thread holding the current lease or null if none
   */
  public RemoteThread getLesseeThread() {
    return lesseeThread;
  }

  /**
   * Returns the member currently leasing this lock or null if no member curently holds this lock.
   *
   * @return member currently leasing this lock or null
   */
  public DistributedMember getLessee() {
    if (lesseeThread == null) {
      return null;
    } else {
      return lesseeThread.getDistributedMember();
    }
  }

  /**
   * Returns the lease id currently used to hold a lease on this lock or -1 if no thread currently
   * holds this lock.
   *
   * @return the id of the current lease on this lock or -1 if none
   */
  public int getLeaseId() {
    return leaseId;
  }

  /**
   * Returns the absolute time at which the current lease will expire or -1 if there is no lease.
   *
   * @return the absolute time at which the current lease will expire or -1
   */
  public long getLeaseExpireTime() {
    return leaseExpireTime;
  }

  /**
   * Returns a string representation of this object.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DLockRemoteToken@");
    sb.append(Integer.toHexString(hashCode()));
    sb.append(", name: ").append(name);
    sb.append(", lesseeThread: ").append(lesseeThread);
    sb.append(", leaseId: ").append(leaseId);
    sb.append(", leaseExpireTime: ").append(leaseExpireTime);
    return sb.toString();
  }

  /**
   * return the ID for serialization of instances of DLockRemoteToken
   */
  @Override
  public int getDSFID() {
    return DataSerializableFixedID.DLOCK_REMOTE_TOKEN;
  }

  /**
   * Writes the contents of this object to the given output.
   */
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    context.getSerializer().writeObject(name, out);
    context.getSerializer().writeObject(lesseeThread.getDistributedMember(), out);
    out.writeInt(lesseeThread.getThreadId());
    out.writeInt(leaseId);
    out.writeLong(leaseExpireTime);
  }

  /**
   * Unsupported. Use {@link DLockRemoteToken#createFromDataInput(DataInput)} instead.
   */
  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    throw new UnsupportedOperationException(
        "Use DLockRemoteToken#createFromDataInput(DataInput) instead.");
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
