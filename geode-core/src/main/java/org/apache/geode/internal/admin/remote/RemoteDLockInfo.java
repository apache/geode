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


package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.locks.DLockToken;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.DLockInfo;

public class RemoteDLockInfo implements DLockInfo, DataSerializable {
  private static final long serialVersionUID = 3350265007784675017L;
  private String serviceName;
  private String threadId;
  private String lockName;
  private boolean acquired;
  private int recursion;
  private InternalDistributedMember owner;
  private long startTime;
  private long leaseExpiration;
  private transient Date expirationDate;

  public RemoteDLockInfo(String serviceName, String name, DLockToken lock,
      InternalDistributedMember localId) {
    this.serviceName = serviceName;
    lockName = name;
    synchronized (lock) {
      threadId = lock.getThreadName();
      acquired = threadId != null;
      if (acquired) {
        owner = localId;
      }
      recursion = lock.getRecursion();
      leaseExpiration = lock.getLeaseExpireTime();
    }
  }

  /**
   * for DataExternalizable only
   */
  public RemoteDLockInfo() {}

  @Override
  public String getService() {
    return serviceName;
  }

  @Override
  public String getThreadId() {
    return threadId;
  }

  @Override
  public String getLockName() {
    return lockName;
  }

  @Override
  public boolean isAcquired() {
    return acquired;
  }

  @Override
  public int getRecursionCount() {
    return recursion;
  }

  @Override
  public InternalDistributedMember getOwner() {
    return owner;
  }

  public long getStartTime() {
    return startTime;
  }

  @Override
  public synchronized Date getLeaseExpireTime() {
    if (expirationDate == null && leaseExpiration > -1) {
      expirationDate = new Date(leaseExpiration);
    }
    return expirationDate;

  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(serviceName, out);
    DataSerializer.writeString(threadId, out);
    DataSerializer.writeString(lockName, out);
    out.writeBoolean(acquired);
    out.writeInt(recursion);
    DataSerializer.writeObject(owner, out);
    out.writeLong(startTime);
    out.writeLong(leaseExpiration);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    serviceName = DataSerializer.readString(in);
    threadId = DataSerializer.readString(in);
    lockName = DataSerializer.readString(in);
    acquired = in.readBoolean();
    recursion = in.readInt();
    owner = DataSerializer.readObject(in);
    startTime = in.readLong();
    leaseExpiration = in.readLong();
  }

}
