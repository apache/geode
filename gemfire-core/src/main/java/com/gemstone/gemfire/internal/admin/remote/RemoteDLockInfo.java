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
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.admin.*;
//import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.locks.*;
//import com.gemstone.gemfire.internal.*;
import java.io.*;
import java.util.Date;
import com.gemstone.gemfire.distributed.internal.membership.*;

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

  public RemoteDLockInfo(String serviceName, String name,
                         DLockToken lock, InternalDistributedMember localId) {
    this.serviceName = serviceName;
    this.lockName = name;
    synchronized (lock) {
      this.threadId = lock.getThreadName();
      this.acquired = this.threadId != null;
      if (this.acquired) {
        this.owner = localId;
      }
      this.recursion = lock.getRecursion();
      this.leaseExpiration = lock.getLeaseExpireTime();
    }
  }

  /**
   * for DataExternalizable only
   */
  public RemoteDLockInfo(){}

  public String getService() {
    return serviceName;
  }
  public String getThreadId() {
    return threadId;
  }
  public String getLockName() {
    return lockName;
  }
  public boolean isAcquired() {
    return acquired;
  }  
  public int getRecursionCount() {
    return recursion;
  }
  public InternalDistributedMember getOwner() {
    return owner;
  }
  public long getStartTime() {
    return startTime;
  }
  public synchronized Date getLeaseExpireTime() {    
    if (expirationDate == null && leaseExpiration > -1) {
      expirationDate = new Date(leaseExpiration);
    }
    return expirationDate;

  }

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

  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    this.serviceName = DataSerializer.readString(in);
    this.threadId = DataSerializer.readString(in);
    this.lockName = DataSerializer.readString(in);
    this.acquired = in.readBoolean();
    this.recursion = in.readInt();
    this.owner = (InternalDistributedMember)DataSerializer.readObject(in);
    this.startTime = in.readLong();
    this.leaseExpiration = in.readLong();
  }

}
