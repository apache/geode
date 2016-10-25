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

package org.apache.geode.internal.cache;
import java.io.*;

import org.apache.geode.internal.ExternalizableDSFID;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * Stores information about a PartitionedRegion singleton instance running
 * inside a virtual machine. This Node is said to be a participant, cooperating
 * with other Nodes to physically manage a logically named PartitionedRegion.
 * <p>
 * Nodes are stored in a list within PartitionedRegionConfig which is stored
 * globally as the value of an named entry in the
 * <code>DistributedHashMap.PARTITIONED_REGION_NAME</code>. Node must be
 * Serializable and have a no args Constructor. Since Nodes are used as keys in
 * each Thread's ThreadLocal map of Nodes to Connections, Node must implement
 * hashCode and equals.
 * <p>
 * Node maintains a field for maxMemory (fixed) to enable other Nodes to
 * determine whether the Node should be the target of a canRebalance request. If
 * the Node's maxMemory is 0, then the Node is advertising that it never should
 * receive a request to rebalance.
 * 
 */
public final class Node extends ExternalizableDSFID
{
  private InternalDistributedMember memberId;
  
  public static final int NONE = 0;
  public static final int ACCESSOR = 1;
  public static final int DATASTORE = 2;
  public static final int ACCESSOR_DATASTORE = 3;
  public static final int FIXED_PR_ACCESSOR = 4;
  public static final int FIXED_PR_DATASTORE = 5;
  
  private int prType = Node.NONE;
  
  private boolean isPersistent = false;

  private byte cacheLoaderWriterByte;
  
  private int serialNumber;

  public Node(InternalDistributedMember id, int serialNumber) {
    this.memberId = id;
    this.serialNumber = serialNumber;
  }
  
  public Node(Node node) {
    this.memberId = node.getMemberId();
    this.serialNumber = node.serialNumber;
  }
  
  public Node(DataInput in) throws IOException, ClassNotFoundException {
    fromData(in);
  }

  // for Externalizable
  public Node() {
  }
  
  public InternalDistributedMember getMemberId()
  {
    return this.memberId;
  }

  public boolean isPersistent() {
    return this.isPersistent;
  }
  
  public void setPersistence(boolean isPersistent) {
    this.isPersistent = isPersistent;
  }
  
  @Override  
  public String toString()
  {
    return ("Node=[memberId=" + this.memberId + "; prType=" + prType + "; isPersistent="+ isPersistent+"]");
  }

  @Override  
  public int hashCode()
  {
    return this.memberId.hashCode() + 31 * serialNumber;
  }

  public int getPRType() {
    return prType;
  }
  
  public void setPRType(int type) {
    this.prType = type;
  }

  public void setLoaderWriterByte(byte b) {
    this.cacheLoaderWriterByte = b;
  }
  

  public boolean isCacheLoaderAttached() {
    if (this.cacheLoaderWriterByte == 0x01
        || this.cacheLoaderWriterByte == 0x03) {
      return true;
    }
    else {
      return false;
    }
  }

  public boolean isCacheWriterAttached() {
    if (this.cacheLoaderWriterByte == 0x02
        || this.cacheLoaderWriterByte == 0x03) {
      return true;
    }
    else {
      return false;
    }
  }
  
  @Override  
  public boolean equals(Object obj)
  {
    if (obj == this) {
      return true;
    }
    if (obj instanceof Node) {
      Node n = (Node)obj;
      if (this.memberId.equals(n.memberId)
          && this.serialNumber == n.serialNumber) {
        return true;
      }
    }
    return false;
  }

  @Override  
  public int getDSFID() {
    return PR_NODE;
  }

  @Override  
  public void toData(DataOutput out) throws IOException
  {
    InternalDataSerializer.invokeToData(this.memberId, out);
    out.writeInt(this.prType);
    out.writeBoolean(this.isPersistent);
    out.writeByte(this.cacheLoaderWriterByte);
    out.writeInt(serialNumber);
  }

  @Override  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    this.memberId = new InternalDistributedMember();
    InternalDataSerializer.invokeFromData(this.memberId, in);
    this.prType = in.readInt();
    this.isPersistent = in.readBoolean();
    this.cacheLoaderWriterByte = in.readByte();
    this.serialNumber = in.readInt();
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
