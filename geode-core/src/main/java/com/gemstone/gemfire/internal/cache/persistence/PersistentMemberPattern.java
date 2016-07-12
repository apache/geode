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
package com.gemstone.gemfire.internal.cache.persistence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.internal.net.SocketCreator;

/**
 * Implementation of the public PersistentID. It holds the region,
 * host, directory, and timestamp.
 * 
 * This class also is also used to describe members that the user
 * has revoked. Any fields that are null will be considered a wildcard
 * matching any members.
 * 
 * @since GemFire prPersistSprint1
 */
public class PersistentMemberPattern implements PersistentID, Comparable<PersistentMemberPattern> {
  protected InetAddress host;
  protected String directory;
  protected UUID diskStoreID;
  protected long revokedTime;
  
  public PersistentMemberPattern(PersistentMemberID id) {
    this(id.host, id.directory, id.diskStoreId.toUUID(), -1);
  }
  
  public PersistentMemberPattern(InetAddress host,
      String directory) {
    this(host, directory, null, -1);
  }
  
  public PersistentMemberPattern(InetAddress host,
      String directory, long revokedTime) {
    this(host, directory, null, revokedTime);
  }
  
  public PersistentMemberPattern(UUID id) {
    this(null, null, id, -1);
  }
  
  public PersistentMemberPattern(InetAddress host,
      String directory, UUID diskStoreID, long revokedTime) {
    this.host = host;
    this.directory = directory;
    this.revokedTime = revokedTime;
    this.diskStoreID = diskStoreID;
  }

  //Used for deserialization only
  public PersistentMemberPattern() {
  }

  public boolean matches(PersistentMemberID id) {
    boolean matches = true;
    if(id == null) {
      return false;
    }
    matches &= host == null || host.equals(id.host);
    matches &= directory == null || directory.equals(id.directory);
    matches &= diskStoreID == null
        || id.diskStoreId.getMostSignificantBits() == diskStoreID
            .getMostSignificantBits()
        && id.diskStoreId.getLeastSignificantBits() == diskStoreID
            .getLeastSignificantBits();
    
    //Safety measure. Id's which are generated after this pattern was revoked
    //should not be revoked. For example, if someone loses the disk for server A
    //They may revoke the pattern host==A. But A will start and generate a new ID
    //if they then close the region everywhere and then reopen it, we don't want
    //the new pattern to be revoked.
    if(diskStoreID == null) {
      matches &= revokedTime > id.timeStamp;
    }
    return matches;
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(diskStoreID);
    if(host != null) {
      result.append(" [");
      result.append(SocketCreator.getHostName(host));
      result.append(":");
      result.append(directory);
      result.append("]");
    }
    
    return result.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((directory == null) ? 0 : directory.hashCode());
    result = prime * result
        + ((diskStoreID == null) ? 0 : diskStoreID.hashCode());
    result = prime * result + ((host == null) ? 0 : host.hashCode());
    result = prime * result + (int) (revokedTime ^ (revokedTime >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PersistentMemberPattern other = (PersistentMemberPattern) obj;
    if (directory == null) {
      if (other.directory != null)
        return false;
    } else if (!directory.equals(other.directory))
      return false;
    if (diskStoreID == null) {
      if (other.diskStoreID != null)
        return false;
    } else if (!diskStoreID.equals(other.diskStoreID))
      return false;
    if (host == null) {
      if (other.host != null)
        return false;
    } else if (!host.equals(other.host))
      return false;
    if (revokedTime != other.revokedTime)
      return false;
    return true;
  }

  public InetAddress getHost() {
    return host;
  }

  public String getDirectory() {
    return directory;
  }
  
  public UUID getUUID() {
    return diskStoreID;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    boolean hasHost = in.readBoolean();
    if(hasHost) {
      host = DataSerializer.readInetAddress(in);
    }
    boolean hasDirectory = in.readBoolean();
    if(hasDirectory) {
      directory = DataSerializer.readString(in);
    }
    diskStoreID = DataSerializer.readObject(in);
    revokedTime = in.readLong();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeBoolean(host != null);
    if(host != null) {
      DataSerializer.writeInetAddress(host, out);
    }
    out.writeBoolean(directory != null);
    if(directory != null) {
      DataSerializer.writeString(directory, out);
    }
      
    DataSerializer.writeObject(diskStoreID, out);
    out.writeLong(revokedTime);
  }

  public int compareTo(PersistentMemberPattern o) {
    int result = compare(diskStoreID, o.diskStoreID);
    if(result != 0) {
      return result;
    }
    result = compare(host == null ? null : host.getCanonicalHostName(),
        o.host == null ? null : o.host.getCanonicalHostName());
    if(result != 0) {
      return result;
    }
    result = compare(directory, o.directory);
    if(result != 0) {
      return result;
    }
    
    result = Long.signum(revokedTime - o.revokedTime);
    
    return result;
  }
  
  private <X> int compare(Comparable<X> a, X b) {
    if(a == null) {
      if(b == null) {
        return 0;
      } else {
        return -1;
      }
    }
    if(b == null) {
      return 1;
    }
    
    return a.compareTo(b);
  }
}
