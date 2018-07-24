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
package org.apache.geode.internal.cache.persistence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.cache.versions.VersionSource;

public class PersistentMemberID implements DataSerializable {
  private static final long serialVersionUID = 7037022320499508045L;

  private InetAddress host;
  private String directory;
  private long timeStamp;
  private short version;
  private DiskStoreID diskStoreId;
  private String name;

  public PersistentMemberID() {

  }

  public PersistentMemberID(DiskStoreID diskStoreId, InetAddress host, String directory,
      String name, long timeStamp, short version) {
    this.diskStoreId = diskStoreId;
    this.host = host;
    this.directory = directory;
    this.name = name;
    this.timeStamp = timeStamp;
    this.version = version;
  }

  public PersistentMemberID(DiskStoreID diskStoreId, InetAddress host, String directory,
      long timeStamp, short version) {
    this.diskStoreId = diskStoreId;
    this.host = host;
    this.directory = directory;
    this.timeStamp = timeStamp;
    this.version = version;
  }

  /**
   * Indicate that this persistent member id is from the same disk store, and is older than, or
   * equal to, the passed in disk store.
   *
   * This is an older version if it has an older timestamp, or the same timestamp and an older
   * version
   */
  public boolean isOlderOrEqualVersionOf(PersistentMemberID id) {
    return (id != null) && (diskStoreId.equals(id.diskStoreId))
        && (timeStamp <= id.timeStamp && (timeStamp < id.timeStamp || version <= id.version));
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    long diskStoreIdHigh = in.readLong();
    long diskStoreIdLow = in.readLong();
    this.diskStoreId = new DiskStoreID(diskStoreIdHigh, diskStoreIdLow);
    this.host = DataSerializer.readInetAddress(in);
    this.directory = DataSerializer.readString(in);
    this.timeStamp = in.readLong();
    this.version = in.readShort();
    this.name = DataSerializer.readString(in);
  }

  public void _fromData662(DataInput in) throws IOException, ClassNotFoundException {
    long diskStoreIdHigh = in.readLong();
    long diskStoreIdLow = in.readLong();
    this.diskStoreId = new DiskStoreID(diskStoreIdHigh, diskStoreIdLow);
    this.host = DataSerializer.readInetAddress(in);
    this.directory = DataSerializer.readString(in);
    this.timeStamp = in.readLong();
    this.version = in.readShort();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeLong(this.diskStoreId.getMostSignificantBits());
    out.writeLong(this.diskStoreId.getLeastSignificantBits());
    DataSerializer.writeInetAddress(host, out);
    DataSerializer.writeString(directory, out);
    out.writeLong(timeStamp);
    out.writeShort(version);
    DataSerializer.writeString(this.name, out);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((diskStoreId == null) ? 0 : diskStoreId.hashCode());
    result = prime * result + ((directory == null) ? 0 : directory.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((host == null) ? 0 : host.hashCode());
    result = prime * result + (int) (timeStamp ^ (timeStamp >>> 32));
    result = prime * result + version;
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
    PersistentMemberID other = (PersistentMemberID) obj;
    if (directory == null) {
      if (other.directory != null)
        return false;
    } else if (!directory.equals(other.directory))
      return false;
    if (diskStoreId == null) {
      if (other.diskStoreId != null)
        return false;
    } else if (!diskStoreId.equals(other.diskStoreId))
      return false;
    if (host == null) {
      if (other.host != null)
        return false;
    } else if (!host.equals(other.host))
      return false;
    if (timeStamp != other.timeStamp)
      return false;
    if (version != other.version)
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;

    return true;
  }

  @Override
  public String toString() {
    return host + ":" + directory + " created at timestamp " + timeStamp + " version " + version
        + " diskStoreId " + diskStoreId + " name " + name;
  }

  public String abbrev() {
    return diskStoreId.abbrev();
  }

  public VersionSource getVersionMember() {
    return this.diskStoreId;
  }

  public InetAddress getHost() {
    return host;
  }

  public String getDirectory() {
    return directory;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public short getVersion() {
    return version;
  }

  public DiskStoreID getDiskStoreId() {
    return diskStoreId;
  }

  public String getName() {
    return name;
  }
}
