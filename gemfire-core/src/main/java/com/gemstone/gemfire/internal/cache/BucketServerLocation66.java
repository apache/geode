/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

/**
 * Represents the {@link ServerLocation} of a {@link BucketRegion}
 * 
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * 
 * @since 6.5 
 */
@SuppressWarnings("serial")
public class BucketServerLocation66 extends ServerLocation {
  
  private byte version;

  private int bucketId;

  private boolean isPrimary;

  private String[] serverGroups;
  
  private byte numServerGroups;
  
  public BucketServerLocation66() {
  }

  public BucketServerLocation66(int bucketId, int port, String host,
      boolean isPrimary, byte version, String[] groups) {
    super(host, port);
    this.bucketId = bucketId;
    this.isPrimary = isPrimary;
    this.version = version ;
    
    this.serverGroups = new String[groups.length];
    System.arraycopy(groups, 0, this.serverGroups, 0, groups.length);
    this.numServerGroups = (byte)this.serverGroups.length;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.bucketId = DataSerializer.readInteger(in);
    this.isPrimary = DataSerializer.readBoolean(in);
    this.version = DataSerializer.readByte(in);
    this.numServerGroups = in.readByte();
    this.serverGroups = new String[this.numServerGroups];
    if(this.numServerGroups > 0) {
      for(int i=0;i<this.numServerGroups;i++) {
        this.serverGroups[i] = DataSerializer.readString(in);
      }
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeInteger(this.bucketId, out);
    DataSerializer.writeBoolean(this.isPrimary, out);
    DataSerializer.writeByte(this.version, out);
    out.writeByte(this.numServerGroups);
    if(this.numServerGroups > 0) {
      for(String s: this.serverGroups) {
        DataSerializer.writeString(s, out);
      }
    }
  }

  public boolean isPrimary() {
    return this.isPrimary;
  }
  
  public byte getVersion(){
    return this.version;
  }

  @Override
  public String toString() {
    return "BucketServerLocation{bucketId=" + bucketId + ",host="
        + this.getHostName() + ",port=" + this.getPort() + ",isPrimary="
        + isPrimary + ",version=" + this.version + "}";
  }

  public int getBucketId() {
    return bucketId;
  }
  
  public String[] getServerGroups() {
    return this.serverGroups;
  }
}
