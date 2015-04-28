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
public class BucketServerLocation extends ServerLocation {
  
  private byte version;

  private int bucketId;

  private boolean isPrimary;

  public BucketServerLocation() {
  }

  public BucketServerLocation(int bucketId, int port, String host,
      boolean isPrimary, byte version) {
    super(host, port);
    this.bucketId = bucketId;
    this.isPrimary = isPrimary;
    this.version = version ;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.bucketId = DataSerializer.readInteger(in);
    this.isPrimary = DataSerializer.readBoolean(in);
    this.version = DataSerializer.readByte(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeInteger(this.bucketId, out);
    DataSerializer.writeBoolean(this.isPrimary, out);
    DataSerializer.writeByte(this.version, out);
  }

  public boolean isPrimary() {
    return this.isPrimary;
  }
  
  public byte getVersion(){
    return this.version;
  }

/*  @Override
  public String toString() {
    return "BucketServerLocation{bucketId=" + bucketId + ",host="
        + this.getHostName() + ",port=" + this.getPort() + ",isPrimary="
        + isPrimary + ",version=" + this.version + "}";
  }*/

  public int getBucketId() {
    return bucketId;
  }
}
