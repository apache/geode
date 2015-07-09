/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.gemstone.gemfire.internal.util.BlobHelper;

public class GFKey implements WritableComparable<GFKey> {
  private Object key;

  public Object getKey() {
    return key;
  }

  public void setKey(Object key) {
    this.key = key;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] bytes = BlobHelper.serializeToBlob(key);
    out.writeInt(bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int len = in.readInt();
    byte[] bytes = new byte[len];
    in.readFully(bytes, 0, len);
    try {
      key = BlobHelper.deserializeBlob(bytes);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public int compareTo(GFKey o) {
    try {
      byte[] b1 = BlobHelper.serializeToBlob(key);
      byte[] b2 = BlobHelper.serializeToBlob(o.key);
      return WritableComparator.compareBytes(b1, 0, b1.length, b2, 0, b2.length);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return 0;
  }
}
