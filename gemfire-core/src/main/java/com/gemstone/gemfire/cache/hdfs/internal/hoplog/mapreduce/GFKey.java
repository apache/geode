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
