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
 *
 */

package org.apache.geode.redis.internal.data;

import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS;
import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS_PER_BUCKET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.executor.cluster.CRC16;
import org.apache.geode.redis.internal.executor.cluster.RedisPartitionResolver;
import org.apache.geode.redis.internal.netty.Coder;

/**
 * This class is a wrapper for the any Regions that need to store a byte[]. The only data this an
 * instance will store is a byte[] for the data but it is also serializable and comparable so it is
 * able to be used in querying. Class is also marked as Serializable for test support.
 */
public class ByteArrayWrapper
    implements DataSerializableFixedID, Comparable<ByteArrayWrapper> {
  /**
   * The data portion of ValueWrapper
   */
  protected byte[] value;

  private transient Object routingId;

  /**
   * Empty constructor for serialization
   */
  public ByteArrayWrapper() {}

  /**
   * Default constructor constructs a ValueWrapper and initialize the {@link #value}
   */
  public ByteArrayWrapper(byte[] value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return Coder.bytesToString(value);
  }

  public byte[] toBytes() {
    return this.value;
  }

  public void setBytes(byte[] bytes) {
    this.value = bytes;
  }

  /**
   * Getter for the length of the {@link #value} array
   *
   * @return The length of the value array
   */
  public int length() {
    return value.length;
  }

  /**
   * Hash code for byte[] wrapped by this object
   */
  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }


  /**
   * This equals is neither symmetric and therefore not transitive, because a String with the same
   * underlying bytes is considered equal. Clearly calling {@link String#equals(Object)} would not
   * yield the same result
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof ByteArrayWrapper) {
      return Arrays.equals(value, ((ByteArrayWrapper) other).value);
    } else if (other instanceof String) {
      return Arrays.equals(value, Coder.stringToBytes((String) other));
    }
    return false;
  }

  /**
   * This is a byte to byte comparator, it is not lexicographical but purely compares byte by byte
   * values
   */
  @Override
  public int compareTo(ByteArrayWrapper other) {
    return arrayCmp(value, other.value);
  }

  /**
   * Used by the {@link RedisPartitionResolver} to map slots to buckets. Supports using hashtags
   * in the same way that redis does.
   *
   * @see <a href="https://redis.io/topics/cluster-spec">Redis Cluster Spec</a>
   */
  public synchronized Object getRoutingId() {
    if (routingId == null && value != null) {
      int startHashtag = Integer.MAX_VALUE;
      int endHashtag = 0;

      for (int i = 0; i < value.length; i++) {
        if (value[i] == '{' && startHashtag == Integer.MAX_VALUE) {
          startHashtag = i;
        } else if (value[i] == '}') {
          endHashtag = i;
          break;
        }
      }

      if (endHashtag - startHashtag <= 1) {
        startHashtag = -1;
        endHashtag = value.length;
      }

      // & (REDIS_SLOTS - 1) is equivalent to % REDIS_SLOTS but supposedly faster
      routingId = (CRC16.calculate(value, startHashtag + 1, endHashtag) & (REDIS_SLOTS - 1))
          / REDIS_SLOTS_PER_BUCKET;
    }

    return routingId;
  }

  /**
   * Private helper method to compare two byte arrays, A.compareTo(B). The comparison is basically
   * numerical, for each byte index, the byte representing the greater value will be the greater
   *
   * @param A byte[]
   * @param B byte[]
   * @return 1 if A > B, -1 if B > A, 0 if A == B
   */
  private int arrayCmp(byte[] A, byte[] B) {
    if (A == B) {
      return 0;
    }
    if (A == null) {
      return -1;
    } else if (B == null) {
      return 1;
    }

    int len = Math.min(A.length, B.length);

    for (int i = 0; i < len; i++) {
      byte a = A[i];
      byte b = B[i];
      int diff = a - b;
      if (diff > 0) {
        return 1;
      } else if (diff < 0) {
        return -1;
      }
    }

    if (A.length > B.length) {
      return 1;
    } else if (B.length > A.length) {
      return -1;
    }

    return 0;
  }


  private static byte[] concatArrays(byte[] o, byte[] n) {
    int oLen = o.length;
    int nLen = n.length;
    byte[] combined = new byte[oLen + nLen];
    System.arraycopy(o, 0, combined, 0, oLen);
    System.arraycopy(n, 0, combined, oLen, nLen);
    return combined;
  }

  public void append(byte[] appendBytes) {
    setBytes(concatArrays(value, appendBytes));
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.REDIS_BYTE_ARRAY_WRAPPER;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    DataSerializer.writeByteArray(value, out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException {
    value = DataSerializer.readByteArray(in);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
