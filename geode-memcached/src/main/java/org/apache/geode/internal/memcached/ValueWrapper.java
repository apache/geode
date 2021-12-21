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
package org.apache.geode.internal.memcached;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.internal.MakeNotStatic;

/**
 * For CAS operation, we have to store a unique long with all the values being stored. This class
 * encapsulates the version and the value. Instances of this class can be obtained by using
 * {@link #getWrappedValue(byte[], int)}
 *
 *
 */
public class ValueWrapper implements DataSerializable {

  private static final long serialVersionUID = 7931598505833835569L;

  /**
   * used to uniquely identify the value. used for "cas" operation
   */
  private long casVersion;

  /**
   * the value being wrapped
   */
  private byte[] value;


  /**
   * the flags sent by the client
   */
  private int flags;

  /**
   * used to generate the version while constructing an instance.
   */
  @MakeNotStatic
  private static final AtomicLong versionGenerator = new AtomicLong();

  public ValueWrapper() {}

  private ValueWrapper(byte[] value, long version, int flags) {
    this.value = value;
    casVersion = version;
    this.flags = flags;
  }

  /**
   * This method should be used to obtain instances of ValueWrapper.
   *
   * @param value the value to be wrapped
   * @param flags the flags sent by the client
   * @return an instance of ValueWrapper that includes a version along with the given value.
   */
  public static ValueWrapper getWrappedValue(byte[] value, int flags) {
    return new ValueWrapper(value, versionGenerator.incrementAndGet(), flags);
  }

  /**
   * For binary protocol we always have to compare the cas version. To avoid turning each put into a
   * get and replace, use ValueWrapper instances from this method which only uses cas version. note
   * that equals and hashCode of this class have also been changed to only use the cas version.
   *
   * @return an instance with null value
   */
  public static ValueWrapper getDummyValue(long cas) {
    return new ValueWrapper(null, cas, 0);
  }

  /**
   * @return the encapsulated value
   */
  public byte[] getValue() {
    return value;
  }

  /**
   * @return the unique version for the encapsulated value
   */
  public long getVersion() {
    return casVersion;
  }

  /**
   * @return the flags
   */
  public int getFlags() {
    return flags;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ValueWrapper) {
      ValueWrapper other = (ValueWrapper) obj;
      return casVersion == other.casVersion;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(casVersion).hashCode();
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    casVersion = in.readLong();
    value = DataSerializer.readByteArray(in);
    flags = in.readInt();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeLong(casVersion);
    DataSerializer.writeByteArray(value, out);
    out.writeInt(flags);
  }

}
