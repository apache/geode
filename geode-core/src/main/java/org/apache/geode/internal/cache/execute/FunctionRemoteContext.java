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
package org.apache.geode.internal.cache.execute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.BucketSetHelper;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.StaticDeserialization;
import org.apache.geode.internal.serialization.StaticSerialization;

/**
 * FunctionContext for remote/target nodes
 */
public class FunctionRemoteContext implements DataSerializable {

  private Set filter;

  private Object args;

  private int[] bucketArray;

  private boolean isReExecute;

  private boolean isFnSerializationReqd;

  private String functionId;

  private Function function;

  private Object principal;

  public FunctionRemoteContext() {}

  public FunctionRemoteContext(final Function function, Object object, Set filter,
      int[] bucketArray, boolean isReExecute, boolean isFnSerializationReqd, Object principal) {
    this.function = function;
    this.args = object;
    this.filter = filter;
    this.bucketArray = bucketArray;
    this.isReExecute = isReExecute;
    this.isFnSerializationReqd = isFnSerializationReqd;
    this.principal = principal;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    Object object = DataSerializer.readObject(in);
    if (object instanceof String) {
      this.isFnSerializationReqd = false;
      this.function = FunctionService.getFunction((String) object);
      if (this.function == null) {
        this.functionId = (String) object;
      }
    } else {
      this.function = (Function) object;
      this.isFnSerializationReqd = true;
    }
    this.args = DataSerializer.readObject(in);
    this.filter = (HashSet) DataSerializer.readHashSet(in);
    if (StaticDeserialization.getVersionForDataStream(in)
        .isNotOlderThan(KnownVersion.GEODE_1_11_0)) {
      this.bucketArray = DataSerializer.readIntArray(in);
    } else {
      HashSet<Integer> bucketSet = DataSerializer.readHashSet(in);
      this.bucketArray = BucketSetHelper.fromSet(bucketSet);
    }
    this.isReExecute = DataSerializer.readBoolean(in);

    KnownVersion dataStreamVersion = StaticDeserialization.getVersionForDataStream(in);
    if (dataStreamVersion.isNewerThanOrEqualTo(KnownVersion.GEODE_1_14_0)
        || (dataStreamVersion.isNewerThanOrEqualTo(KnownVersion.GEODE_1_12_1)
            && dataStreamVersion.isOlderThan(KnownVersion.GEODE_1_13_0))
        || (dataStreamVersion.isNewerThanOrEqualTo(KnownVersion.GEODE_1_13_1)
            && dataStreamVersion.isOlderThan(KnownVersion.GEODE_1_14_0))) {
      this.principal = DataSerializer.readObject(in);
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    if (this.isFnSerializationReqd) {
      DataSerializer.writeObject(this.function, out);
    } else {
      DataSerializer.writeObject(function.getId(), out);
    }
    DataSerializer.writeObject(this.args, out);
    DataSerializer.writeHashSet((HashSet) this.filter, out);
    if (StaticSerialization.getVersionForDataStream(out)
        .isNotOlderThan(KnownVersion.GEODE_1_11_0)) {
      DataSerializer.writeIntArray(this.bucketArray, out);
    } else {
      Set<Integer> bucketSet = BucketSetHelper.toSet(this.bucketArray);
      DataSerializer.writeHashSet((HashSet) bucketSet, out);
    }
    DataSerializer.writeBoolean(this.isReExecute, out);

    KnownVersion dataStreamVersion = StaticSerialization.getVersionForDataStream(out);
    if (dataStreamVersion.isNewerThanOrEqualTo(KnownVersion.GEODE_1_14_0)
        || (dataStreamVersion.isNewerThanOrEqualTo(KnownVersion.GEODE_1_12_1)
            && dataStreamVersion.isOlderThan(KnownVersion.GEODE_1_13_0))
        || (dataStreamVersion.isNewerThanOrEqualTo(KnownVersion.GEODE_1_13_1)
            && dataStreamVersion.isOlderThan(KnownVersion.GEODE_1_14_0))) {
      DataSerializer.writeObject(this.principal, out);
    }
  }

  public Set getFilter() {
    return filter;
  }

  public Object getArgs() {
    return args;
  }

  public int[] getBucketArray() {
    return bucketArray;
  }

  public boolean isReExecute() {
    return isReExecute;
  }

  public Function getFunction() {
    return function;
  }

  public String getFunctionId() {
    return functionId;
  }

  public Object getPrincipal() {
    return principal;
  }

  @Override
  public String toString() {

    StringBuffer buff = new StringBuffer();
    buff.append("{FunctionRemoteContext ");
    buff.append("functionId=" + functionId);
    buff.append(" args=" + args);
    buff.append(" isReExecute=" + isReExecute);
    buff.append(" filter=" + filter);
    buff.append(" bucketArray=" + Arrays.toString(bucketArray) + "}");
    return buff.toString();
  }
}
