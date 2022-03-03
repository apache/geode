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
    args = object;
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
      isFnSerializationReqd = false;
      function = FunctionService.getFunction((String) object);
      if (function == null) {
        functionId = (String) object;
      }
    } else {
      function = (Function) object;
      isFnSerializationReqd = true;
    }
    args = DataSerializer.readObject(in);
    filter = DataSerializer.readHashSet(in);
    if (StaticSerialization.getVersionForDataStream(in).isNotOlderThan(KnownVersion.GEODE_1_11_0)) {
      bucketArray = DataSerializer.readIntArray(in);
    } else {
      HashSet<Integer> bucketSet = DataSerializer.readHashSet(in);
      bucketArray = BucketSetHelper.fromSet(bucketSet);
    }
    isReExecute = DataSerializer.readBoolean(in);

    KnownVersion dataStreamVersion = StaticSerialization.getVersionForDataStream(in);
    if (dataStreamVersion.isNewerThanOrEqualTo(KnownVersion.GEODE_1_14_0)
        || (dataStreamVersion.isNewerThanOrEqualTo(KnownVersion.GEODE_1_12_1)
            && dataStreamVersion.isOlderThan(KnownVersion.GEODE_1_13_0))
        || (dataStreamVersion.isNewerThanOrEqualTo(KnownVersion.GEODE_1_13_2)
            && dataStreamVersion.isOlderThan(KnownVersion.GEODE_1_14_0))) {
      principal = DataSerializer.readObject(in);
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    if (isFnSerializationReqd) {
      DataSerializer.writeObject(function, out);
    } else {
      DataSerializer.writeObject(function.getId(), out);
    }
    DataSerializer.writeObject(args, out);
    DataSerializer.writeHashSet((HashSet) filter, out);
    if (StaticSerialization.getVersionForDataStream(out)
        .isNotOlderThan(KnownVersion.GEODE_1_11_0)) {
      DataSerializer.writeIntArray(bucketArray, out);
    } else {
      Set<Integer> bucketSet = BucketSetHelper.toSet(bucketArray);
      DataSerializer.writeHashSet((HashSet) bucketSet, out);
    }
    DataSerializer.writeBoolean(isReExecute, out);

    KnownVersion dataStreamVersion = StaticSerialization.getVersionForDataStream(out);
    if (dataStreamVersion.isNewerThanOrEqualTo(KnownVersion.GEODE_1_14_0)
        || (dataStreamVersion.isNewerThanOrEqualTo(KnownVersion.GEODE_1_12_1)
            && dataStreamVersion.isOlderThan(KnownVersion.GEODE_1_13_0))
        || (dataStreamVersion.isNewerThanOrEqualTo(KnownVersion.GEODE_1_13_2)
            && dataStreamVersion.isOlderThan(KnownVersion.GEODE_1_14_0))) {
      DataSerializer.writeObject(principal, out);
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

    return "{FunctionRemoteContext "
        + "functionId=" + functionId
        + " args=" + args
        + " isReExecute=" + isReExecute
        + " filter=" + filter
        + " bucketArray=" + Arrays.toString(bucketArray) + "}";
  }
}
