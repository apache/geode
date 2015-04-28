/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
/**
 * FunctionContext for remote/target nodes
 * 
 * @author Yogesh Mahajan  
 *
 */
public class FunctionRemoteContext implements DataSerializable {

  private Set filter;

  private Object args;

  private Set<Integer> bucketSet;

  private boolean isReExecute;

  private boolean isFnSerializationReqd;

  private String functionId;

  private Function function;

  public FunctionRemoteContext() {
  }

  public FunctionRemoteContext(final Function function, Object object,
      Set filter, Set<Integer> bucketSet, boolean isReExecute,
      boolean isFnSerializationReqd) {
    this.function = function;
    this.args = object;
    this.filter = filter;
    this.bucketSet = bucketSet;
    this.isReExecute = isReExecute;
    this.isFnSerializationReqd = isFnSerializationReqd;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    Object object = DataSerializer.readObject(in);
    if (object instanceof String) {
      this.isFnSerializationReqd = false;
      this.function = FunctionService.getFunction((String)object);
      if (this.function == null) {
        this.functionId = (String)object;
      }
    }
    else {
      this.function = (Function)object;
      this.isFnSerializationReqd = true;
    }
    this.args = DataSerializer.readObject(in);
    this.filter = (HashSet)DataSerializer.readHashSet(in);
    this.bucketSet = (HashSet)DataSerializer.readHashSet(in);
    this.isReExecute = DataSerializer.readBoolean(in);
  }

  public void toData(DataOutput out) throws IOException {
    if (this.isFnSerializationReqd) {
      DataSerializer.writeObject(this.function, out);
    }
    else {
      DataSerializer.writeObject(function.getId(), out);
    }
    DataSerializer.writeObject(this.args, out);
    DataSerializer.writeHashSet((HashSet)this.filter, out);
    DataSerializer.writeHashSet((HashSet)this.bucketSet, out);
    DataSerializer.writeBoolean(this.isReExecute, out);
  }

  public Set getFilter() {
    return filter;
  }

  public Object getArgs() {
    return args;
  }

  public Set<Integer> getBucketSet() {
    return bucketSet;
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
  @Override
  public String toString() {

    StringBuffer buff = new StringBuffer();
    buff.append("{FunctionRemoteContext ");
    buff.append("functionId=" + functionId);
    buff.append(" args=" + args);
    buff.append(" isReExecute=" + isReExecute);
    buff.append(" filter=" + filter);
    buff.append(" bucketSet=" + bucketSet + "}");
    return buff.toString();
  }
}
