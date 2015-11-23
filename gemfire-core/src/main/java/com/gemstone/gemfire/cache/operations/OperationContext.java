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

package com.gemstone.gemfire.cache.operations;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;

/**
 * Encapsulates a cache operation and the data associated with it for both the
 * pre-operation and post-operation cases. Implementations for specific
 * operations will extend this with the specifics as required e.g. a getKey()
 * method for a GET operation. Implementations for all the cache operations that
 * require authorization are provided.
 * 
 * Implementations of this interface are <b>not</b> expected to be thread-safe.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public abstract class OperationContext {

  /**
   * Enumeration for various cache operations. Implementations for each of the
   * supported operation listed here are provided.
   * 
   * @author Sumedh Wale
   * @since 5.5
   */
  public static final class OperationCode {

    private static final byte OP_GET = 1;

    private static final byte OP_PUT = 2;

    private static final byte OP_DESTROY = 3;

    private static final byte OP_INVALIDATE = 4;

    private static final byte OP_REGISTER_INTEREST = 5;

    private static final byte OP_UNREGISTER_INTEREST = 6;

    private static final byte OP_CONTAINS_KEY = 7;

    private static final byte OP_KEY_SET = 8;

    private static final byte OP_QUERY = 9;

    private static final byte OP_EXECUTE_CQ = 10;

    private static final byte OP_STOP_CQ = 11;

    private static final byte OP_CLOSE_CQ = 12;

    private static final byte OP_REGION_CLEAR = 13;

    private static final byte OP_REGION_CREATE = 14;

    private static final byte OP_REGION_DESTROY = 15;
    
    private static final byte OP_PUTALL = 16;
    
    private static final byte OP_EXECUTE_FUNCTION = 17;
    
    private static final byte OP_GET_DURABLE_CQS = 18;
    
    private static final byte OP_REMOVEALL = 19;
    
    private static final byte OP_RESOURCE = 20;
    
    private static final OperationCode[] VALUES = new OperationCode[22];

    private static final Map OperationNameMap = new HashMap();

    /**
     * An entry get operation.
     * 
     * @see Region#get(Object)
     */
    public static final OperationCode GET = new OperationCode("GET", OP_GET);

    /**
     * An entry create/update operation.
     * 
     * @see Region#put(Object, Object)
     */
    public static final OperationCode PUT = new OperationCode("PUT", OP_PUT);

    /**
     * An map putAll operation.
     * 
     * @see Region#putAll(Map map)
     */
    public static final OperationCode PUTALL = new OperationCode("PUTALL", OP_PUTALL);
    
    /**
     * A region removeAll operation.
     * 
     * @see Region#removeAll(java.util.Collection)
     * @since 8.1
     */
    public static final OperationCode REMOVEALL = new OperationCode("REMOVEALL", OP_REMOVEALL);
    
    /**
     * An entry destroy operation.
     * 
     * @see Region#destroy(Object, Object)
     */
    public static final OperationCode DESTROY = new OperationCode("DESTROY",
        OP_DESTROY);

    /**
     * An entry invalidate operation.
     * 
     * @see Region#invalidate(Object, Object)
     */
    public static final OperationCode INVALIDATE = new OperationCode(
        "INVALIDATE", OP_INVALIDATE);

    /**
     * A register interest operation.
     * 
     * @see Region#registerInterest(Object)
     */
    public static final OperationCode REGISTER_INTEREST = new OperationCode(
        "REGISTER_INTEREST", OP_REGISTER_INTEREST);

    /**
     * An unregister interest operation.
     * 
     * @see Region#unregisterInterest
     */
    public static final OperationCode UNREGISTER_INTEREST = new OperationCode(
        "UNREGISTER_INTEREST", OP_UNREGISTER_INTEREST);

    /**
     * A region <code>containsKey</code> operation.
     * 
     * @see Region#containsKey
     */
    public static final OperationCode CONTAINS_KEY = new OperationCode(
        "CONTAINS_KEY", OP_CONTAINS_KEY);

    /**
     * A region <code>keySet</code> operation.
     * 
     * @see Region#keySet
     */
    public static final OperationCode KEY_SET = new OperationCode("KEY_SET",
        OP_KEY_SET);

    /**
     * A cache query operation.
     * 
     * @see Region#query
     */
    public static final OperationCode QUERY = new OperationCode("QUERY",
        OP_QUERY);

    /**
     * A continuous query execution operation.
     */
    public static final OperationCode EXECUTE_CQ = new OperationCode(
        "EXECUTE_CQ", OP_EXECUTE_CQ);

    /**
     * A continuous query stop operation.
     */
    public static final OperationCode STOP_CQ = new OperationCode("STOP_CQ",
        OP_STOP_CQ);

    /**
     * A continuous query close operation.
     */
    public static final OperationCode CLOSE_CQ = new OperationCode("CLOSE_CQ",
        OP_CLOSE_CQ);

    /**
     * A region clear operation.
     * 
     * @see Region#clear
     */
    public static final OperationCode REGION_CLEAR = new OperationCode(
        "REGION_CLEAR", OP_REGION_CLEAR);

    /**
     * A region create operation.
     * 
     * @see Region#createSubregion
     * @see Cache#createRegion
     */
    public static final OperationCode REGION_CREATE = new OperationCode(
        "REGION_CREATE", OP_REGION_CREATE);

    /**
     * A region destroy operation.
     * 
     * @see Region#destroyRegion(Object)
     */
    public static final OperationCode REGION_DESTROY = new OperationCode(
        "REGION_DESTROY", OP_REGION_DESTROY);
    
    /**
     * A function execution operation
     */
    public static final OperationCode EXECUTE_FUNCTION = new OperationCode(
        "EXECUTE_FUNCTION", OP_EXECUTE_FUNCTION);
    
    /**
     * A get durable continuous query operation
     */
    public static final OperationCode GET_DURABLE_CQS = new OperationCode(
        "GET_DURABLE_CQS", OP_GET_DURABLE_CQS);
    
    
    /**
     * A resource operation. See ResourceOperationContext for more details
     */
    public static final OperationCode RESOURCE = new OperationCode(
        "RESOURCE", OP_RESOURCE);

    /** The name of this operation. */
    private final String name;

    /**
     * One of the following: OP_GET, OP_CREATE, OP_UPDATE, OP_INVALIDATE,
     * OP_DESTROY, OP_REGISTER_INTEREST, OP_REGISTER_INTEREST_REGEX,
     * OP_UNREGISTER_INTEREST, OP_UNREGISTER_INTEREST_REGEX, OP_QUERY,
     * OP_REGION_CREATE, OP_REGION_DESTROY, OP_PUTALL
     */
    private final byte opCode;

    /** Creates a new instance of Operation. */
    private OperationCode(String name, byte opCode) {
      this.name = name;
      this.opCode = opCode;
      VALUES[opCode] = this;
      OperationNameMap.put(name, this);
    }

    /**
     * Returns true if this is a entry get operation.
     */
    public boolean isGet() {
      return (this.opCode == OP_GET);
    }

    /**
     * Returns true if this is a entry create/update operation.
     */
    public boolean isPut() {
      return (this.opCode == OP_PUT);
    }
    
    /**
     * Returns true if this is a map putAll operation.
     */
    public boolean isPutAll() {
      return (this.opCode == OP_PUTALL);
    }
    
    /**
     * Returns true if this is a region removeAll operation.
     * @since 8.1
     */
    public boolean isRemoveAll() {
      return (this.opCode == OP_REMOVEALL);
    }

    /**
     * Returns true if this is an entry destroy operation.
     */
    public boolean isDestroy() {
      return (this.opCode == OP_DESTROY);
    }

    /**
     * Returns true if this is an entry invalidate operation.
     */
    public boolean isInvalidate() {
      return (this.opCode == OP_INVALIDATE);
    }

    /**
     * Returns true if this is a register interest operation.
     */
    public boolean isRegisterInterest() {
      return (this.opCode == OP_REGISTER_INTEREST);
    }

    /**
     * Returns true if this is an unregister interest operation.
     */
    public boolean isUnregisterInterest() {
      return (this.opCode == OP_UNREGISTER_INTEREST);
    }

    /**
     * Returns true if this is a region <code>containsKey</code> operation.
     */
    public boolean isContainsKey() {
      return (this.opCode == OP_CONTAINS_KEY);
    }

    /**
     * Returns true if this is a region <code>keySet</code> operation.
     */
    public boolean isKeySet() {
      return (this.opCode == OP_KEY_SET);
    }

    /**
     * Returns true if this is a cache query operation.
     */
    public boolean isQuery() {
      return (this.opCode == OP_QUERY);
    }

    /**
     * Returns true if this is a continuous query execution operation.
     */
    public boolean isExecuteCQ() {
      return (this.opCode == OP_EXECUTE_CQ);
    }

    /**
     * Returns true if this is a continuous query stop operation.
     */
    public boolean isStopCQ() {
      return (this.opCode == OP_STOP_CQ);
    }

    /**
     * Returns true if this is a continuous query close operation.
     */
    public boolean isCloseCQ() {
      return (this.opCode == OP_CLOSE_CQ);
    }

    /**
     * Returns true if this is a region clear operation.
     */
    public boolean isRegionClear() {
      return (this.opCode == OP_REGION_CLEAR);
    }

    /**
     * Returns true if this is a region create operation.
     */
    public boolean isRegionCreate() {
      return (this.opCode == OP_REGION_CREATE);
    }

    /**
     * Returns true if this is a region destroy operation.
     */
    public boolean isRegionDestroy() {
      return (this.opCode == OP_REGION_DESTROY);
    }
    
    /**
     * Returns true if this is a execute region function operation.
     */
    public boolean isExecuteRegionFunction() {
      return (this.opCode == OP_EXECUTE_FUNCTION);
    }

    /**
     * Returns true if this is a get durable cqs operation.
     */
    public boolean isGetDurableCQs() {
      return (this.opCode == OP_GET_DURABLE_CQS);
    }
    
    /**
     * Returns the <code>OperationCode</code> represented by specified byte.
     */
    public static OperationCode fromOrdinal(byte opCode) {
      return VALUES[opCode];
    }

    /**
     * Returns the <code>OperationCode</code> represented by specified string.
     */
    public static OperationCode parse(String operationName) {
      return (OperationCode)OperationNameMap.get(operationName);
    }

    /**
     * Returns the byte representing this operation code.
     * 
     * @return a byte representing this operation.
     */
    public byte toOrdinal() {
      return this.opCode;
    }

    /**
     * Returns a string representation for this operation.
     * 
     * @return the name of this operation.
     */
    @Override
    final public String toString() {
      return this.name;
    }

    /**
     * Indicates whether other object is same as this one.
     * 
     * @return true if other object is same as this one.
     */
    @Override
    final public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof OperationCode)) {
        return false;
      }
      final OperationCode other = (OperationCode)obj;
      return (other.opCode == this.opCode);
    }

    /**
     * Indicates whether other <code>OperationCode</code> is same as this one.
     * 
     * @return true if other <code>OperationCode</code> is same as this one.
     */
    final public boolean equals(final OperationCode opCode) {
      return (opCode != null && opCode.opCode == this.opCode);
    }

    /**
     * Returns a hash code value for this <code>OperationCode</code> which is
     * the same as the byte representing its operation type.
     * 
     * @return the hashCode of this operation.
     */
    @Override
    final public int hashCode() {
      return this.opCode;
    }

  }

  /**
   * Return the operation code associated with the <code>OperationContext</code>
   * object.
   */
  public abstract OperationCode getOperationCode();

  /**
   * True if the context is for post-operation.
   * 
   * The <code>OperationContext</code> interface encapsulates the data both
   * before the operation is performed and after the operation is complete. For
   * example, for a query operation the <code>Query</code> object as well as
   * the list of region names referenced by the query would be part of the
   * context object in the pre-processing stage. In the post-processing stage
   * the context object shall contain results of the query.
   */
  public abstract boolean isPostOperation();

  /**
   * When called post-operation, returns true if the operation was one that performed an update.
   * An update occurs when one of the following methods on <code>getOperationCode()</code> returns true:
   * <code>isPut()</code>, <code>isPutAll()</code>, <code>isDestroy()</code>, <code>isRemoveAll()</code>,
   * <code>isInvalidate()</code>, <code>isRegionCreate()</code>, <code>isRegionClear()</code>, <code>isRegionDestroy()</code>.
   * Otherwise, returns false.
   * 
   * @since 6.6
   */
  public boolean isClientUpdate() {
    if (isPostOperation()) {
      switch (getOperationCode().opCode) {
        case OperationCode.OP_PUT:
        case OperationCode.OP_PUTALL:
        case OperationCode.OP_DESTROY:
        case OperationCode.OP_REMOVEALL:
        case OperationCode.OP_INVALIDATE:
        case OperationCode.OP_REGION_CREATE:
        case OperationCode.OP_REGION_DESTROY:
        case OperationCode.OP_REGION_CLEAR:
          return true;
      }
    }
    return false;
  }

  /**
   * True if the context is created before sending the updates to a client.
   */
  @Deprecated
  public boolean isClientUpdate(OperationContext context) {
    OperationCode opCode = context.getOperationCode();
    return context.isPostOperation()
        && (opCode.isPut() || opCode.isPutAll() || opCode.isDestroy()
            || opCode.isRemoveAll()
            || opCode.isInvalidate() || opCode.isRegionCreate()
            || opCode.isRegionDestroy() || opCode.isRegionClear());
  }
}
