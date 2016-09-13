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

import org.apache.geode.security.ResourcePermission;

/**
 * Encapsulates a cache operation and the data associated with it for both the
 * pre-operation and post-operation cases. Implementations for specific
 * operations will extend this with the specifics as required e.g. a getKey()
 * method for a GET operation. Implementations for all the cache operations that
 * require authorization are provided.
 *
 * Implementations of this interface are <b>not</b> expected to be thread-safe.
 *
 * @since GemFire 5.5
 *
 * @deprecated since Geode1.0, use {@link ResourcePermission} instead
 */
public abstract class OperationContext{

  public enum OperationCode {
    @Deprecated
    GET,
    @Deprecated
    PUT,
    @Deprecated
    PUTALL,
    @Deprecated
    REMOVEALL,
    @Deprecated
    DESTROY,
    @Deprecated
    INVALIDATE,
    @Deprecated
    REGISTER_INTEREST,
    @Deprecated
    UNREGISTER_INTEREST,
    @Deprecated
    CONTAINS_KEY,
    @Deprecated
    KEY_SET,
    @Deprecated
    QUERY,
    @Deprecated
    EXECUTE_CQ,
    @Deprecated
    STOP_CQ,
    @Deprecated
    CLOSE_CQ,
    @Deprecated
    REGION_CLEAR,
    @Deprecated
    REGION_CREATE,
    @Deprecated
    REGION_DESTROY,
    @Deprecated
    EXECUTE_FUNCTION,
    @Deprecated
    GET_DURABLE_CQS;

    /**
     * Check if this is an entry get operation.
     *
     * @return true if this is an entry get operation
     * @deprecated Use {@code getOperationCode() == GET} instead
     */
    @Deprecated
    public boolean isGet() {
      return (this == GET);
    }

    /**
     * Check if this is a entry create/update operation.
     *
     * @return true if this is a entry create/update operation.
     * @deprecated Use {@code getOperationCode() == PUT} instead
     */
    @Deprecated
    public boolean isPut() {
      return (this == PUT);
    }

    /**
     * Check if this is a map putAll operation.
     *
     * @return true if this is a map putAll operation.
     * @deprecated Use {@code getOperationCode() == PUTALL} instead
     */
    @Deprecated
    public boolean isPutAll() {
      return (this == PUTALL);
    }

    /**
     * Check if this is a region removeAll operation.
     *
     * @return true if this is a region removeAll operation.
     * @deprecated Use {@code getOperationCode() == REMOVEALL} instead
     * @since GemFire 8.1
     */
    @Deprecated
    public boolean isRemoveAll() {
      return (this == REMOVEALL);
    }

    /**
     * Check if this is an entry destroy operation.
     *
     * @return true if this is an entry destroy operation.
     * @deprecated Use {@code getOperationCode() == DESTROY} instead
     */
    @Deprecated
    public boolean isDestroy() {
      return (this == DESTROY);
    }

    /**
     * Check if this is an entry invalidate operation.
     *
     * @return true if this is an entry invalidate operation.
     * @deprecated Use {@code getOperationCode() == INVALIDATE} instead
     */
    @Deprecated
    public boolean isInvalidate() {
      return (this == INVALIDATE);
    }

    /**
     * Check if this is a register interest operation.
     *
     * @return true if this is a register interest operation.
     * @deprecated Use {@code getOperationCode() == REGISTER_INTEREST} instead
     */
    @Deprecated
    public boolean isRegisterInterest() {
      return (this == REGISTER_INTEREST);
    }

    /**
     * Check if this is an unregister interest operation.
     *
     * @return true if this is an unregister interest operation.
     * @deprecated Use {@code getOperationCode() ==  UNREGISTER_INTEREST} instead
     */
    @Deprecated
    public boolean isUnregisterInterest() {
      return (this == UNREGISTER_INTEREST);
    }

    /**
     * Check if this is a region <code>containsKey</code> operation.
     *
     * @return true if this is a region <code>containsKey</code> operation.
     * @deprecated Use {@code getOperationCode() == CONTAINS_KEY} instead
     */
    @Deprecated
    public boolean isContainsKey() {
      return (this == CONTAINS_KEY);
    }

    /**
     * Check if this is a region <code>keySet</code> operation.
     *
     * @return true if this is a region <code>keySet</code> operation.
     * @deprecated Use {@code getOperationCode() == KEY_SET} instead
     */
    @Deprecated
    public boolean isKeySet() {
      return (this == KEY_SET);
    }

    /**
     * Check if this is a cache query operation.
     *
     * @return true if this is a cache query operation.
     * @deprecated Use {@code getOperationCode() == QUERY} instead
     */
    @Deprecated
    public boolean isQuery() {
      return (this == QUERY);
    }

    /**
     * Check if this is a continuous query execution operation.
     *
     * @return true if this is a continuous query execution operation.
     * @deprecated Use {@code getOperationCode() == EXECUTE_CQ} instead
     */
    @Deprecated
    public boolean isExecuteCQ() {
      return (this == EXECUTE_CQ);
    }

    /**
     * Check if this is a continuous query stop operation.
     *
     * @return true if this is a continuous query stop operation.
     * @deprecated Use {@code getOperationCode() == STOP_CQ} instead
     */
    @Deprecated
    public boolean isStopCQ() {
      return (this == STOP_CQ);
    }

    /**
     * Check if this is a continuous query close operation.
     *
     * @return true if this is a continuous query close operation.
     * @deprecated Use {@code getOperationCode() == CLOSE_CQ} instead
     */
    @Deprecated
    public boolean isCloseCQ() {
      return (this == CLOSE_CQ);
    }

    /**
     * Check if this is a region clear operation.
     *
     * @return true if this is a region clear operation.
     * @deprecated Use {@code getOperationCode() == REGION_CLEAR} instead
     */
    @Deprecated
    public boolean isRegionClear() {
      return (this == REGION_CLEAR);
    }

    /**
     * Check if this is a region create operation.
     *
     * @return true if this is a region create operation.
     * @deprecated Use {@code getOperationCode() == REGION_CREATE} instead
     */
    @Deprecated
    public boolean isRegionCreate() {
      return (this == REGION_CREATE);
    }

    /**
     * Check if this is a region destroy operation.
     *
     * @return true if this is a region destroy operation.
     * @deprecated Use {@code getOperationCode() == REGION_DESTROY} instead
     */
    @Deprecated
    public boolean isRegionDestroy() {
      return (this == REGION_DESTROY);
    }

    /**
     * Check if this is a execute region function operation.
     *
     * @return true if this is a execute region function operation.
     * @deprecated Use {@code getOperationCode() == EXECUTE_FUNCTION} instead
     */
    @Deprecated
    public boolean isExecuteRegionFunction() {
      return (this == EXECUTE_FUNCTION);
    }

    /**
     * Check if this is a get durable cqs operation.
     *
     * @return true if this is a get durable cqs operation.
     * @deprecated Use {@code getOperationCode() == GET_DURABLE_CQS} instead
     */
    @Deprecated
    public boolean isGetDurableCQs() {
      return (this == GET_DURABLE_CQS);
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
   * @since GemFire 6.6
   */
  public boolean isClientUpdate() {
    if (isPostOperation()) {
      switch (getOperationCode()) {
        case PUT:
        case PUTALL:
        case DESTROY:
        case REMOVEALL:
        case INVALIDATE:
        case REGION_CREATE:
        case REGION_DESTROY:
        case REGION_CLEAR:
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
