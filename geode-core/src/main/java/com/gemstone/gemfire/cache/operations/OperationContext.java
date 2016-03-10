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

import com.gemstone.gemfire.management.internal.security.Resource;

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
  public enum OperationCode {
    ALL,
    ALTER,
    ALTER_RUNTIME,
    BACKUP,
    BECOME_LOCK_GRANTOR,
    CLOSE_CQ,
    COMPACT,
    COMPACT_DISKSTORE, // TODO: Do we need this?
    CONFIGURE,
    CONTAINS_KEY,
    CREATE,
    CREATE_MANAGER, // TODO: Do we need this?
    CREATE_QUERY,
    CREATE_REGION, // TODO: Do we need this?
    DELETE,
    DELETE_QUERY,
    DEPLOY,
    DESTROY,
    DESTROY_INDEX, // TODO: Do we need this?
    EXECUTE,
    EXECUTE_CQ,
    EXECUTE_FUNCTION,
    EXPORT,
    EXPORT_DATA, // TODO: Do we need this?
    EXPORT_LOGS,
    EXPORT_STACKTRACE,
    FLUSH,
    GC,
    GET,
    GET_DURABLE_CQS,
    IMPORT,
    IMPORT_DATA, // TODO: Do we need this?
    INVALIDATE,
    KEY_SET,
    LIST,
    LIST_DS, // TODO: Do we need this?
    LOCATE_ENTRY, // TODO: Do we need this?
    MANAGE,  // TODO: Do we need this?
    NETSTAT,
    PAUSE,
    PROCESS_COMMAND,
    PUT,
    PUTALL,
    QUERY,
    REBALANCE,
    REGION_CLEAR,
    REGION_CREATE,
    REGION_DESTROY,
    REGISTER_INTEREST,
    REMOVEALL,
    RENAME,
    RESOURCE,
    RESUME,
    REVOKE_MISSING,
    ROLL,
    SET_ALERT_LEVEL,
    SET_DISK_USAGE_CRITICAL,
    SET_DISK_USAGE_WARNING,
    SET_PULSE_URL,
    SET_QUERY_RESULT_LIMIT,
    SET_QUERY_COLLECTION_DEPTH,
    SET_STATUS_MESSAGE,
    SHOW_DEADLOCKS,
    SHOW_LOG,
    SHOW_METRICS,
    SHOW_MISSING,
    SHUTDOWN,
    START,
    STATUS,
    STOP,
    STOP_CONTINUOUS_QUERY, // TODO: Do we need this?
    STOP_CQ,
    UNDEPLOY,
    UNREGISTER_INTEREST,
    UPDATE_QUERY,
    VALIDATE;

    /**
     * Returns true if this is a entry get operation.
     */
    public boolean isGet() {
      return (this == GET);
    }

    /**
     * Returns true if this is a entry create/update operation.
     */
    public boolean isPut() {
      return (this == PUT);
    }

    /**
     * Returns true if this is a map putAll operation.
     */
    public boolean isPutAll() {
      return (this == PUTALL);
    }

    /**
     * Returns true if this is a region removeAll operation.
     * @since 8.1
     */
    public boolean isRemoveAll() {
      return (this == REMOVEALL);
    }

    /**
     * Returns true if this is an entry destroy operation.
     */
    public boolean isDestroy() {
      return (this == DESTROY);
    }

    /**
     * Returns true if this is an entry invalidate operation.
     */
    public boolean isInvalidate() {
      return (this == INVALIDATE);
    }

    /**
     * Returns true if this is a register interest operation.
     */
    public boolean isRegisterInterest() {
      return (this == REGISTER_INTEREST);
    }

    /**
     * Returns true if this is an unregister interest operation.
     */
    public boolean isUnregisterInterest() {
      return (this == UNREGISTER_INTEREST);
    }

    /**
     * Returns true if this is a region <code>containsKey</code> operation.
     */
    public boolean isContainsKey() {
      return (this == CONTAINS_KEY);
    }

    /**
     * Returns true if this is a region <code>keySet</code> operation.
     */
    public boolean isKeySet() {
      return (this == KEY_SET);
    }

    /**
     * Returns true if this is a cache query operation.
     */
    public boolean isQuery() {
      return (this == QUERY);
    }

    /**
     * Returns true if this is a continuous query execution operation.
     */
    public boolean isExecuteCQ() {
      return (this == EXECUTE_CQ);
    }

    /**
     * Returns true if this is a continuous query stop operation.
     */
    public boolean isStopCQ() {
      return (this == STOP_CQ);
    }

    /**
     * Returns true if this is a continuous query close operation.
     */
    public boolean isCloseCQ() {
      return (this == CLOSE_CQ);
    }

    /**
     * Returns true if this is a region clear operation.
     */
    public boolean isRegionClear() {
      return (this == REGION_CLEAR);
    }

    /**
     * Returns true if this is a region create operation.
     */
    public boolean isRegionCreate() {
      return (this == REGION_CREATE);
    }

    /**
     * Returns true if this is a region destroy operation.
     */
    public boolean isRegionDestroy() {
      return (this == REGION_DESTROY);
    }

    /**
     * Returns true if this is a execute region function operation.
     */
    public boolean isExecuteRegionFunction() {
      return (this == EXECUTE_FUNCTION);
    }

    /**
     * Returns true if this is a get durable cqs operation.
     */
    public boolean isGetDurableCQs() {
      return (this == GET_DURABLE_CQS);
    }



  }

  /**
   * Return the operation code associated with the <code>OperationContext</code>
   * object.
   */
  public abstract OperationCode getOperationCode();

  public Resource getResource(){
    return Resource.DEFAULT;
  }


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
