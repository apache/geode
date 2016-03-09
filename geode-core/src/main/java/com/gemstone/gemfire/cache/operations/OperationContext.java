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
    DESTROY,
    INVALIDATE,
    REGISTER_INTEREST,
    UNREGISTER_INTEREST,
    CONTAINS_KEY,
    KEY_SET,
    EXECUTE_CQ,
    STOP_CQ,
    CLOSE_CQ,
    REGION_CLEAR,
    REGION_CREATE,
    REGION_DESTROY,
    PUTALL,
    GET_DURABLE_CQS,
    REMOVEALL,
    RESOURCE,
    MANAGE,
    LIST,
    CREATE_QUERY,
    UPDATE_QUERY,
    DELETE_QUERY,
    ALTER_REGION,
    ALTER_RUNTIME,
    BACKUP_DISKSTORE,
    CHANGE_ALERT_LEVEL,
    CLOSE_DURABLE_CLIENT,
    CLOSE_DURABLE_CQ,
    COMPACT_DISKSTORE,
    CONFIGURE_PDX,
    CREATE_AEQ,
    CREATE_DISKSTORE,
    CREATE_GW_RECEIVER,
    CREATE_GW_SENDER,
    CREATE_INDEX,
    CREATE_REGION,
    DEPLOY,
    DESTROY_DISKSTORE,
    DESTROY_FUNCTION,
    DESTROY_INDEX,
    DESTROY_REGION,
    EXECUTE_FUNCTION,
    EXPORT_CONFIG,
    EXPORT_DATA,
    EXPORT_LOGS,
    EXPORT_OFFLINE_DISKSTORE,
    EXPORT_STACKTRACE,
    GC,
    GET,
    IMPORT_CONFIG,
    IMPORT_DATA,
    LIST_DS,
    LOAD_BALANCE_GW_SENDER,
    LOCATE_ENTRY,
    NETSTAT,
    PAUSE_GW_SENDER,
    PUT,
    QUERY,
    REBALANCE,
    REMOVE,
    RENAME_PDX,
    RESUME_GW_SENDER,
    REVOKE_MISSING_DISKSTORE,
    SHOW_DEADLOCKS,
    SHOW_LOG,
    SHOW_METRICS,
    SHOW_MISSING_DISKSTORES,
    SHOW_SUBSCRIPTION_QUEUE_SIZE,
    SHUTDOWN,
    STOP_GW_RECEIVER,
    STOP_GW_SENDER,
    UNDEPLOY,
    BACKUP_MEMBERS,
    ROLL_DISKSTORE,
    FORCE_COMPACTION,
    FORCE_ROLL,
    FLUSH_DISKSTORE,
    START_GW_RECEIVER,
    START_GW_SENDER,
    BECOME_LOCK_GRANTOR,
    START_MANAGER,
    STOP_MANAGER,
    CREATE_MANAGER,
    STOP_CONTINUOUS_QUERY,
    SET_DISK_USAGE,
    CREATE_HDFS_STORE,
    ALTER_HDFS_STORE,
    DESTROY_HDFS_STORE,
    PULSE_DASHBOARD,
    PULSE_DATABROWSER,
    PULSE_WEBGFSH,
    PULSE_ADMIN;

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
