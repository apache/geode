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


package org.apache.geode.cache;

import java.io.ObjectStreamException;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.OpType;

/**
 * Enumerated type for an event operation. This class describes the operation that generated the
 * event.
 *
 *
 *
 * @see CacheEvent#getOperation
 *
 * @since GemFire 5.0
 */
@Immutable
public class Operation implements java.io.Serializable {
  private static final long serialVersionUID = -7521751729852504238L;

  @Immutable
  private static final Operation[] VALUES = new Operation[55];

  private static final byte OP_TYPE_CREATE = OpType.CREATE;
  private static final byte OP_TYPE_UPDATE = OpType.UPDATE;
  private static final byte OP_TYPE_GET = OpType.GET;
  private static final byte OP_TYPE_INVALIDATE = OpType.INVALIDATE;
  private static final byte OP_TYPE_GET_ENTRY = OpType.GET_ENTRY;
  private static final byte OP_TYPE_CONTAINS_KEY = OpType.CONTAINS_KEY;
  private static final byte OP_TYPE_CONTAINS_VALUE = OpType.CONTAINS_VALUE;
  private static final byte OP_TYPE_DESTROY = OpType.DESTROY;
  private static final byte OP_TYPE_CONTAINS_VALUE_FOR_KEY = OpType.CONTAINS_VALUE_FOR_KEY;
  private static final byte OP_TYPE_FUNCTION_EXECUTION = OpType.FUNCTION_EXECUTION;
  private static final byte OP_TYPE_CLEAR = OpType.CLEAR;
  private static final byte OP_TYPE_MARKER = OpType.MARKER;
  private static final byte OP_TYPE_UPDATE_VERSION = OpType.UPDATE_ENTRY_VERSION;
  private static final byte OP_TYPE_GET_FOR_REGISTER_INTEREST = OpType.GET_FOR_REGISTER_INTEREST;

  private static final int OP_DETAILS_NONE = 0;
  private static final int OP_DETAILS_SEARCH = 1;
  private static final int OP_DETAILS_LOCAL_LOAD = 2;
  private static final int OP_DETAILS_NET_LOAD = 4;
  private static final int OP_DETAILS_EXPIRE = 8;
  private static final int OP_DETAILS_EVICT = 16;
  private static final int OP_DETAILS_PUTALL = 32;
  private static final int OP_DETAILS_GUARANTEES_OLD_VALUE = 64;
  private static final int OP_DETAILS_REMOVEALL = 128;

  /*
   * TAKE NOTE!!! The order if the following static constructors calls must be maintained for
   * backwards compatibility. Any new operations need to be added to the end.
   */

  /**
   * A marker operation.
   */
  @Immutable
  public static final Operation MARKER = new Operation("MARKER", false, // isLocal
      false, // isRegion
      OP_TYPE_MARKER, OP_DETAILS_NONE, 0);

  /**
   * An entry creation.
   *
   * @see Region#create(Object, Object)
   */
  @Immutable
  public static final Operation CREATE = new Operation("CREATE", false, // isLocal
      false, // isRegion
      OP_TYPE_CREATE, OP_DETAILS_NONE, 1);

  /**
   * An entry creation caused by a putAll invocation
   *
   * @see Region#putAll
   */
  @Immutable
  public static final Operation PUTALL_CREATE = new Operation("PUTALL_CREATE", false, // isLocal
      false, // isRegion
      OP_TYPE_CREATE, OP_DETAILS_PUTALL, 2);

  /**
   * A 'value for key' operation.
   *
   * @see Region#get(Object)
   */
  @Immutable
  public static final Operation GET = new Operation("GET", false, // isLocal
      false, // isRegion
      OP_TYPE_GET, OP_DETAILS_NONE, 3);

  /**
   * A 'entry for key' operation.
   *
   * @see Region#getEntry(Object)
   */
  @Immutable
  public static final Operation GET_ENTRY = new Operation("GET_ENTRY", false, // isLocal
      false, // isRegion
      OP_TYPE_GET_ENTRY, OP_DETAILS_NONE, 4);

  /**
   * A 'check for existence of key' operation.
   *
   * @see Region#containsKey(Object)
   */
  @Immutable
  public static final Operation CONTAINS_KEY = new Operation("CONTAINS_KEY", false, // isLocal
      false, // isRegion
      OP_TYPE_CONTAINS_KEY, OP_DETAILS_NONE, 5);
  /**
   * A 'check for existence of value' operation.
   *
   * @see Region#containsValueForKey(Object)
   */
  @Immutable
  public static final Operation CONTAINS_VALUE = new Operation("CONTAINS_VALUE", false, // isLocal
      false, // isRegion
      OP_TYPE_CONTAINS_VALUE, OP_DETAILS_NONE, 6);

  /**
   * A 'check for existence of value for given key' operation.
   *
   * @see Region#containsValueForKey(Object)
   */
  @Immutable
  public static final Operation CONTAINS_VALUE_FOR_KEY =
      new Operation("CONTAINS_VALUE_FOR_KEY", false, // isLocal
          false, // isRegion
          OP_TYPE_CONTAINS_VALUE_FOR_KEY, OP_DETAILS_NONE, 7);

  /**
   * A 'function execution' operation.
   *
   * @see FunctionService
   */
  @Immutable
  public static final Operation FUNCTION_EXECUTION = new Operation("FUNCTION_EXECUTION", false, // isLocal
      false, // isRegion
      OP_TYPE_FUNCTION_EXECUTION, OP_DETAILS_NONE, 8);

  /**
   * An entry creation caused by a netsearch
   *
   * @see Region#get(Object)
   */
  @Immutable
  public static final Operation SEARCH_CREATE = new Operation("SEARCH_CREATE", false, // isLocal
      false, // isRegion
      OP_TYPE_CREATE, OP_DETAILS_SEARCH, 9);

  /**
   * An entry creation caused by a local loader
   *
   * @see Region#get(Object)
   * @see CacheLoader
   */
  @Immutable
  public static final Operation LOCAL_LOAD_CREATE = new Operation("LOCAL_LOAD_CREATE", false, // isLocal
      false, // isRegion
      OP_TYPE_CREATE, OP_DETAILS_LOCAL_LOAD, 10);
  /**
   * An entry creation caused by a net loader
   *
   * @see Region#get(Object)
   * @see CacheLoader
   */
  @Immutable
  public static final Operation NET_LOAD_CREATE = new Operation("NET_LOAD_CREATE", false, // isLocal
      false, // isRegion
      OP_TYPE_CREATE, OP_DETAILS_NET_LOAD, 11);

  /**
   * An entry update.
   *
   * @see Region#put(Object, Object)
   */
  @Immutable
  public static final Operation UPDATE = new Operation("UPDATE", false, // isLocal
      false, // isRegion
      OP_TYPE_UPDATE, OP_DETAILS_NONE, 12);

  /**
   * An entry update caused by a putAll invocation.
   *
   * @see Region#putAll
   */
  @Immutable
  public static final Operation PUTALL_UPDATE = new Operation("PUTALL_UPDATE", false, // isLocal
      false, // isRegion
      OP_TYPE_UPDATE, OP_DETAILS_PUTALL, 13);

  /**
   * An entry update caused by a net search.
   *
   * @see Region#get(Object)
   */
  @Immutable
  public static final Operation SEARCH_UPDATE = new Operation("SEARCH_UPDATE", false, // isLocal
      false, // isRegion
      OP_TYPE_UPDATE, OP_DETAILS_SEARCH, 14);

  /**
   * An entry update caused by a local load.
   *
   * @see Region#get(Object)
   * @see CacheLoader
   */
  @Immutable
  public static final Operation LOCAL_LOAD_UPDATE = new Operation("LOCAL_LOAD_UPDATE", false, // isLocal
      false, // isRegion
      OP_TYPE_UPDATE, OP_DETAILS_LOCAL_LOAD, 15);

  /**
   * An entry update caused by a net load.
   *
   * @see Region#get(Object)
   * @see CacheLoader
   */
  @Immutable
  public static final Operation NET_LOAD_UPDATE = new Operation("NET_LOAD_UPDATE", false, // isLocal
      false, // isRegion
      OP_TYPE_UPDATE, OP_DETAILS_NET_LOAD, 16);

  /**
   * An entry distributed invalidate.
   *
   * @see Region#invalidate(Object)
   */
  @Immutable
  public static final Operation INVALIDATE = new Operation("INVALIDATE", false, // isLocal
      false, // isRegion
      OP_TYPE_INVALIDATE, OP_DETAILS_NONE, 17);

  /**
   * An entry local invalidate.
   *
   * @see Region#localInvalidate(Object)
   */
  @Immutable
  public static final Operation LOCAL_INVALIDATE = new Operation("LOCAL_INVALIDATE", true, // isLocal
      false, // isRegion
      OP_TYPE_INVALIDATE, OP_DETAILS_NONE, 18);

  /**
   * An entry distributed destroy.
   *
   * @see Region#destroy(Object)
   */
  @Immutable
  public static final Operation DESTROY = new Operation("DESTROY", false, // isLocal
      false, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_NONE, 19);
  /**
   * An entry local destroy.
   *
   * @see Region#localDestroy(Object)
   */
  @Immutable
  public static final Operation LOCAL_DESTROY = new Operation("LOCAL_DESTROY", true, // isLocal
      false, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_NONE, 20);

  /**
   * An entry local destroy caused by an eviction.
   *
   * @see Region#localDestroy(Object)
   */
  @Immutable
  public static final Operation EVICT_DESTROY = new Operation("EVICT_DESTROY", true, // isLocal
      false, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_EVICT, 21);


  /**
   * A region load snapshot.
   *
   * @see Region#loadSnapshot
   */
  @Immutable
  public static final Operation REGION_LOAD_SNAPSHOT = new Operation("REGION_LOAD_SNAPSHOT", false, // isLocal
      true, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_NONE, 22);

  /**
   * A region local destroy.
   *
   * @see Region#localDestroyRegion()
   */
  @Immutable
  public static final Operation REGION_LOCAL_DESTROY = new Operation("REGION_LOCAL_DESTROY", true, // isLocal
      true, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_NONE, 23);

  /**
   * A region create.
   *
   * @see Region#createSubregion
   * @see Cache#createRegion
   */
  @Immutable
  public static final Operation REGION_CREATE = new Operation("REGION_CREATE", true, // isLocal
      true, // isRegion
      OP_TYPE_CREATE, OP_DETAILS_NONE, 24);

  /**
   * A region close
   *
   * @see Region#close
   */
  @Immutable
  public static final Operation REGION_CLOSE = new Operation("REGION_CLOSE", true, // isLocal
      true, // isRegion
      OP_TYPE_DESTROY, // @todo darrel: should close be a destroy?
      OP_DETAILS_NONE, 25);

  /**
   * A region distributed destroy.
   *
   * @see Region#destroyRegion()
   */
  @Immutable
  public static final Operation REGION_DESTROY = new Operation("REGION_DESTROY", false, // isLocal
      true, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_NONE, 26);

  /**
   * An entry distributed destroy triggered by expiration
   *
   * @see RegionAttributes#getEntryTimeToLive
   * @see RegionAttributes#getEntryIdleTimeout
   * @see ExpirationAction#DESTROY
   */
  @Immutable
  public static final Operation EXPIRE_DESTROY = new Operation("EXPIRE_DESTROY", false, // isLocal
      false, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_EXPIRE, 27);
  /**
   * An entry local destroy triggered by expiration
   *
   * @see RegionAttributes#getEntryTimeToLive
   * @see RegionAttributes#getEntryIdleTimeout
   * @see ExpirationAction#LOCAL_DESTROY
   */
  @Immutable
  public static final Operation EXPIRE_LOCAL_DESTROY = new Operation("EXPIRE_LOCAL_DESTROY", true, // isLocal
      false, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_EXPIRE, 28);
  /**
   * An entry distributed invalidate triggered by expiration
   *
   * @see RegionAttributes#getEntryTimeToLive
   * @see RegionAttributes#getEntryIdleTimeout
   * @see ExpirationAction#INVALIDATE
   */
  @Immutable
  public static final Operation EXPIRE_INVALIDATE = new Operation("EXPIRE_INVALIDATE", false, // isLocal
      false, // isRegion
      OP_TYPE_INVALIDATE, OP_DETAILS_EXPIRE, 29);

  /**
   * An entry local invalidate triggered by expiration
   *
   * @see RegionAttributes#getEntryTimeToLive
   * @see RegionAttributes#getEntryIdleTimeout
   * @see ExpirationAction#LOCAL_INVALIDATE
   */
  @Immutable
  public static final Operation EXPIRE_LOCAL_INVALIDATE =
      new Operation("EXPIRE_LOCAL_INVALIDATE", true, // isLocal
          false, // isRegion
          OP_TYPE_INVALIDATE, OP_DETAILS_EXPIRE, 30);

  /**
   * A region distributed destroy triggered by expiration
   *
   * @see RegionAttributes#getRegionTimeToLive
   * @see RegionAttributes#getRegionIdleTimeout
   * @see ExpirationAction#DESTROY
   */
  @Immutable
  public static final Operation REGION_EXPIRE_DESTROY =
      new Operation("REGION_EXPIRE_DESTROY", false, // isLocal
          true, // isRegion
          OP_TYPE_DESTROY, OP_DETAILS_EXPIRE, 31);
  /**
   * A region local destroy triggered by expiration
   *
   * @see RegionAttributes#getRegionTimeToLive
   * @see RegionAttributes#getRegionIdleTimeout
   * @see ExpirationAction#LOCAL_DESTROY
   */
  @Immutable
  public static final Operation REGION_EXPIRE_LOCAL_DESTROY =
      new Operation("REGION_EXPIRE_LOCAL_DESTROY", true, // isLocal
          true, // isRegion
          OP_TYPE_DESTROY, OP_DETAILS_EXPIRE, 32);
  /**
   * A region distributed invalidate triggered by expiration
   *
   * @see RegionAttributes#getRegionTimeToLive
   * @see RegionAttributes#getRegionIdleTimeout
   * @see ExpirationAction#INVALIDATE
   */
  @Immutable
  public static final Operation REGION_EXPIRE_INVALIDATE =
      new Operation("REGION_EXPIRE_INVALIDATE", false, // isLocal
          true, // isRegion
          OP_TYPE_INVALIDATE, OP_DETAILS_EXPIRE, 33);
  /**
   * A region local invalidate triggered by expiration
   *
   * @see RegionAttributes#getRegionTimeToLive
   * @see RegionAttributes#getRegionIdleTimeout
   * @see ExpirationAction#LOCAL_INVALIDATE
   */
  @Immutable
  public static final Operation REGION_EXPIRE_LOCAL_INVALIDATE =
      new Operation("REGION_EXPIRE_LOCAL_INVALIDATE", true, // isLocal
          true, // isRegion
          OP_TYPE_INVALIDATE, OP_DETAILS_EXPIRE, 34);
  /**
   * A region local invalidate.
   *
   * @see Region#localInvalidateRegion()
   */
  @Immutable
  public static final Operation REGION_LOCAL_INVALIDATE =
      new Operation("REGION_LOCAL_INVALIDATE", true, // isLocal
          true, // isRegion
          OP_TYPE_INVALIDATE, OP_DETAILS_NONE, 35);

  /**
   * A region distributed invalidate.
   *
   * @see Region#invalidateRegion()
   */
  @Immutable
  public static final Operation REGION_INVALIDATE = new Operation("REGION_INVALIDATE", false, // isLocal
      true, // isRegion
      OP_TYPE_INVALIDATE, OP_DETAILS_NONE, 36);

  /**
   * A region clear.
   *
   * @see Region#clear
   */
  @Immutable
  public static final Operation REGION_CLEAR = new Operation("REGION_CLEAR", false, // isLocal
      true, // isRegion
      OP_TYPE_CLEAR, OP_DETAILS_NONE, 37);
  /**
   * A region local clear.
   *
   * @see Region#localClear
   */
  @Immutable
  public static final Operation REGION_LOCAL_CLEAR = new Operation("REGION_LOCAL_CLEAR", true, // isLocal
      true, // isRegion
      OP_TYPE_CLEAR, OP_DETAILS_NONE, 38);

  /**
   * A cache create. Note that this is marked as a region operation.
   *
   * @see CacheFactory#create
   */
  @Immutable
  public static final Operation CACHE_CREATE = new Operation("CACHE_CREATE", true, // isLocal
      true, // isRegion
      OP_TYPE_CREATE, OP_DETAILS_NONE, 39);

  /**
   * A cache close. Note that this is marked as a region operation.
   *
   * @see Cache#close()
   */
  @Immutable
  public static final Operation CACHE_CLOSE = new Operation("CACHE_CLOSE", true, // isLocal
      true, // isRegion
      OP_TYPE_DESTROY, // @todo darrel: should close be a destroy?
      OP_DETAILS_NONE, 40);

  /**
   * A cache close due to being forced out of the distributed system by other members. This
   * typically happens when a member becomes unresponsive and does not respond to heartbeat requests
   * within the <a href="../distributed/DistributedSystem.html#member-timeout">"member-timeout"</a>
   * period.<br>
   * Note that this is marked as a region operation.
   */
  @Immutable
  public static final Operation FORCED_DISCONNECT = new Operation("FORCED_DISCONNECT", true, // isLocal
      true, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_NONE, 41);

  /**
   * A region destroy triggered by {@link ResumptionAction#REINITIALIZE}.
   *
   * @see ResumptionAction#REINITIALIZE
   */
  @Immutable
  public static final Operation REGION_REINITIALIZE = new Operation("REGION_REINITIALIZE", true, // isLocal
      true, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_NONE, 42);

  /**
   * A cache close triggered by {@link LossAction#RECONNECT}.
   *
   * @see LossAction#RECONNECT
   */
  @Immutable
  public static final Operation CACHE_RECONNECT = new Operation("CACHE_RECONNECT", true, // isLocal
      true, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_NONE, 43);

  /**
   * An atomic entry creation operation
   *
   * @see java.util.concurrent.ConcurrentMap#putIfAbsent(Object, Object)
   * @since GemFire 6.5
   */
  @Immutable
  public static final Operation PUT_IF_ABSENT = new Operation("PUT_IF_ABSENT", false, // isLocal
      false, // isRegion
      OP_TYPE_CREATE, OP_DETAILS_GUARANTEES_OLD_VALUE, 44);

  /**
   * An atomic update operation
   *
   * @see java.util.concurrent.ConcurrentMap#replace(Object, Object, Object)
   * @since GemFire 6.5
   */
  @Immutable
  public static final Operation REPLACE = new Operation("REPLACE", false, // isLocal
      false, // isRegion
      OP_TYPE_UPDATE, OP_DETAILS_GUARANTEES_OLD_VALUE, 45);

  /**
   * An atomic destroy destroy operation
   *
   * @see java.util.concurrent.ConcurrentMap#remove(Object, Object)
   * @since GemFire 6.5
   */
  @Immutable
  public static final Operation REMOVE = new Operation("REMOVE", false, // isLocal
      false, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_NONE, 46);

  /**
   * An internal operation used to update the version stamp of an entry.
   */
  @Immutable
  public static final Operation UPDATE_VERSION_STAMP = new Operation("UPDATE_VERSION", false, // isLocal
      false, // isRegion
      OP_TYPE_UPDATE_VERSION, // opType
      OP_DETAILS_NONE // opDetails
      , 47);

  /**
   * An entry distributed destroy caused by a removeAll.
   *
   * @see Region#removeAll(java.util.Collection)
   * @since GemFire 8.1
   */
  @Immutable
  public static final Operation REMOVEALL_DESTROY = new Operation("REMOVEALL_DESTROY", false, // isLocal
      false, // isRegion
      OP_TYPE_DESTROY, OP_DETAILS_REMOVEALL, 48);

  /**
   * A 'get for register interest' operation.
   */
  @Immutable
  public static final Operation GET_FOR_REGISTER_INTEREST =
      new Operation("GET_FOR_REGISTER_INTEREST", false, // isLocal
          false, // isRegion
          OP_TYPE_GET_FOR_REGISTER_INTEREST, OP_DETAILS_NONE, 49);

  /** The name of this mirror type. */
  private final transient String name;

  /** byte used as ordinal to represent this Operation */
  public final byte ordinal;

  /** True if a local op; false if distributed op. */
  private final transient boolean isLocal;

  /** True if a region operation; false if entry op. */
  private final transient boolean isRegion;

  /**
   * One of the following: OP_TYPE_CREATE, OP_TYPE_UPDATE, OP_TYPE_INVALIDATE, OP_TYPE_DESTROY,
   * OP_TYPE_CLEAR
   */
  private final transient byte opType;

  /**
   * One of the following: OP_DETAILS_NONE, OP_DETAILS_SEARCH, OP_DETAILS_LOCAL_LOAD,
   * OP_DETAILS_NET_LOAD, OP_DETAILS_EXPIRE, OP_DETAILS_EVICT OP_DETAILS_PUTALL
   * OP_DETAILS_GUARANTEES_OLD_VALUE OP_DETAILS_REMOVEALL
   */
  private final transient int opDetails;


  private Object readResolve() throws ObjectStreamException {
    return VALUES[ordinal]; // Canonicalize
  }


  /** Creates a new instance of Operation. */
  private Operation(String name, boolean isLocal, boolean isRegion, byte opType, int opDetails,
      int ordinal) {
    this.name = name;
    this.isLocal = isLocal;
    this.isRegion = isRegion;
    this.opType = opType;
    this.opDetails = opDetails;
    this.ordinal = (byte) ordinal;
    VALUES[this.ordinal] = this;
  }

  /** Return the Operation represented by specified ordinal */
  public static Operation fromOrdinal(byte ordinal) {
    return VALUES[ordinal];
  }


  /**
   * Returns true if this operation created a new entry.
   */
  public boolean isCreate() {
    return this.opType == OP_TYPE_CREATE && isEntry();
  }

  /**
   * Returns true if this operation updated an existing entry.
   */
  public boolean isUpdate() {
    return this.opType == OP_TYPE_UPDATE && isEntry();
  }

  /**
   * Returns true if this operation gets the value for given key.
   */
  public boolean isGet() {
    return this.opType == OP_TYPE_GET;
  }

  /**
   * Returns true if this operation checks whether given key is present in region.
   */
  public boolean isContainsKey() {
    return this.opType == OP_TYPE_CONTAINS_KEY;
  }

  /**
   * Returns true if this operation checks whether given value is present in region.
   */
  public boolean isContainsValue() {
    return this.opType == OP_TYPE_CONTAINS_VALUE;
  }

  /**
   * Returns true if this operation checks whether value is present for the given key.
   */
  public boolean isContainsValueForKey() {
    return this.opType == OP_TYPE_CONTAINS_VALUE_FOR_KEY;
  }

  /**
   * Returns true if this operation is function execution operation.
   */
  public boolean isFunctionExecution() {
    return this.opType == OP_TYPE_FUNCTION_EXECUTION;
  }

  /**
   * Returns true if this operation gets the entry for given key.
   */
  public boolean isGetEntry() {
    return this.opType == OP_TYPE_GET_ENTRY;
  }

  /**
   * Returns true if this operation is a get for register interest.
   */
  public boolean isGetForRegisterInterest() {
    return this.opType == OP_TYPE_GET_FOR_REGISTER_INTEREST;
  }

  /**
   * Returns true if the operation invalidated an entry.
   */
  public boolean isInvalidate() {
    return this.opType == OP_TYPE_INVALIDATE && isEntry();
  }

  /**
   * Returns true if the operation destroyed an entry.
   */
  public boolean isDestroy() {
    return this.opType == OP_TYPE_DESTROY && isEntry();
  }

  /**
   * Returns true if the operation cleared the region.
   */
  public boolean isClear() {
    return this.opType == OP_TYPE_CLEAR;
  }

  /**
   * Returns true if the operation closed the cache or a region.
   */
  public boolean isClose() {
    return (this == REGION_CLOSE) || (this == CACHE_CLOSE) || (this == CACHE_RECONNECT)
        || (this == FORCED_DISCONNECT);
  }

  /**
   * Returns true if this operation was initiated by a putAll.
   */
  public boolean isPutAll() {
    return (this.opDetails & OP_DETAILS_PUTALL) != 0;
  }

  /**
   * Returns true if this operation was initiated by a removeAll.
   *
   * @see Region#removeAll(java.util.Collection)
   * @since GemFire 8.1
   */
  public boolean isRemoveAll() {
    return (this.opDetails & OP_DETAILS_REMOVEALL) != 0;
  }

  /**
   * Returns true if the operation invalidated a region.
   */
  public boolean isRegionInvalidate() {
    return this.opType == OP_TYPE_INVALIDATE && isRegion();
  }

  /**
   * Returns true if the operation destroyed a region.
   */
  public boolean isRegionDestroy() {
    return this.opType == OP_TYPE_DESTROY && isRegion();
  }

  /**
   * Returns true if the operation applies to the entire region.
   */
  public boolean isRegion() {
    return this.isRegion;
  }

  /**
   * Returns true if the operation is limited to the local cache.
   */
  public boolean isLocal() {
    return this.isLocal;
  }

  /**
   * Returns true if the operation may be distributed.
   */
  public boolean isDistributed() {
    return !isLocal();
  }

  /**
   * Returns true if the operation applies to a single entry.
   */
  public boolean isEntry() {
    return !isRegion();
  }

  /**
   * Answer true if this operation resulted from expiration.
   *
   * @return true if this operation resulted from expiration
   *
   */
  public boolean isExpiration() {
    return (this.opDetails & OP_DETAILS_EXPIRE) != 0;
  }

  /**
   * Answer true if this operation resulted from eviction
   *
   * @return true if this operatino resulted from eviction
   */
  public boolean isEviction() {
    return (this.opDetails & OP_DETAILS_EVICT) != 0;
  }

  /**
   * Returns true if this operation included a loader running in this cache. Note that this will be
   * true even if the local loader called <code>netSearch</code>.
   *
   * If this operation is for a Partitioned Region, then true will be returned if the loader ran in
   * the same VM as where the data is hosted. If true is returned, and
   * {@link CacheEvent#isOriginRemote} is true, it means the data is not hosted locally, but the
   * loader was run local to the data.
   *
   * @return true if this operation included a local loader execution
   */
  public boolean isLocalLoad() {
    return (this.opDetails & OP_DETAILS_LOCAL_LOAD) != 0;
  }

  /**
   * Returns true if this operation included a loader running that was remote from the cache that
   * requested it, i.e., a netLoad. Note that the cache that requested the netLoad may not be this
   * cache.
   *
   * @return true if this operation included a netLoad
   */
  public boolean isNetLoad() {
    return (this.opDetails & OP_DETAILS_NET_LOAD) != 0;
  }

  /**
   * Returns true if this operation included running a loader.
   *
   * @return true if isLocalLoad or isNetLoad
   */
  public boolean isLoad() {
    return (this.opDetails & (OP_DETAILS_LOCAL_LOAD | OP_DETAILS_NET_LOAD)) != 0;
  }

  /**
   * Returns true if this operation included a <code>netSearch</code>. If the <code>netSearch</code>
   * was invoked by a loader however, this will return false and <code>isLocalLoad()</code> or
   * <code>isNetLoad()</code> will return true instead.
   *
   * @return true if this operation included a netSearch
   */
  public boolean isNetSearch() {
    return (this.opDetails & OP_DETAILS_SEARCH) != 0;
  }

  /**
   * Returns true if this operation was a {@link #isNetSearch net search} or a {@link #isLoad load}.
   *
   * @return true if this operation include a netSearch or any type of load.
   */
  public boolean isSearchOrLoad() {
    return (this.opDetails
        & (OP_DETAILS_SEARCH | OP_DETAILS_LOCAL_LOAD | OP_DETAILS_NET_LOAD)) != 0;
  }

  /**
   * Returns true if this operation is a ConcurrentMap operation that guarantees the old value to be
   * returned no matter what expense may be incurred in doing so.
   *
   * @return true if this operation has this guarantee
   * @since GemFire 6.5
   */
  public boolean guaranteesOldValue() {
    return (this.opDetails & OP_DETAILS_GUARANTEES_OLD_VALUE) != 0;
  }

  /**
   * Returns the update operation that corresponds to this operation. For a create operation the
   * corresponding update op is returned. For all other operations <code>this</code> is returned.
   */
  public Operation getCorrespondingUpdateOp() {
    if (isCreate()) {
      switch (this.opDetails) {
        case OP_DETAILS_SEARCH:
          return Operation.SEARCH_UPDATE;
        case OP_DETAILS_LOCAL_LOAD:
          return Operation.LOCAL_LOAD_UPDATE;
        case OP_DETAILS_NET_LOAD:
          return Operation.NET_LOAD_UPDATE;
        case OP_DETAILS_PUTALL:
          return Operation.PUTALL_UPDATE;
        default:
          return Operation.UPDATE;
      }
    } else {
      return this;
    }
  }

  /**
   * Returns the create operation that corresponds to this operation. For an update operation the
   * corresponding create op is returned. For all other operations <code>this</code> is returned.
   */
  public Operation getCorrespondingCreateOp() {
    if (isUpdate()) {
      switch (this.opDetails) {
        case OP_DETAILS_SEARCH:
          return Operation.SEARCH_CREATE;
        case OP_DETAILS_LOCAL_LOAD:
          return Operation.LOCAL_LOAD_CREATE;
        case OP_DETAILS_NET_LOAD:
          return Operation.NET_LOAD_CREATE;
        case OP_DETAILS_PUTALL:
          return Operation.PUTALL_CREATE;
        default:
          return Operation.CREATE;
      }
    } else {
      return this;
    }
  }

  /**
   * Returns a string representation for this operation.
   *
   * @return the name of this operation.
   */
  @Override
  public String toString() {
    return this.name;
  }
}
