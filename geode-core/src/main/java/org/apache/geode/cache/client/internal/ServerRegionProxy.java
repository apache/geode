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
package org.apache.geode.cache.client.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ContainsKeyOp.MODE;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.EventIDHolder;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.execute.ServerRegionFunctionExecutor;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList.Iterator;
import org.apache.geode.internal.cache.tx.ClientTXStateStub;
import org.apache.geode.internal.cache.tx.TransactionalOperation.ServerRegionOperation;
import org.apache.geode.internal.logging.LogService;

/**
 * Used to send region operations from a client to a server
 *
 * @since GemFire 5.7
 */
@SuppressWarnings("deprecation")
public class ServerRegionProxy extends ServerProxy implements ServerRegionDataAccess {
  private static final Logger logger = LogService.getLogger();

  private final LocalRegion region;
  private final String regionName;


  /**
   * Creates a server region proxy for the given region.
   *
   * @param r the region
   * @throws IllegalStateException if the region does not have a pool
   */
  public ServerRegionProxy(Region r) {
    super(calcPool(r));
    assert r instanceof LocalRegion;
    this.region = (LocalRegion) r;
    this.regionName = r.getFullPath();
  }

  /**
   * Used by tests to create proxies for "fake" regions. Also, used by ClientStatsManager for admin
   * region.
   */
  public ServerRegionProxy(String regionName, PoolImpl pool) {
    super(pool);
    this.region = null;
    this.regionName = regionName;
  }

  private static InternalPool calcPool(Region r) {
    String poolName = r.getAttributes().getPoolName();
    if (poolName == null || "".equals(poolName)) {
      throw new IllegalStateException(
          "The region " + r.getFullPath() + " did not have a client pool configured.");
    } else {
      InternalPool pool = (InternalPool) PoolManager.find(poolName);
      if (pool == null) {
        throw new IllegalStateException("The pool " + poolName + " does not exist.");
      }
      return pool;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.client.internal.ServerRegionDataAccess#get(java.lang.Object,
   * java.lang.Object)
   */
  public Object get(Object key, Object callbackArg, EntryEventImpl clientEvent) {
    recordTXOperation(ServerRegionOperation.GET, key, callbackArg);
    return GetOp.execute(this.pool, this.region, key, callbackArg,
        this.pool.getPRSingleHopEnabled(), clientEvent);
  }



  public int size() {
    return SizeOp.execute(this.pool, this.regionName);
  }

  /**
   * Do not call this method if the value is Delta instance. Exclicitly passing
   * <code>Operation.CREATE</code> to the <code>PutOp.execute()</code> method as the caller of this
   * method does not put Delta instances as value.
   *
   */
  public Object putForMetaRegion(Object key, Object value, byte[] deltaBytes, EntryEventImpl event,
      Object callbackArg, boolean isMetaRegionPutOp) {
    if (this.region == null) {
      return PutOp.execute(this.pool, this.regionName, key, value, deltaBytes, event,
          Operation.CREATE, false, null, callbackArg, this.pool.getPRSingleHopEnabled());
    } else {
      return PutOp.execute(this.pool, this.region, key, value, deltaBytes, event, Operation.CREATE,
          false, null, callbackArg, this.pool.getPRSingleHopEnabled());
    }
  }

  public Object put(Object key, Object value, byte[] deltaBytes, EntryEventImpl event, Operation op,
      boolean requireOldValue, Object expectedOldValue, Object callbackArg, boolean isCreate) {
    recordTXOperation(ServerRegionOperation.PUT, key, value, deltaBytes, event.getEventId(), op,
        Boolean.valueOf(requireOldValue), expectedOldValue, callbackArg, Boolean.valueOf(isCreate));
    Operation operation = op;
    if (!isCreate && this.region.getDataPolicy() == DataPolicy.EMPTY && op.isCreate()
        && op != Operation.PUT_IF_ABSENT) {
      operation = Operation.UPDATE;
    }

    if (this.region == null) {
      return PutOp.execute(this.pool, this.regionName, key, value, deltaBytes, event, operation,
          requireOldValue, expectedOldValue, callbackArg, this.pool.getPRSingleHopEnabled());
    } else {
      return PutOp.execute(this.pool, this.region, key, value, deltaBytes, event, operation,
          requireOldValue, expectedOldValue, callbackArg, this.pool.getPRSingleHopEnabled());
    }
  }


  /**
   * Does a region put on the server using the given connection.
   *
   * @param con the connection to use to send to the server
   * @param key the entry key to do the put on
   * @param value the entry value to put
   * @param eventId the event ID for this put
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public void putOnForTestsOnly(Connection con, Object key, Object value, EventID eventId,
      Object callbackArg) {
    EventIDHolder event = new EventIDHolder(eventId);
    PutOp.execute(con, this.pool, this.regionName, key, value, event, callbackArg,
        this.pool.getPRSingleHopEnabled());
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.client.internal.ServerRegionDataAccess#destroy(java.lang.Object,
   * java.lang.Object, org.apache.geode.cache.Operation, org.apache.geode.internal.cache.EventID,
   * java.lang.Object)
   */
  public Object destroy(Object key, Object expectedOldValue, Operation operation,
      EntryEventImpl event, Object callbackArg) {
    if (event.isBulkOpInProgress()) {
      // this is a removeAll, ignore this!
      return null;
    }
    recordTXOperation(ServerRegionOperation.DESTROY, key, expectedOldValue, operation,
        event.getEventId(), callbackArg);
    return DestroyOp.execute(this.pool, this.region, key, expectedOldValue, operation, event,
        callbackArg, this.pool.getPRSingleHopEnabled());
  }


  public void invalidate(EntryEventImpl event) {
    recordTXOperation(ServerRegionOperation.INVALIDATE, event.getKey(), event);
    InvalidateOp.execute(this.pool, this.region.getFullPath(), event);
  }


  /**
   * Does a region entry destroy on the server using the given connection.
   *
   * @param con the connection to use to send to the server
   * @param key the entry key to do the destroy on
   * @param expectedOldValue the value that the entry must have to perform the operation, or null
   * @param operation the operation being performed (Operation.DESTROY, Operation.REMOVE)
   * @param event the event for this destroy operation
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public void destroyOnForTestsOnly(Connection con, Object key, Object expectedOldValue,
      Operation operation, EntryEventImpl event, Object callbackArg) {
    DestroyOp.execute(con, this.pool, this.regionName, key, expectedOldValue, operation, event,
        callbackArg);
  }

  /**
   * Does a region destroy on the server
   *
   * @param eventId the event id for this destroy
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public void destroyRegion(EventID eventId, Object callbackArg) {
    DestroyRegionOp.execute(this.pool, this.regionName, eventId, callbackArg);
  }

  /**
   * Does a region destroy on the server using the given connection.
   *
   * @param con the connection to use to send to the server
   * @param eventId the event id for this destroy
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public void destroyRegionOnForTestsOnly(Connection con, EventID eventId, Object callbackArg) {
    DestroyRegionOp.execute(con, this.pool, this.regionName, eventId, callbackArg);
  }

  public TXCommitMessage commit(int txId) {
    TXCommitMessage tx = CommitOp.execute(this.pool, txId);
    return tx;
  }

  public void rollback(int txId) {
    RollbackOp.execute(this.pool, txId);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.client.internal.ServerRegionDataAccess#clear(org.apache.geode.internal.
   * cache.EventID, java.lang.Object)
   */
  public void clear(EventID eventId, Object callbackArg) {
    ClearOp.execute(this.pool, this.regionName, eventId, callbackArg);
  }

  /**
   * Does a region clear on the server using the given connection.
   *
   * @param con the connection to use to send to the server
   * @param eventId the event id for this clear
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public void clearOnForTestsOnly(Connection con, EventID eventId, Object callbackArg) {
    ClearOp.execute(con, this.pool, this.regionName, eventId, callbackArg);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.client.internal.ServerRegionDataAccess#containsKey(java.lang.Object)
   */
  public boolean containsKey(Object key) {
    recordTXOperation(ServerRegionOperation.CONTAINS_KEY, key);
    return ContainsKeyOp.execute(this.pool, this.regionName, key, MODE.KEY);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.client.internal.ServerRegionDataAccess#containsKey(java.lang.Object)
   */
  public boolean containsValueForKey(Object key) {
    recordTXOperation(ServerRegionOperation.CONTAINS_VALUE_FOR_KEY, key);
    return ContainsKeyOp.execute(this.pool, this.regionName, key, MODE.VALUE_FOR_KEY);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.client.internal.ServerRegionDataAccess#containsKey(java.lang.Object)
   */
  public boolean containsValue(Object value) {
    recordTXOperation(ServerRegionOperation.CONTAINS_VALUE, null, value);
    return ContainsKeyOp.execute(this.pool, this.regionName, value, MODE.VALUE);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.client.internal.ServerRegionDataAccess#keySet()
   */
  public Set keySet() {
    recordTXOperation(ServerRegionOperation.KEY_SET, null);
    return KeySetOp.execute(this.pool, this.regionName);
  }

  /**
   * Does a region registerInterest on a server
   *
   * @param key describes what we are interested in
   * @param interestType the {@link InterestType} for this registration
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public List registerInterest(final Object key, final int interestType,
      final InterestResultPolicy policy, final boolean isDurable, final byte regionDataPolicy) {
    return registerInterest(key, interestType, policy, isDurable, false, regionDataPolicy);
  }

  /**
   * Does a region registerInterest on a server
   *
   * @param key describes what we are interested in
   * @param interestType the {@link InterestType} for this registration
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param receiveUpdatesAsInvalidates whether to act like notify-by-subscription is false.
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public List registerInterest(final Object key, final int interestType,
      final InterestResultPolicy policy, final boolean isDurable,
      final boolean receiveUpdatesAsInvalidates, final byte regionDataPolicy) {
    if (interestType == InterestType.KEY && key instanceof List) {
      logger.warn(
          "Usage of registerInterest(List) has been deprecated. Please use registerInterestForKeys(Iterable)");
      return registerInterestList((List) key, policy, isDurable, receiveUpdatesAsInvalidates,
          regionDataPolicy);
    } else {
      final RegisterInterestTracker rit = this.pool.getRITracker();
      List result = null;
      boolean finished = false;
      try {
        // register with the tracker early
        rit.addSingleInterest(this.region, key, interestType, policy, isDurable,
            receiveUpdatesAsInvalidates);
        result = RegisterInterestOp.execute(this.pool, this.regionName, key, interestType, policy,
            isDurable, receiveUpdatesAsInvalidates, regionDataPolicy);
        //////// TEST PURPOSE ONLY ///////////
        if (PoolImpl.AFTER_REGISTER_CALLBACK_FLAG) {
          ClientServerObserver bo = ClientServerObserverHolder.getInstance();
          bo.afterInterestRegistration();
        }
        /////////////////////////////////////////
        finished = true;
        return result;
      } finally {
        if (!finished) {
          rit.removeSingleInterest(this.region, key, interestType, isDurable,
              receiveUpdatesAsInvalidates);
        }
      }
    }
  }

  /**
   * Support for server-side interest registration
   */
  public void addSingleInterest(Object key, int interestType, InterestResultPolicy pol,
      boolean isDurable, boolean receiveUpdatesAsInvalidates) {
    RegisterInterestTracker rit = this.pool.getRITracker();
    boolean finished = false;
    try {
      rit.addSingleInterest(this.region, key, interestType, pol, isDurable,
          receiveUpdatesAsInvalidates);
      finished = true;
    } finally {
      if (!finished) {
        rit.removeSingleInterest(this.region, key, interestType, isDurable,
            receiveUpdatesAsInvalidates);
      }
    }
  }

  public void addListInterest(List keys, InterestResultPolicy pol, boolean isDurable,
      boolean receiveUpdatesAsInvalidates) {
    RegisterInterestTracker rit = this.pool.getRITracker();
    boolean finished = false;
    try {
      rit.addInterestList(this.region, keys, pol, isDurable, receiveUpdatesAsInvalidates);
      finished = true;
    } finally {
      if (!finished) {
        rit.removeInterestList(this.region, keys, isDurable, receiveUpdatesAsInvalidates);
      }
    }
  }

  /**
   * Support for server-side interest registration
   */
  public void removeSingleInterest(Object key, int interestType, boolean isDurable,
      boolean receiveUpdatesAsInvalidates) {
    this.pool.getRITracker().removeSingleInterest(this.region, key, interestType, isDurable,
        receiveUpdatesAsInvalidates);
  }

  public void removeListInterest(List keys, boolean isDurable,
      boolean receiveUpdatesAsInvalidates) {
    this.pool.getRITracker().removeInterestList(this.region, keys, isDurable,
        receiveUpdatesAsInvalidates);
  }

  /**
   * Does a region registerInterest on a server described by the given server location
   * <p>
   * Note that this call by-passes the RegisterInterestTracker.
   *
   * @param sl the server to do the register interest on.
   * @param key describes what we are interested in
   * @param interestType the {@link InterestType} for this registration
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public List registerInterestOn(ServerLocation sl, final Object key, final int interestType,
      final InterestResultPolicy policy, final boolean isDurable, final byte regionDataPolicy) {
    return registerInterestOn(sl, key, interestType, policy, isDurable, false, regionDataPolicy);
  }

  /**
   * Does a region registerInterest on a server described by the given server location
   * <p>
   * Note that this call by-passes the RegisterInterestTracker.
   *
   * @param sl the server to do the register interest on.
   * @param key describes what we are interested in
   * @param interestType the {@link InterestType} for this registration
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param receiveUpdatesAsInvalidates whether to act like notify-by-subscription is false.
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public List registerInterestOn(ServerLocation sl, final Object key, final int interestType,
      final InterestResultPolicy policy, final boolean isDurable,
      final boolean receiveUpdatesAsInvalidates, final byte regionDataPolicy) {
    if (interestType == InterestType.KEY && key instanceof List) {
      return RegisterInterestListOp.executeOn(sl, this.pool, this.regionName, (List) key, policy,
          isDurable, receiveUpdatesAsInvalidates, regionDataPolicy);
    } else {
      return RegisterInterestOp.executeOn(sl, this.pool, this.regionName, key, interestType, policy,
          isDurable, receiveUpdatesAsInvalidates, regionDataPolicy);
    }
  }

  /**
   * Does a region registerInterest on a server described by the given connection
   * <p>
   * Note that this call by-passes the RegisterInterestTracker.
   *
   * @param conn the connection to do the register interest on.
   * @param key describes what we are interested in
   * @param interestType the {@link InterestType} for this registration
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public List registerInterestOn(Connection conn, final Object key, final int interestType,
      final InterestResultPolicy policy, final boolean isDurable, final byte regionDataPolicy) {
    return registerInterestOn(conn, key, interestType, policy, isDurable, false, regionDataPolicy);
  }

  /**
   * Does a region registerInterest on a server described by the given connection
   * <p>
   * Note that this call by-passes the RegisterInterestTracker.
   *
   * @param conn the connection to do the register interest on.
   * @param key describes what we are interested in
   * @param interestType the {@link InterestType} for this registration
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param receiveUpdatesAsInvalidates whether to act like notify-by-subscription is false.
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public List registerInterestOn(Connection conn, final Object key, final int interestType,
      final InterestResultPolicy policy, final boolean isDurable,
      final boolean receiveUpdatesAsInvalidates, final byte regionDataPolicy) {
    if (interestType == InterestType.KEY && key instanceof List) {
      return RegisterInterestListOp.executeOn(conn, this.pool, this.regionName, (List) key, policy,
          isDurable, receiveUpdatesAsInvalidates, regionDataPolicy);
    } else {
      return RegisterInterestOp.executeOn(conn, this.pool, this.regionName, key, interestType,
          policy, isDurable, receiveUpdatesAsInvalidates, regionDataPolicy);
    }
  }



  /**
   * Does a region registerInterestList on a server
   *
   * @param keys list of keys we are interested in
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public List registerInterestList(List keys, InterestResultPolicy policy, boolean isDurable,
      boolean receiveUpdatesAsInvalidates, final byte regionDataPolicy) {
    final RegisterInterestTracker rit = this.pool.getRITracker();
    List result = null;
    boolean finished = false;
    try {
      // register with the tracker early
      rit.addInterestList(this.region, keys, policy, isDurable, receiveUpdatesAsInvalidates);
      result = RegisterInterestListOp.execute(this.pool, this.regionName, keys, policy, isDurable,
          receiveUpdatesAsInvalidates, regionDataPolicy);
      finished = true;
      //////// TEST PURPOSE ONLY ///////////
      if (PoolImpl.AFTER_REGISTER_CALLBACK_FLAG) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.afterInterestRegistration();
      }
      /////////////////////////////////////////
      return result;
    } finally {
      if (!finished) {
        rit.removeInterestList(this.region, keys, isDurable, receiveUpdatesAsInvalidates);
      }
    }
  }

  /**
   * Does a region unregisterInterest on a server
   *
   * @param key describes what we are no longer interested in
   * @param interestType the {@link InterestType} for this unregister
   * @param isClosing true if this unregister is done by a close
   * @param keepAlive true if this unregister should not undo a durable registration
   */
  public void unregisterInterest(Object key, int interestType, boolean isClosing,
      boolean keepAlive) {
    if (interestType == InterestType.KEY && key instanceof List) {
      unregisterInterestList((List) key, isClosing, keepAlive);
    } else {
      RegisterInterestTracker rit = this.pool.getRITracker();
      boolean removed = rit.removeSingleInterest(this.region, key, interestType, false, false)
          || rit.removeSingleInterest(this.region, key, interestType, true, false)
          || rit.removeSingleInterest(this.region, key, interestType, false, true)
          || rit.removeSingleInterest(this.region, key, interestType, true, true);
      if (removed) {
        UnregisterInterestOp.execute(this.pool, this.regionName, key, interestType, isClosing,
            keepAlive);
      }
    }
  }

  /**
   * Does a region unregisterInterestList on a server
   *
   * @param keys list of keys we are interested in
   * @param isClosing true if this unregister is done by a close
   * @param keepAlive true if this unregister should not undo a durable registration
   */
  public void unregisterInterestList(List keys, boolean isClosing, boolean keepAlive) {
    RegisterInterestTracker rit = this.pool.getRITracker();
    boolean removed = rit.removeInterestList(this.region, keys, false, true)
        || rit.removeInterestList(this.region, keys, false, false)
        || rit.removeInterestList(this.region, keys, true, true)
        || rit.removeInterestList(this.region, keys, true, false);
    if (removed) {
      UnregisterInterestListOp.execute(this.pool, this.regionName, keys, isClosing, keepAlive);
    }
  }

  public List getInterestList(int interestType) {
    return this.pool.getRITracker().getInterestList(this.regionName, interestType);
  }

  @Override
  public VersionedObjectList putAll(Map map, EventID eventId, boolean skipCallbacks,
      Object callbackArg) {
    recordTXOperation(ServerRegionOperation.PUT_ALL, null, map, eventId);
    int txID = TXManagerImpl.getCurrentTXUniqueId();
    if (this.pool.getPRSingleHopEnabled() && (txID == TXManagerImpl.NOTX)) {
      return PutAllOp.execute(this.pool, this.region, map, eventId, skipCallbacks,
          this.pool.getRetryAttempts(), callbackArg);
    } else {
      return PutAllOp.execute(this.pool, this.region, map, eventId, skipCallbacks, false,
          callbackArg);
    }
  }

  @Override
  public VersionedObjectList removeAll(Collection<Object> keys, EventID eventId,
      Object callbackArg) {
    recordTXOperation(ServerRegionOperation.REMOVE_ALL, null, keys, eventId);
    int txID = TXManagerImpl.getCurrentTXUniqueId();
    if (this.pool.getPRSingleHopEnabled() && (txID == TXManagerImpl.NOTX)) {
      return RemoveAllOp.execute(this.pool, this.region, keys, eventId,
          this.pool.getRetryAttempts(), callbackArg);
    } else {
      return RemoveAllOp.execute(this.pool, this.region, keys, eventId, false, callbackArg);
    }
  }


  @Override
  public VersionedObjectList getAll(List keys, Object callback) {
    recordTXOperation(ServerRegionOperation.GET_ALL, null, keys);
    int txID = TXManagerImpl.getCurrentTXUniqueId();
    VersionedObjectList result;
    if (this.pool.getPRSingleHopEnabled() && (txID == TXManagerImpl.NOTX)) {
      result =
          GetAllOp.execute(this.pool, this.region, keys, this.pool.getRetryAttempts(), callback);
    } else {
      result = GetAllOp.execute(this.pool, this.regionName, keys, callback);
    }
    if (result != null) {
      for (Iterator it = result.iterator(); it.hasNext();) {
        VersionedObjectList.Entry entry = it.next();
        Object key = entry.getKey();
        Object value = entry.getValue();
        boolean isOnServer = entry.isKeyNotOnServer();
        if (!isOnServer) {
          if (value instanceof Throwable) {
            logger.warn(String.format(
                "%s: Caught the following exception attempting to get value for key=%s",
                new Object[] {value, key}),
                (Throwable) value);
          }
        }
      }
    }
    return result;
  }

  /**
   * Release use of this pool
   */
  public void detach(boolean keepalive) {
    this.pool.getRITracker().unregisterRegion(this, keepalive);
    super.detach();
  }

  public String getRegionName() {
    return this.regionName;
  }

  public Region getRegion() {
    return this.region;
  }

  public void executeFunction(String rgnName, Function function,
      ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector resultCollector,
      byte hasResult, boolean replaying) {

    recordTXOperation(ServerRegionOperation.EXECUTE_FUNCTION, null, Integer.valueOf(1), function,
        serverRegionExecutor, resultCollector, Byte.valueOf(hasResult));

    int retryAttempts = pool.getRetryAttempts();

    if (this.pool.getPRSingleHopEnabled()) {
      ClientMetadataService cms = region.getCache().getClientMetadataService();
      if (cms.isMetadataStable()) {
        if (serverRegionExecutor.getFilter().isEmpty()) {
          HashMap<ServerLocation, HashSet<Integer>> serverToBuckets =
              cms.groupByServerToAllBuckets(this.region, function.optimizeForWrite());
          if (serverToBuckets == null || serverToBuckets.isEmpty()) {
            ExecuteRegionFunctionOp.execute(this.pool, rgnName, function, serverRegionExecutor,
                resultCollector, hasResult, retryAttempts);
            cms.scheduleGetPRMetaData(region, false);
          } else {
            ExecuteRegionFunctionSingleHopOp.execute(this.pool, this.region, function,
                serverRegionExecutor, resultCollector, hasResult, serverToBuckets, retryAttempts,
                true);
          }
        } else {
          boolean isBucketFilter = serverRegionExecutor.getExecuteOnBucketSetFlag();
          Map<ServerLocation, HashSet> serverToFilterMap =
              cms.getServerToFilterMap(serverRegionExecutor.getFilter(), region,
                  function.optimizeForWrite(), isBucketFilter);
          if (serverToFilterMap == null || serverToFilterMap.isEmpty()) {
            ExecuteRegionFunctionOp.execute(this.pool, rgnName, function, serverRegionExecutor,
                resultCollector, hasResult, retryAttempts);
            cms.scheduleGetPRMetaData(region, false);
          } else {
            ExecuteRegionFunctionSingleHopOp.execute(this.pool, this.region, function,
                serverRegionExecutor, resultCollector, hasResult, serverToFilterMap, retryAttempts,
                isBucketFilter);
          }
        }
      } else {
        cms.scheduleGetPRMetaData(region, false);
        ExecuteRegionFunctionOp.execute(this.pool, rgnName, function, serverRegionExecutor,
            resultCollector, hasResult, retryAttempts);
      }
    } else {
      ExecuteRegionFunctionOp.execute(this.pool, rgnName, function, serverRegionExecutor,
          resultCollector, hasResult, retryAttempts);
    }
  }


  public void executeFunction(String rgnName, String functionId,
      ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector resultCollector,
      byte hasResult, boolean isHA, boolean optimizeForWrite, boolean replaying) {

    recordTXOperation(ServerRegionOperation.EXECUTE_FUNCTION, null, Integer.valueOf(2), functionId,
        serverRegionExecutor, resultCollector, Byte.valueOf(hasResult), Boolean.valueOf(isHA),
        Boolean.valueOf(optimizeForWrite));

    int retryAttempts = pool.getRetryAttempts();
    if (this.pool.getPRSingleHopEnabled()) {
      ClientMetadataService cms = this.region.getCache().getClientMetadataService();
      if (cms.isMetadataStable()) {
        if (serverRegionExecutor.getFilter().isEmpty()) {
          HashMap<ServerLocation, HashSet<Integer>> serverToBuckets =
              cms.groupByServerToAllBuckets(this.region, optimizeForWrite);
          if (serverToBuckets == null || serverToBuckets.isEmpty()) {
            ExecuteRegionFunctionOp.execute(this.pool, rgnName, functionId, serverRegionExecutor,
                resultCollector, hasResult, retryAttempts, isHA, optimizeForWrite);
            cms.scheduleGetPRMetaData(this.region, false);
          } else {
            ExecuteRegionFunctionSingleHopOp.execute(this.pool, this.region, functionId,
                serverRegionExecutor, resultCollector, hasResult, serverToBuckets, retryAttempts,
                true, isHA, optimizeForWrite);
          }
        } else {
          boolean isBucketsAsFilter = serverRegionExecutor.getExecuteOnBucketSetFlag();
          Map<ServerLocation, HashSet> serverToFilterMap = cms.getServerToFilterMap(
              serverRegionExecutor.getFilter(), region, optimizeForWrite, isBucketsAsFilter);
          if (serverToFilterMap == null || serverToFilterMap.isEmpty()) {
            ExecuteRegionFunctionOp.execute(this.pool, rgnName, functionId, serverRegionExecutor,
                resultCollector, hasResult, retryAttempts, isHA, optimizeForWrite);
            cms.scheduleGetPRMetaData(region, false);
          } else {
            ExecuteRegionFunctionSingleHopOp.execute(this.pool, this.region, functionId,
                serverRegionExecutor, resultCollector, hasResult, serverToFilterMap, retryAttempts,
                false, isHA, optimizeForWrite);
          }
        }
      } else {
        cms.scheduleGetPRMetaData(region, false);
        ExecuteRegionFunctionOp.execute(this.pool, rgnName, functionId, serverRegionExecutor,
            resultCollector, hasResult, retryAttempts, isHA, optimizeForWrite);
      }
    } else {
      ExecuteRegionFunctionOp.execute(this.pool, rgnName, functionId, serverRegionExecutor,
          resultCollector, hasResult, retryAttempts, isHA, optimizeForWrite);
    }
  }


  public void executeFunctionNoAck(String rgnName, Function function,
      ServerRegionFunctionExecutor serverRegionExecutor, byte hasResult, boolean replaying) {
    recordTXOperation(ServerRegionOperation.EXECUTE_FUNCTION, null, Integer.valueOf(3), function,
        serverRegionExecutor, Byte.valueOf(hasResult));
    ExecuteRegionFunctionNoAckOp.execute(this.pool, rgnName, function, serverRegionExecutor,
        hasResult);
  }

  public void executeFunctionNoAck(String rgnName, String functionId,
      ServerRegionFunctionExecutor serverRegionExecutor, byte hasResult, boolean isHA,
      boolean optimizeForWrite, boolean replaying) {
    recordTXOperation(ServerRegionOperation.EXECUTE_FUNCTION, null, Integer.valueOf(4), functionId,
        serverRegionExecutor, Byte.valueOf(hasResult));
    ExecuteRegionFunctionNoAckOp.execute(this.pool, rgnName, functionId, serverRegionExecutor,
        hasResult, isHA, optimizeForWrite);
  }

  public Entry getEntry(Object key) {
    recordTXOperation(ServerRegionOperation.GET_ENTRY, key);
    return (Entry) GetEntryOp.execute(pool, region, key);
  }


  /**
   * Transaction synchronization notification to the servers
   *
   * @see org.apache.geode.internal.cache.tx.ClientTXStateStub#beforeCompletion()
   */
  public void beforeCompletion(int txId) {
    TXSynchronizationOp.execute(pool, 0, txId,
        TXSynchronizationOp.CompletionType.BEFORE_COMPLETION);
  }

  /**
   * Transaction synchronization notification to the servers
   *
   * @return the server's TXCommitMessage
   * @see org.apache.geode.internal.cache.tx.ClientTXStateStub#afterCompletion(int)
   */
  public TXCommitMessage afterCompletion(int status, int txId) {
    return TXSynchronizationOp.execute(pool, status, txId,
        TXSynchronizationOp.CompletionType.AFTER_COMPLETION);
  }

  public byte[] getFunctionAttributes(String functionId) {
    return (byte[]) GetFunctionAttributeOp.execute(this.pool, functionId);
  }

  /** test hook */
  private void recordTXOperation(ServerRegionOperation op, Object key, Object... arguments) {
    if (ClientTXStateStub.transactionRecordingEnabled()) {
      TXStateProxy tx = TXManagerImpl.getCurrentTXState();
      if (tx == null) {
        return;
      }
      tx.recordTXOperation(this, op, key, arguments);
    }
  }

}
