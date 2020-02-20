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

package org.apache.geode.pdx.internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.util.concurrent.CopyOnWriteHashMap;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInitializationException;
import org.apache.geode.pdx.PdxRegistryMismatchException;

public class PeerTypeRegistration implements TypeRegistration {
  private static final Logger logger = LogService.getLogger();

  private static final int MAX_TRANSACTION_FAILURES = 10;

  public static final String LOCK_SERVICE_NAME = "__PDX";
  private static final String LOCK_NAME = "PDX_LOCK";

  public static final String REGION_NAME = "PdxTypes";
  public static final String REGION_FULL_PATH = "/" + REGION_NAME;

  @VisibleForTesting
  public static final int PLACE_HOLDER_FOR_TYPE_ID = 0xFFFFFF;
  private static final int PLACE_HOLDER_FOR_DS_ID = 0xFF000000;
  private static final int MAX_TYPE_ID = 0xFFFFFF;

  private final TypeRegistrationStatistics statistics;

  private final int typeIdPrefix;
  private final Object dlsLock = new Object();
  private final InternalCache cache;

  private volatile DistributedLockService dls;

  /**
   * The region where the PDX metadata is stored. Because this region is transactional for our
   * internal updates but we don't want to participate in the users transactions, all operations on
   * this region must suspend any existing transactions with suspendTX/resumeTX.
   */
  private Region<Object/* Integer or EnumCode */, Object/* PdxType or enum info */> idToType;

  private PeerTypeRegistrationReverseMap reverseMap = new PeerTypeRegistrationReverseMap();

  private final Map<String, CopyOnWriteHashSet<PdxType>> classToType = new CopyOnWriteHashMap<>();

  private volatile boolean typeRegistryInUse = false;

  public PeerTypeRegistration(final InternalCache cache) {
    this.cache = cache;

    final InternalDistributedSystem internalDistributedSystem =
        cache.getInternalDistributedSystem();
    typeIdPrefix = getDistributedSystemId(internalDistributedSystem) << 24;
    statistics =
        new TypeRegistrationStatistics(internalDistributedSystem.getStatisticsManager(), this);
  }

  private static int getDistributedSystemId(
      final InternalDistributedSystem internalDistributedSystem) {
    final int distributedSystemId =
        internalDistributedSystem.getDistributionManager().getDistributedSystemId();
    if (distributedSystemId == -1) {
      return 0;
    }
    return distributedSystemId;
  }

  private Region<Object/* Integer or EnumCode */, Object/* PdxType or enum info */> getIdToType() {
    if (idToType != null) {
      return idToType;
    } else {
      if (cache.getPdxPersistent() && cache.getCacheConfig().pdxDiskStoreUserSet) {
        throw new PdxInitializationException(
            "PDX registry could not be initialized because the disk store "
                + cache.getPdxDiskStore() + " was not created.");
      } else {
        throw new PdxInitializationException("PDX registry was not initialized.");
      }
    }
  }

  @Override
  public void initialize() {
    // Relevant during reconnect
    TypeRegistry typeRegistry = cache.getPdxRegistry();
    if (typeRegistry != null) {
      typeRegistry.flushCache();
      logger.debug("Flushing TypeRegistry");
    }

    InternalRegionFactory factory = cache.createInternalRegionFactory();

    factory.setScope(Scope.DISTRIBUTED_ACK);
    if (cache.getPdxPersistent()) {
      if (cache.getCacheConfig().pdxDiskStoreUserSet) {
        factory.setDiskStoreName(cache.getPdxDiskStore());
      } else {
        factory.setDiskStoreName(cache.getOrCreateDefaultDiskStore().getName());
      }
      factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    } else {
      factory.setDataPolicy(DataPolicy.REPLICATE);
    }

    // Add a listener that makes sure that if anyone in the DS is using PDX
    // Our PDX configuration is valid for this member. This is important if
    // we are the gateway, we need to validate that we have a distributed system
    // id.
    factory.addCacheListener(new CacheListenerAdapter<Object, Object>() {
      @Override
      public void afterCreate(EntryEvent<Object, Object> event) {
        verifyConfiguration();
        // update the local map and reverse map with the pdxtypes registered
        Object value = event.getNewValue();
        Object key = event.getKey();
        if (value != null) {
          updateLocalAndReverseMaps(key, value);
        }
      }
    });

    factory.setCacheWriter(new CacheWriterAdapter<Object, Object>() {

      @Override
      public void beforeCreate(EntryEvent<Object, Object> event) throws CacheWriterException {
        Object newValue = event.getNewValue();
        if (newValue instanceof PdxType) {
          logger.info("Adding new type: {}", ((PdxType) event.getNewValue()).toFormattedString());
        } else {
          logger.info("Adding new type: {} {}", event.getKey(),
              ((EnumInfo) newValue).toFormattedString());
        }
      }

      @Override
      public void beforeUpdate(EntryEvent<Object, Object> event) throws CacheWriterException {
        if (!event.getRegion().get(event.getKey()).equals(event.getNewValue())) {
          PdxRegistryMismatchException ex = new PdxRegistryMismatchException(
              "Trying to add a PDXType with the same id as an existing PDX type. id="
                  + event.getKey() + ", existing pdx type " + event.getOldValue() + ", new type "
                  + event.getNewValue());
          throw new CacheWriterException(ex);
        }
      }

    });

    factory.setIsUsedForMetaRegion(true);
    factory.setMetaRegionWithTransactions(true);
    try {
      idToType = factory.create(REGION_NAME);
    } catch (TimeoutException | RegionExistsException ex) {
      throw new PdxInitializationException("Could not create pdx registry", ex);
    }

    statistics.initialize();

    // If there is anything in the id to type registry,
    // we should validate our configuration now.
    // And send those types to any existing gateways.
    if (!getIdToType().isEmpty()) {
      verifyConfiguration();
    }
  }

  protected DistributedLockService getLockService() {
    if (dls != null) {
      return dls;
    }
    synchronized (dlsLock) {
      if (dls == null) {
        try {
          dls = DLockService.create(LOCK_SERVICE_NAME,
              cache.getInternalDistributedSystem(), true /* distributed */,
              true /* destroyOnDisconnect */, true /* automateFreeResources */);
        } catch (IllegalArgumentException e) {
          dls = DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME);
          if (dls == null) {
            throw e;
          }
        }
      }
      return dls;
    }
  }

  private int allocateTypeId(PdxType newType) {
    TXStateProxy currentState = suspendTX();
    Region<Object, Object> r = getIdToType();

    int id = newType.hashCode() & PLACE_HOLDER_FOR_TYPE_ID;
    int newTypeId = id | typeIdPrefix;

    try {
      int maxTry = MAX_TYPE_ID;
      while (r.get(newTypeId) != null) {
        maxTry--;
        if (maxTry == 0) {
          throw new InternalGemFireError(
              "Used up all of the PDX type ids for this distributed system. The maximum number of PDX types is "
                  + MAX_TYPE_ID);
        }

        // Find the next available type id.
        id++;
        if (id > MAX_TYPE_ID) {
          id = 1;
        }
        newTypeId = id | typeIdPrefix;
      }

      return newTypeId;
    } finally {
      resumeTX(currentState);
    }
  }

  private EnumId allocateEnumId(EnumInfo ei) {
    TXStateProxy currentState = suspendTX();
    Region<Object, Object> r = getIdToType();

    int id = ei.hashCode() & PLACE_HOLDER_FOR_TYPE_ID;
    int newEnumId = id | typeIdPrefix;
    try {
      int maxTry = MAX_TYPE_ID;
      // Find the next available type id.
      while (r.get(new EnumId(newEnumId)) != null) {
        maxTry--;
        if (maxTry == 0) {
          throw new InternalGemFireError(
              "Used up all of the PDX type ids for this distributed system. The maximum number of PDX types is "
                  + MAX_TYPE_ID);
        }

        // Find the next available type id.
        id++;
        if (id > MAX_TYPE_ID) {
          id = 1;
        }
        newEnumId = id | typeIdPrefix;
      }

      return new EnumId(newEnumId);
    } finally {
      resumeTX(currentState);
    }
  }

  private void unlock() {
    try {
      DistributedLockService dls = getLockService();
      dls.unlock(LOCK_NAME);
    } catch (LockServiceDestroyedException e) {
      // fix for bug 43574
      cache.getCancelCriterion().checkCancelInProgress(e);
      throw e;
    }
  }

  private void lock() {
    DistributedLockService dls = getLockService();
    try {
      if (!dls.lock(LOCK_NAME, -1, -1)) {
        // this should be impossible
        throw new InternalGemFireException("Could not obtain pdx lock");
      }
    } catch (LockServiceDestroyedException e) {
      // fix for bug 43172
      cache.getCancelCriterion().checkCancelInProgress(e);
      throw e;
    }
  }

  /**
   * For bug #43690 we cannot use shared sockets when doing pdx type registration
   */
  private boolean useUDPMessagingIfNecessary() {
    boolean result = false;
    InternalDistributedSystem sys = cache.getInternalDistributedSystem();
    if (sys != null && !sys.threadOwnsResources()) {
      sys.getDistributionManager().forceUDPMessagingForCurrentThread();
      result = true;
    }
    return result;
  }

  private void releaseUDPMessaging(boolean release) {
    if (release) {
      InternalDistributedSystem sys = cache.getInternalDistributedSystem();
      if (sys != null) {
        sys.getDistributionManager().releaseUDPMessagingForCurrentThread();
      }
    }
  }

  @Override
  public int defineType(PdxType newType) {
    statistics.typeDefined();
    verifyConfiguration();
    Integer existingId = reverseMap.getIdFromReverseMap(newType);
    if (existingId != null) {
      return existingId;
    }
    lock();
    try {
      if (shouldReload()) {
        buildReverseMapsFromRegion();
      }
      reverseMap.flushPendingReverseMap();

      // double check if my PdxType is in the reverse map in case it was just flushed into it
      existingId = reverseMap.getIdFromReverseMap(newType);
      if (existingId != null) {
        return existingId;
      }

      int id = allocateTypeId(newType);
      newType.setTypeId(id);
      updateIdToTypeRegion(newType);

      return newType.getTypeId();
    } finally {
      // flush the reverse map for the member that introduced this new PdxType
      reverseMap.flushPendingReverseMap();
      unlock();
    }
  }

  private void updateIdToTypeRegion(PdxType newType) {
    updateRegion(newType.getTypeId(), newType);
    statistics.typeCreated();
  }

  private void updateIdToEnumRegion(EnumId id, EnumInfo ei) {
    updateRegion(id, ei);
    statistics.enumCreated();
  }

  private void updateRegion(Object k, Object v) {
    Region<Object, Object> r = getIdToType();
    InternalCache cache = (InternalCache) r.getRegionService();

    checkDistributedTypeRegistryState();

    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    TXStateProxy currentState = suspendTX();
    boolean state = useUDPMessagingIfNecessary();
    try {

      // The loop might not be necessary because we're
      // updating a replicated region inside a dlock,
      // but just in case we'll make sure to retry the transaction.
      int failureCount = 0;
      while (true) {
        txManager.begin();
        try {
          r.put(k, v);
          txManager.commit();
          return;
        } catch (TransactionException e) {
          // even put can now throw a TransactionException, rollback if required
          if (txManager.exists()) {
            txManager.rollback();
          }
          // Let's just make sure things don't get out of hand.
          if (++failureCount > MAX_TRANSACTION_FAILURES) {
            throw e;
          }
        }
      }
    } finally {
      releaseUDPMessaging(state);
      resumeTX(currentState);
    }
  }

  private void checkDistributedTypeRegistryState() {
    CheckTypeRegistryState.send(cache.getDistributionManager());
  }

  @Override
  public PdxType getType(int typeId) {
    return getById(typeId);
  }

  @SuppressWarnings("unchecked")
  private <T> T getById(Object typeId) {
    verifyConfiguration();
    TXStateProxy currentState = suspendTX();
    try {
      T pdxType = (T) getIdToType().get(typeId);
      if (pdxType == null) {
        lock();
        try {
          pdxType = (T) getIdToType().get(typeId);
        } finally {
          unlock();
        }
      }
      return pdxType;
    } finally {
      resumeTX(currentState);
    }

  }

  @Override
  public void addRemoteType(int typeId, PdxType type) {
    verifyConfiguration();
    TXStateProxy currentState = suspendTX();
    Region<Object, Object> r = getIdToType();
    try {
      if (!r.containsKey(typeId)) {
        // This type could actually be for this distributed system,
        // so we need to make sure the type is published while holding
        // the distributed lock.
        lock();
        try {
          r.putIfAbsent(typeId, type);
        } finally {
          unlock();
        }
      }
    } finally {
      resumeTX(currentState);
    }
  }

  @Override
  public void gatewaySenderStarted(GatewaySender gatewaySender) {
    if (!typeRegistryInUse || idToType == null) {
      return;
    }
    checkAllowed(true, cache.hasPersistentRegion());
  }

  @Override
  public void creatingPersistentRegion() {
    // Anything is allowed until the registry is in use.
    if (!typeRegistryInUse) {
      return;
    }
    checkAllowed(hasGatewaySender(), true);
  }

  private boolean hasGatewaySender() {
    Set<GatewaySender> sendersAndAsyncQueues = cache.getGatewaySenders();
    sendersAndAsyncQueues.removeIf(sender -> AsyncEventQueueImpl.isAsyncEventQueue(sender.getId()));
    return !sendersAndAsyncQueues.isEmpty();
  }

  @Override
  public void creatingPool() {
    if (typeRegistryInUse) {
      throw new PdxInitializationException(
          "The PDX metadata has already been created as a peer metadata region. Please create your pools first");
    }
  }

  void verifyConfiguration() {
    if (!typeRegistryInUse) {
      checkAllowed(hasGatewaySender(), cache.hasPersistentRegion());

      for (Pool pool : PoolManager.getAll().values()) {
        if (!((PoolImpl) pool).isUsedByGateway()) {
          throw new PdxInitializationException(
              "The PDX metadata has already been " + "created as a peer metadata region. "
                  + "Please use ClientCacheFactory to create clients.");
        }
      }

      typeRegistryInUse = true;
    }
  }

  private void checkAllowed(boolean hasGatewaySender, boolean hasPersistentRegion) {
    if (hasPersistentRegion && !cache.getPdxPersistent()) {
      throw new PdxInitializationException(
          "The PDX metadata must be persistent in a member that has persistent data. See CacheFactory.setPdxPersistent.");
    }
    int distributedSystemId =
        cache.getInternalDistributedSystem().getDistributionManager().getDistributedSystemId();
    if (hasGatewaySender && distributedSystemId == -1) {
      throw new PdxInitializationException(
          "When using PDX with a WAN gateway sender, you must set the distributed-system-id gemfire property for your distributed system. See the javadocs for DistributedSystem.");
    }
  }

  /**
   * Should only be called holding the dlock
   * This method iterates through the entire PdxTypes region and syncs the reverse map with the pdx
   * region This is an expensive operation and should only be called during initialization. A cache
   * listener is used to keep the reverse maps up to date.
   */
  void buildReverseMapsFromRegion() {
    int totalPdxTypeIdInDS = 0;
    int totalEnumIdInDS = 0;
    TXStateProxy currentState = suspendTX();
    try {
      reverseMap.clear();
      for (Map.Entry<Object, Object> entry : getIdToType().entrySet()) {
        Object k = entry.getKey();
        Object v = entry.getValue();
        if (k instanceof EnumId) {
          EnumId id = (EnumId) k;
          int tmpDsId = PLACE_HOLDER_FOR_DS_ID & id.intValue();
          if (tmpDsId == typeIdPrefix) {
            totalEnumIdInDS++;
            if (totalEnumIdInDS >= MAX_TYPE_ID) {
              throw new InternalGemFireError(
                  "Used up all of the PDX enum ids for this distributed system. The maximum number of PDX types is "
                      + MAX_TYPE_ID);
            }
          }
        } else {
          Integer id = (Integer) k;
          int tmpDsId = PLACE_HOLDER_FOR_DS_ID & id;
          if (tmpDsId == typeIdPrefix) {
            totalPdxTypeIdInDS++;
            if (totalPdxTypeIdInDS >= MAX_TYPE_ID) {
              throw new InternalGemFireError(
                  "Used up all of the PDX type ids for this distributed system. The maximum number of PDX types is "
                      + MAX_TYPE_ID);
            }
          }
        }
        reverseMap.save(k, v);
      }
    } finally {
      resumeTX(currentState);
    }
  }

  private TXStateProxy suspendTX() {
    InternalCache cache = (InternalCache) getIdToType().getRegionService();
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    // A new transaction will be started to register pdx.
    return txManager.internalSuspend();
  }

  private void resumeTX(TXStateProxy state) {
    if (state != null) {
      TXManagerImpl txManager = state.getTxMgr();
      txManager.internalResume(state);
    }
  }

  @Override
  public int getEnumId(Enum<?> v) {
    return defineEnum(new EnumInfo(v));
  }

  @Override
  public void addRemoteEnum(int id, EnumInfo enumInfo) {
    verifyConfiguration();
    TXStateProxy currentState = suspendTX();
    EnumId enumId = new EnumId(id);
    Region<Object, Object> r = getIdToType();
    try {
      if (!r.containsKey(enumId)) {
        // This enum could actually be for this distributed system,
        // so we need to make sure the enum is published while holding
        // the distributed lock.
        lock();
        try {
          r.put(enumId, enumInfo);
        } finally {
          unlock();
        }
      }
    } finally {
      resumeTX(currentState);
    }
  }

  boolean shouldReload() {
    boolean shouldReload = false;
    TXStateProxy currentState = suspendTX();
    try {
      shouldReload = reverseMap.shouldReloadFromRegion(getIdToType());
    } finally {
      resumeTX(currentState);
    }
    return shouldReload;
  }

  @Override
  public int defineEnum(final EnumInfo newInfo) {
    statistics.enumDefined();
    verifyConfiguration();
    EnumId existingId = reverseMap.getIdFromReverseMap(newInfo);
    if (existingId != null) {
      return existingId.intValue();
    }
    lock();
    try {
      if (shouldReload()) {
        buildReverseMapsFromRegion();
      }
      reverseMap.flushPendingReverseMap();

      // double check if my Enum is in the reverse map in case it was just flushed into it
      existingId = reverseMap.getIdFromReverseMap(newInfo);
      if (existingId != null) {
        return existingId.intValue();
      }

      EnumId id = allocateEnumId(newInfo);
      updateIdToEnumRegion(id, newInfo);

      return id.intValue();
    } finally {
      // flush the reverse map for the member that introduced this new enumInfo
      reverseMap.flushPendingReverseMap();
      unlock();
    }
  }

  @Override
  public EnumInfo getEnumById(int id) {
    EnumId enumId = new EnumId(id);
    return getById(enumId);
  }

  @Override
  public Map<Integer, PdxType> types() {
    // ugh, I don't think we can rely on the local map to contain all types
    Map<Integer, PdxType> types = new HashMap<>();
    for (Entry<Object, Object> type : getIdToType().entrySet()) {
      Object id = type.getKey();
      if (type.getValue() instanceof PdxType) {
        types.put((Integer) id, (PdxType) type.getValue());
      }
    }
    return types;
  }

  @Override
  public Map<Integer, EnumInfo> enums() {
    // ugh, I don't think we can rely on the local map to contain all types
    Map<Integer, EnumInfo> enums = new HashMap<>();
    for (Entry<Object, Object> type : getIdToType().entrySet()) {
      Object id = type.getKey();
      if (type.getValue() instanceof EnumInfo) {
        enums.put(((EnumId) id).intValue(), (EnumInfo) type.getValue());
      }
    }
    return enums;
  }

  /**
   * adds a PdxType for a field to a {@code className => Set<PdxType>} map
   */
  private void updateLocalAndReverseMaps(Object key, Object value) {
    reverseMap.saveToPending(key, value);
    if (value instanceof PdxType) {
      PdxType type = (PdxType) value;
      synchronized (classToType) {
        if (type.getClassName().equals(JSONFormatter.JSON_CLASSNAME)) {
          return; // no need to include here
        }
        CopyOnWriteHashSet<PdxType> pdxTypeSet = classToType.get(type.getClassName());
        if (pdxTypeSet == null) {
          pdxTypeSet = new CopyOnWriteHashSet<>();
        }
        pdxTypeSet.add(type);
        classToType.put(type.getClassName(), pdxTypeSet);
      }
    }
  }

  @Override
  public PdxType getPdxTypeForField(String fieldName, String className) {
    Set<PdxType> pdxTypes = getPdxTypesForClassName(className);
    for (PdxType pdxType : pdxTypes) {
      if (pdxType.getPdxField(fieldName) != null) {
        return pdxType;
      }
    }
    return null;
  }

  @Override
  public Set<PdxType> getPdxTypesForClassName(String className) {
    CopyOnWriteHashSet<PdxType> pdxTypeSet = classToType.get(className);
    if (pdxTypeSet == null) {
      return Collections.emptySet();
    } else {
      return pdxTypeSet.getSnapshot();
    }
  }

  @Override
  public boolean isClient() {
    return false;
  }

  @Override
  public void addImportedType(int typeId, PdxType importedType) {
    addRemoteType(typeId, importedType);
  }

  @Override
  public void addImportedEnum(int id, EnumInfo importedInfo) {
    addRemoteEnum(id, importedInfo);
  }

  @Deprecated
  public static int getPdxRegistrySize() {
    InternalCache cache = GemFireCacheImpl.getExisting();
    TypeRegistry registry = cache.getPdxRegistry();
    if (registry == null) {
      return 0;
    }

    return registry.getLocalSize();
  }

  @Override
  public int getLocalSize() {
    if (cache.isClosed()) {
      return 0;
    }
    return getIdToType().size();
  }

  @VisibleForTesting
  public int getTypeToIdSize() {
    return reverseMap.typeToIdSize();
  }

  @VisibleForTesting
  public int getEnumToIdSize() {
    return reverseMap.enumToIdSize();
  }
}
