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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
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
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.concurrent.CopyOnWriteHashMap;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInitializationException;
import org.apache.geode.pdx.PdxRegistryMismatchException;

public class PeerTypeRegistration implements TypeRegistration {
  private static final Logger logger = LogService.getLogger();

  private static final int MAX_TRANSACTION_FAILURES = 10;

  public static final String LOCK_SERVICE_NAME = "__PDX";

  public static final String REGION_NAME = "PdxTypes";
  public static final String REGION_FULL_PATH = "/" + REGION_NAME;
  public static final int PLACE_HOLDER_FOR_TYPE_ID = 0xFFFFFF;
  public static final int PLACE_HOLDER_FOR_DS_ID = 0xFF000000;

  private int dsId;
  private final int maxTypeId;
  private volatile DistributedLockService dls;
  private final Object dlsLock = new Object();
  private InternalCache cache;

  /**
   * The region where the PDX metadata is stored. Because this region is transactional for our
   * internal updates but we don't want to participate in the users transactions, all operations on
   * this region must suspend any existing transactions with suspendTX/resumeTX.
   */
  private Region<Object/* Integer or EnumCode */, Object/* PdxType or enum info */> idToType;

  /**
   * This map serves two purposes. It lets us look up an id based on a type, if we previously found
   * that type in the region. And, if a type is present in this map, that means we read the type
   * while holding the dlock, which means the type was distributed to all members.
   */
  private Map<PdxType, Integer> typeToId =
      Collections.synchronizedMap(new HashMap<PdxType, Integer>());

  private Map<EnumInfo, EnumId> enumToId =
      Collections.synchronizedMap(new HashMap<EnumInfo, EnumId>());

  private final Map<String, CopyOnWriteHashSet<PdxType>> classToType = new CopyOnWriteHashMap<>();

  private volatile boolean typeRegistryInUse = false;

  public PeerTypeRegistration(InternalCache cache) {
    this.cache = cache;

    int distributedSystemId =
        cache.getInternalDistributedSystem().getDistributionManager().getDistributedSystemId();
    if (distributedSystemId == -1) {
      distributedSystemId = 0;
    }
    this.dsId = distributedSystemId << 24;
    this.maxTypeId = 0xFFFFFF;
  }

  private Region<Object/* Integer or EnumCode */, Object/* PdxType or enum info */> getIdToType() {
    if (this.idToType != null) {
      return this.idToType;
    } else {
      if (this.cache.getPdxPersistent() && this.cache.getCacheConfig().pdxDiskStoreUserSet) {
        throw new PdxInitializationException(
            "PDX registry could not be initialized because the disk store "
                + this.cache.getPdxDiskStore() + " was not created.");
      } else {
        throw new PdxInitializationException("PDX registry was not initialized.");
      }
    }
  }

  public void initialize() {
    AttributesFactory<Object, Object> factory = new AttributesFactory<Object, Object>();
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
        // update a local map with the pdxtypes registered
        Object value = event.getNewValue();
        if (value instanceof PdxType) {
          updateClassToTypeMap((PdxType) value);
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

    RegionAttributes<Object, Object> regionAttrs = factory.create();

    InternalRegionArguments internalArgs = new InternalRegionArguments();
    internalArgs.setIsUsedForMetaRegion(true);
    internalArgs.setMetaRegionWithTransactions(true);
    try {
      this.idToType = cache.createVMRegion(REGION_NAME, regionAttrs, internalArgs);
    } catch (IOException ex) {
      throw new PdxInitializationException("Could not create pdx registry", ex);
    } catch (TimeoutException ex) {
      throw new PdxInitializationException("Could not create pdx registry", ex);
    } catch (RegionExistsException ex) {
      throw new PdxInitializationException("Could not create pdx registry", ex);
    } catch (ClassNotFoundException ex) {
      throw new PdxInitializationException("Could not create pdx registry", ex);
    }

    // If there is anything in the id to type registry,
    // we should validate our configuration now.
    // And send those types to any existing gateways.
    if (!getIdToType().isEmpty()) {
      verifyConfiguration();
    }
  }

  protected DistributedLockService getLockService() {
    if (this.dls != null) {
      return this.dls;
    }
    synchronized (this.dlsLock) {
      if (this.dls == null) {
        try {
          this.dls = DLockService.create(LOCK_SERVICE_NAME,
              this.cache.getInternalDistributedSystem(), true /* distributed */,
              true /* destroyOnDisconnect */, true /* automateFreeResources */);
        } catch (IllegalArgumentException e) {
          this.dls = DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME);
          if (this.dls == null) {
            throw e;
          }
        }
      }
      return this.dls;
    }
  }

  private static final String LOCK_NAME = "PDX_LOCK";

  private int allocateTypeId(PdxType newType) {
    TXStateProxy currentState = suspendTX();
    Region<Object, Object> r = getIdToType();

    int id = newType.hashCode() & PLACE_HOLDER_FOR_TYPE_ID;
    int newTypeId = id | this.dsId;

    try {
      int maxTry = maxTypeId;
      while (r.get(newTypeId) != null) {
        maxTry--;
        if (maxTry == 0) {
          throw new InternalGemFireError(
              "Used up all of the PDX type ids for this distributed system. The maximum number of PDX types is "
                  + maxTypeId);
        }

        // Find the next available type id.
        id++;
        if (id > this.maxTypeId) {
          id = 1;
        }
        newTypeId = id | this.dsId;
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
    int newEnumId = id | this.dsId;
    try {
      int maxTry = this.maxTypeId;
      // Find the next available type id.
      while (r.get(new EnumId(newEnumId)) != null) {
        maxTry--;
        if (maxTry == 0) {
          throw new InternalGemFireError(
              "Used up all of the PDX type ids for this distributed system. The maximum number of PDX types is "
                  + this.maxTypeId);
        }

        // Find the next available type id.
        id++;
        if (id > this.maxTypeId) {
          id = 1;
        }
        newEnumId = id | this.dsId;
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

  public int defineType(PdxType newType) {
    verifyConfiguration();
    Integer existingId = typeToId.get(newType);
    if (existingId != null) {
      return existingId;
    }
    lock();
    try {
      int id = getExistingIdForType(newType);
      if (id != -1) {
        return id;
      }

      id = allocateTypeId(newType);
      newType.setTypeId(id);

      updateIdToTypeRegion(newType);

      typeToId.put(newType, id);

      return newType.getTypeId();
    } finally {
      unlock();
    }
  }

  private void updateIdToTypeRegion(PdxType newType) {
    updateRegion(newType.getTypeId(), newType);
  }

  private void updateIdToEnumRegion(EnumId id, EnumInfo ei) {
    updateRegion(id, ei);
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

  public PdxType getType(int typeId) {
    return getById(typeId);
  }

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

  public void gatewaySenderStarted(GatewaySender gatewaySender) {
    if (!typeRegistryInUse || this.idToType == null) {
      return;
    }
    checkAllowed(true, this.cache.hasPersistentRegion());
  }

  public void creatingPersistentRegion() {
    // Anything is allowed until the registry is in use.
    if (!typeRegistryInUse) {
      return;
    }
    checkAllowed(hasGatewaySender(), true);
  }

  public boolean hasGatewaySender() {
    Set<GatewaySender> sendersAndAsyncQueues = cache.getGatewaySenders();
    Iterator<GatewaySender> itr = sendersAndAsyncQueues.iterator();
    while (itr.hasNext()) {
      GatewaySender sender = itr.next();
      if (AsyncEventQueueImpl.isAsyncEventQueue(sender.getId())) {
        itr.remove();
      }
    }
    return !sendersAndAsyncQueues.isEmpty();
  }

  public void creatingPool() {
    if (typeRegistryInUse) {
      throw new PdxInitializationException(
          "The PDX metadata has already been created as a peer metadata region. Please create your pools first");
    }
  }

  void verifyConfiguration() {
    if (typeRegistryInUse) {
      return;
    } else {
      checkAllowed(hasGatewaySender(), this.cache.hasPersistentRegion());

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

  /** Should be called holding the dlock */
  private int getExistingIdForType(PdxType newType) {
    int totalPdxTypeIdInDS = 0;
    TXStateProxy currentState = suspendTX();
    try {
      int result = -1;
      for (Map.Entry<Object, Object> entry : getIdToType().entrySet()) {
        Object v = entry.getValue();
        Object k = entry.getKey();
        if (k instanceof EnumId) {
          EnumId id = (EnumId) k;
          EnumInfo info = (EnumInfo) v;
          enumToId.put(info, id);
        } else {
          PdxType foundType = (PdxType) v;
          Integer id = (Integer) k;
          int tmpDsId = PLACE_HOLDER_FOR_DS_ID & id;
          if (tmpDsId == this.dsId) {
            totalPdxTypeIdInDS++;
          }

          typeToId.put(foundType, id);
          if (foundType.equals(newType)) {
            result = foundType.getTypeId();
          }
        }
      }
      if (totalPdxTypeIdInDS == this.maxTypeId) {
        throw new InternalGemFireError(
            "Used up all of the PDX type ids for this distributed system. The maximum number of PDX types is "
                + this.maxTypeId);
      }
      return result;
    } finally {
      resumeTX(currentState);
    }
  }

  /** Should be called holding the dlock */
  private EnumId getExistingIdForEnum(EnumInfo ei) {
    TXStateProxy currentState = suspendTX();
    int totalEnumIdInDS = 0;
    try {
      EnumId result = null;
      for (Map.Entry<Object, Object> entry : getIdToType().entrySet()) {
        Object v = entry.getValue();
        Object k = entry.getKey();
        if (k instanceof EnumId) {
          EnumId id = (EnumId) k;
          EnumInfo info = (EnumInfo) v;
          enumToId.put(info, id);
          int tmpDsId = PLACE_HOLDER_FOR_DS_ID & id.intValue();
          if (tmpDsId == this.dsId) {
            totalEnumIdInDS++;
          }
          if (ei.equals(info)) {
            result = id;
          }
        } else {
          typeToId.put((PdxType) v, (Integer) k);
        }
      }

      if (totalEnumIdInDS == this.maxTypeId) {
        throw new InternalGemFireError(
            "Used up all of the PDX enum ids for this distributed system. The maximum number of PDX types is "
                + this.maxTypeId);
      }
      return result;
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

  public int getEnumId(Enum<?> v) {
    verifyConfiguration();
    EnumInfo ei = new EnumInfo(v);
    EnumId existingId = enumToId.get(ei);
    if (existingId != null) {
      return existingId.intValue();
    }
    lock();
    try {
      EnumId id = getExistingIdForEnum(ei);
      if (id != null) {
        return id.intValue();
      }

      id = allocateEnumId(ei);

      updateIdToEnumRegion(id, ei);

      enumToId.put(ei, id);

      return id.intValue();
    } finally {
      unlock();
    }
  }

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

  public int defineEnum(EnumInfo newInfo) {
    verifyConfiguration();
    EnumId existingId = enumToId.get(newInfo);
    if (existingId != null) {
      return existingId.intValue();
    }
    lock();
    try {
      EnumId id = getExistingIdForEnum(newInfo);
      if (id != null) {
        return id.intValue();
      }

      id = allocateEnumId(newInfo);

      updateIdToEnumRegion(id, newInfo);

      enumToId.put(newInfo, id);

      return id.intValue();
    } finally {
      unlock();
    }
  }

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
    Map<Integer, EnumInfo> enums = new HashMap<Integer, EnumInfo>();
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
  private void updateClassToTypeMap(PdxType type) {
    if (type != null) {
      synchronized (this.classToType) {
        if (type.getClassName().equals(JSONFormatter.JSON_CLASSNAME)) {
          return; // no need to include here
        }
        CopyOnWriteHashSet<PdxType> pdxTypeSet = this.classToType.get(type.getClassName());
        if (pdxTypeSet == null) {
          pdxTypeSet = new CopyOnWriteHashSet<PdxType>();
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

  public static int getPdxRegistrySize() {
    InternalCache cache = GemFireCacheImpl.getExisting();
    if (cache == null) {
      return 0;
    }

    TypeRegistry registry = cache.getPdxRegistry();
    if (registry == null) {
      return 0;
    }

    return registry.getLocalSize();
  }

  @Override
  public int getLocalSize() {
    return idToType.size();
  }
}
