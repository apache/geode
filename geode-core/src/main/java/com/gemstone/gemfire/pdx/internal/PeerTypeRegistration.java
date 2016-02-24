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
package com.gemstone.gemfire.pdx.internal;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.LockServiceDestroyedException;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.internal.CopyOnWriteHashSet;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateProxy;
import com.gemstone.gemfire.internal.util.concurrent.CopyOnWriteHashMap;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInitializationException;
import com.gemstone.gemfire.pdx.PdxRegistryMismatchException;

/**
 *
 */
public class PeerTypeRegistration implements TypeRegistration {
  /**
   * 
   */
  private static final int MAX_TRANSACTION_FAILURES = 10;
  public static final String LOCK_SERVICE_NAME = "__PDX";
  /**
   * The region name. Public for tests only.
   */
  public static final String REGION_NAME = "PdxTypes";
  public static final String REGION_FULL_PATH = "/" + REGION_NAME;
  
  private int nextTypeId;
  private final int maxTypeId;
  private int nextEnumId;
  private final int maxEnumId;
  private volatile DistributedLockService dls;
  private final Object dlsLock = new Object();
  private GemFireCacheImpl cache;
  
  /**
   * The region where the PDX metadata is stored.
   * Because this region is transactional for our internal updates
   * but we don't want to participate in the users transactions,
   * all operations on this region must suspend any existing
   * transactions with suspendTX/resumeTX. 
   */
  private Region<Object/*Integer or EnumCode*/, Object/*PdxType or enum info*/> idToType;
  
  /** This map serves two purposes. It lets us look
   * up an id based on a type, if we previously found that type
   * in the region. And, if a type is present in this map, that means
   * we read the type while holding the dlock, which means the type
   * was distributed to all members.
   */
  private Map<PdxType, Integer> typeToId = Collections.synchronizedMap(new HashMap<PdxType, Integer>());
  private Map<EnumInfo, EnumId> enumToId = Collections.synchronizedMap(new HashMap<EnumInfo, EnumId>());
  private final Map<String, Set<PdxType>> classToType = new CopyOnWriteHashMap<String, Set<PdxType>>();

  private volatile boolean typeRegistryInUse = false;
  
  public PeerTypeRegistration(GemFireCacheImpl cache) {
    this.cache = cache;
    
        
    int distributedSystemId = cache.getDistributedSystem().getDistributionManager().getDistributedSystemId();
    if(distributedSystemId == -1) {
      distributedSystemId = 0;
    }
    this.nextTypeId = distributedSystemId << 24;
    this.maxTypeId = distributedSystemId << 24 | 0xFFFFFF;
    this.nextEnumId = distributedSystemId << 24;
    this.maxEnumId = distributedSystemId << 24 | 0xFFFFFF;
  }
  
  private Region<Object/*Integer or EnumCode*/, Object/*PdxType or enum info*/> getIdToType() {
    if (this.idToType != null) {
      return this.idToType;
    } else {
      if (this.cache.getPdxPersistent() 
          && this.cache.getCacheConfig().pdxDiskStoreUserSet) {
          throw new PdxInitializationException("PDX registry could not be initialized because the disk store " + this.cache.getPdxDiskStore() + " was not created.");
      } else {
        throw new PdxInitializationException("PDX registry was not initialized.");
      }
    }
  }
  public void initialize() {
    AttributesFactory<Object, Object> factory = new AttributesFactory<Object, Object>();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    if(cache.getPdxPersistent()) {
      if(cache.getCacheConfig().pdxDiskStoreUserSet) {
        factory.setDiskStoreName(cache.getPdxDiskStore());
      } else {
        factory.setDiskStoreName(cache.getOrCreateDefaultDiskStore().getName());
      }
      factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    } else {
      factory.setDataPolicy(DataPolicy.REPLICATE);
    }
    
    //Add a listener that makes sure that if anyone in the DS is using PDX
    //Our PDX configuration is valid for this member. This is important if
    //we are the gateway, we need to validate that we have a distributed system 
    //id.
    factory.addCacheListener(new CacheListenerAdapter<Object, Object>() {
      @Override
      public void afterCreate(EntryEvent<Object, Object> event) {
        verifyConfiguration();
        //update a local map with the pdxtypes registered 
        Object value = event.getNewValue();
         if(value instanceof PdxType){
          updateClassToTypeMap((PdxType) value);
         }
      }
    });
    
    factory.setCacheWriter(new CacheWriterAdapter<Object, Object>() {

      @Override
      public void beforeUpdate(EntryEvent<Object, Object> event)
          throws CacheWriterException {
        if(!event.getOldValue().equals(event.getNewValue())) {
          PdxRegistryMismatchException ex = new PdxRegistryMismatchException("Trying to add a PDXType with the same id as an existing PDX type. id=" + event.getKey() + ", existing pdx type " + event.getOldValue() + ", new type " + event.getNewValue());
          throw new CacheWriterException(ex);
        }
      }
      
    });
    
    RegionAttributes<Object, Object> regionAttrs = factory.create();

    InternalRegionArguments internalArgs = new InternalRegionArguments();
    internalArgs.setIsUsedForMetaRegion(true);
    internalArgs.setMetaRegionWithTransactions(true);
    try {
      this.idToType = cache.createVMRegion(REGION_NAME, regionAttrs,
          internalArgs);
    } catch (IOException ex) {
      throw new PdxInitializationException("Could not create pdx registry", ex);
    } catch (TimeoutException ex) {
      throw new PdxInitializationException("Could not create pdx registry", ex);
    } catch (RegionExistsException ex) {
      throw new PdxInitializationException("Could not create pdx registry", ex);
    } catch (ClassNotFoundException ex) {
      throw new PdxInitializationException("Could not create pdx registry", ex);
    }

    //If there is anything in the id to type registry,
    //we should validate our configuration now.
    //And send those types to any existing gateways.
    if(!getIdToType().isEmpty()) {
      verifyConfiguration();
    }
  }

  private DistributedLockService getLockService() {
    if (this.dls != null) {
      return this.dls;
    }
    synchronized (this.dlsLock) {
      if (this.dls == null) {
        try {
          this.dls = DLockService.create(LOCK_SERVICE_NAME,
              this.cache.getDistributedSystem(), true /* distributed */,
              true /* destroyOnDisconnect */, true /* automateFreeResources */);
        } catch (IllegalArgumentException e) {
          this.dls = DistributedLockService
              .getServiceNamed(LOCK_SERVICE_NAME);
          if (this.dls == null) {
            throw e;
          }
        }
      }
      return this.dls;
    }
  }
  
  private static final String LOCK_NAME = "PDX_LOCK";

  private int allocateTypeId() {
    TXStateProxy currentState = suspendTX();
    Region<Object, Object> r = getIdToType();
    try {
      //Find the next available type id.
      do {
        this.nextTypeId++;
        if(this.nextTypeId == maxTypeId) {
          throw new InternalGemFireError("Used up all of the PDX type ids for this distributed system. The maximum number of PDX types is " + maxTypeId);
        }
      } while(r.get(nextTypeId) != null);

      this.lastAllocatedTypeId = this.nextTypeId;
      return this.nextTypeId;
    } finally {
      resumeTX(currentState);
    }
  }
  private EnumId allocateEnumId() {
    TXStateProxy currentState = suspendTX();
    Region<Object, Object> r = getIdToType();
    try {
      //Find the next available type id.
      do {
        this.nextEnumId++;
        if(this.nextEnumId == maxEnumId) {
          throw new InternalGemFireError("Used up all of the PDX enum ids for this distributed system. The maximum number of PDX types is " + maxEnumId);
        }
      } while(r.get(new EnumId(nextEnumId)) != null);

      this.lastAllocatedEnumId = this.nextEnumId;
      return new EnumId(this.nextEnumId);
    } finally {
      resumeTX(currentState);
    }
  }

  private void unlock() {
    try {
      DistributedLockService dls = getLockService();
      dls.unlock(LOCK_NAME);
    } catch(LockServiceDestroyedException e) {
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
    InternalDistributedSystem sys = cache.getDistributedSystem();
    if (sys != null  &&  !sys.threadOwnsResources()) {
      sys.getDistributionManager().forceUDPMessagingForCurrentThread();
      result = true;
    }
    return result;
  }
  
  private void releaseUDPMessaging(boolean release) {
    if (release) {
      InternalDistributedSystem sys = cache.getDistributedSystem();
      if (sys != null) {
        sys.getDistributionManager().releaseUDPMessagingForCurrentThread();
      }
    }
  }

  private int lastAllocatedTypeId; // for unit tests
  private int lastAllocatedEnumId; // for unit tests

  /**
   * Test hook that returns the most recently allocated type id
   * 
   * @return the most recently allocated type id
   */
  public int getLastAllocatedTypeId() {
    verifyConfiguration();
    return this.lastAllocatedTypeId;
  }
  /**
   * Test hook that returns the most recently allocated enum id
   * 
   * @return the most recently allocated enum id
   */
  public int getLastAllocatedEnumId() {
    verifyConfiguration();
    return this.lastAllocatedEnumId;
  }

  public int defineType(PdxType newType) {
    verifyConfiguration();
    Integer existingId = typeToId.get(newType);
    if(existingId != null) {
      return existingId.intValue();
    }
    lock();
    try {
      int id = getExistingIdForType(newType);
      if(id != -1) {
        return id;
      }
      
      id = allocateTypeId();
      newType.setTypeId(id);
      
      updateIdToTypeRegion(newType);
      
      typeToId.put(newType, Integer.valueOf(id));
      //this.cache.getLogger().info("Defining: " + newType, new RuntimeException("STACK"));
      
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
    Cache c = (Cache)r.getRegionService();
    
    checkDistributedTypeRegistryState();
    
    TXManagerImpl txManager = (TXManagerImpl) c.getCacheTransactionManager();
    TXStateProxy currentState = suspendTX();
    boolean state = useUDPMessagingIfNecessary();
    try {
      
      //The loop might not be necessary because we're
      //updating a replicated region inside a dlock,
      //but just in case we'll make sure to retry the transaction.
      int failureCount = 0;
      while(true) {
        txManager.begin();
        try {
          r.put(k, v);
          txManager.commit();
          return;
        } catch(TransactionException e) {
          // even put can now throw a TransactionException, rollback if required
          if (txManager.exists()) {
            txManager.rollback();
          }
          //Let's just make sure things don't get out of hand.
          if(++failureCount > MAX_TRANSACTION_FAILURES) {
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
    verifyConfiguration();
    TXStateProxy currentState = suspendTX();
    try {
      return (PdxType)getIdToType().get(typeId);
    } finally {
      resumeTX(currentState);
    }
    
  }
  
  public void addRemoteType(int typeId, PdxType type) {
    verifyConfiguration();
    TXStateProxy currentState = suspendTX();
    Region<Object, Object> r = getIdToType();
    try {
      if(!r.containsKey(typeId)) {
        //This type could actually be for this distributed system,
        //so we need to make sure the type is published while holding
        //the distributed lock.
        lock();
        try {
          r.put(typeId, type);
        } finally {
          unlock();
        }
      }
    } finally {
      resumeTX(currentState);
    }
  }

  public void gatewaySenderStarted(GatewaySender gatewaySender) {
    if(!typeRegistryInUse || this.idToType == null) {
      return;
    }
    checkAllowed(true, hasPersistentRegions());
  }

  public void creatingPersistentRegion() {
    //Anything is allowed until the registry is in use.
    if(!typeRegistryInUse) {
      return;
    }
    checkAllowed(hasGatewaySender(), true);
  }
  
  public boolean hasGatewaySender(){
    Set<GatewaySender> sendersAndAsyncQueues = cache.getGatewaySenders();
    Iterator<GatewaySender> itr = sendersAndAsyncQueues.iterator();
    while(itr.hasNext()){
      GatewaySender sender = itr.next();
      if(AsyncEventQueueImpl.isAsyncEventQueue(sender.getId())){
        itr.remove();
      }
    }
    return !sendersAndAsyncQueues.isEmpty();
  }
  public void creatingPool() {
    if(typeRegistryInUse) {
      throw new PdxInitializationException("The PDX metadata has already been created as a peer metadata region. Please create your pools first");
    }
  }
  
  void verifyConfiguration() {
    if(typeRegistryInUse) {
      return;
    } else {
      boolean hasPersistentRegions = hasPersistentRegions();
      checkAllowed(hasGatewaySender(), hasPersistentRegions);
      
      for(Pool pool : PoolManager.getAll().values()) {
        if(!((PoolImpl) pool).isUsedByGateway()) {
        throw new PdxInitializationException("The PDX metadata has already been " +
        		"created as a peer metadata region. " +
        		"Please use ClientCacheFactory to create clients.");
        }
      }
      
      typeRegistryInUse = true;
    }
  }

  public boolean hasPersistentRegions() {
    Collection<DiskStoreImpl> diskStores = cache.listDiskStoresIncludingRegionOwned();
    boolean hasPersistentRegions = false;
    for(DiskStoreImpl store : diskStores) {
      hasPersistentRegions |= store.hasPersistedData();
    }
    return hasPersistentRegions;
  }

  private void checkAllowed(boolean hasGatewaySender, boolean hasDiskStore) {
    if(hasDiskStore && !cache.getPdxPersistent()) {
      throw new PdxInitializationException("The PDX metadata must be persistent in a member that has persistent data. See CacheFactory.setPdxPersistent.");
    }
    int distributedSystemId = cache.getDistributedSystem().getDistributionManager().getDistributedSystemId();
    if(hasGatewaySender && distributedSystemId == -1) {
      throw new PdxInitializationException("When using PDX with a WAN gateway sender, you must set the distributed-system-id gemfire property for your distributed system. See the javadocs for DistributedSystem.");
    }
  }

  /** Should be called holding the dlock */
  private int getExistingIdForType(PdxType newType) {
    TXStateProxy currentState = suspendTX();
    try { 
      int result = -1;
      for (Map.Entry<Object, Object> entry : getIdToType().entrySet()) {
        Object v = entry.getValue();
        Object k = entry.getKey();
        if (k instanceof EnumId) {
          EnumId id = (EnumId)k;
          EnumInfo info = (EnumInfo)v;
          enumToId.put(info, id);
        } else {
          PdxType foundType = (PdxType)v;
          Integer id = (Integer)k;
          typeToId.put(foundType, id);
          if (foundType.equals(newType)) {
            result = foundType.getTypeId();
          }
        }
      }
      return result;
    } finally {
      resumeTX(currentState);
    }
  }

  /** Should be called holding the dlock */
  private EnumId getExistingIdForEnum(EnumInfo ei) {
    TXStateProxy currentState = suspendTX();
    try { 
      EnumId result = null;
      for (Map.Entry<Object, Object> entry : getIdToType().entrySet()) {
        Object v = entry.getValue();
        Object k = entry.getKey();
        if (k instanceof EnumId) {
          EnumId id = (EnumId)k;
          EnumInfo info = (EnumInfo)v;
          enumToId.put(info, id);
          // TODO: do I need to keep track of multiple ids for the same enum?
          if (ei.equals(info)) {
            result = id;
          }
        } else {
          typeToId.put((PdxType)v, (Integer)k);
        }
      }
      return result;
    } finally {
      resumeTX(currentState);
    }
  }
  
  private TXStateProxy suspendTX() {
    Cache c = (Cache)getIdToType().getRegionService();
    TXManagerImpl txManager = (TXManagerImpl) c.getCacheTransactionManager();
    TXStateProxy currentState = txManager.internalSuspend();
    return currentState;
  }
  
  private void resumeTX(TXStateProxy state) {
    if(state != null) {
      TXManagerImpl txManager = state.getTxMgr();
      txManager.resume(state);
    }
  }

  public int getEnumId(Enum<?> v) {
    verifyConfiguration();
    EnumInfo ei = new EnumInfo(v);
    EnumId existingId = enumToId.get(ei);
    if(existingId != null) {
      return existingId.intValue();
    }
    lock();
    try {
      EnumId id = getExistingIdForEnum(ei);
      if(id != null) {
        return id.intValue();
      }

      id = allocateEnumId();

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
      if(!r.containsKey(enumId)) {
        //This enum could actually be for this distributed system,
        //so we need to make sure the enum is published while holding
        //the distributed lock.
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
    if(existingId != null) {
      return existingId.intValue();
    }
    lock();
    try {
      EnumId id = getExistingIdForEnum(newInfo);
      if(id != null) {
        return id.intValue();
      }
      
      id = allocateEnumId();
      
      updateIdToEnumRegion(id, newInfo);
      
      enumToId.put(newInfo, id);
      
      return id.intValue();
    } finally {
      unlock();
    }
  }

  public EnumInfo getEnumById(int id) {
    verifyConfiguration();
    EnumId enumId = new EnumId(id);
    TXStateProxy currentState = suspendTX();
    try {
      return (EnumInfo)getIdToType().get(enumId);
    } finally {
      resumeTX(currentState);
    }
  }

  @Override
  public Map<Integer, PdxType> types() {
    // ugh, I don't think we can rely on the local map to contain all types
    Map<Integer, PdxType> types = new HashMap<Integer, PdxType>();
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
   * 
   * @param type
   */
  private void updateClassToTypeMap(PdxType type) {
    if(type != null){
      synchronized (this.classToType){
        if(type.getClassName().equals(JSONFormatter.JSON_CLASSNAME) )
          return;//no need to include here
        Set<PdxType> pdxTypeSet = this.classToType.get(type.getClassName());
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
    Set<PdxType> pdxTypes = classToType.get(className);
    if (pdxTypes != null) {
      for (PdxType pdxType : pdxTypes) {
        if (pdxType.getPdxField(fieldName) != null) {
          return pdxType;
        }
      }
    }
    return null;
  }

  /*
   * For testing purpose
   */
  public Map<String, Set<PdxType>> getClassToType() {
    return classToType;
  }

  /*
   * test hook
   */
  @Override
  public void testClearRegistry(){
    idToType.clear();
    enumToId.clear();
    typeToId.clear();
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
    GemFireCacheImpl cache = GemFireCacheImpl.getExisting();
    if(cache == null) {
      return 0;
    }
    
    TypeRegistry registry = cache.getPdxRegistry();
    if(registry == null) {
      return 0;
    }
    
    return registry.getLocalSize();
  }

  @Override
  public int getLocalSize() {
    return idToType.size();
  }
}
