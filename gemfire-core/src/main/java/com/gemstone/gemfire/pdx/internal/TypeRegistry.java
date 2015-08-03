/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx.internal;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.util.concurrent.CopyOnWriteHashMap;
import com.gemstone.gemfire.internal.util.concurrent.CopyOnWriteWeakHashMap;
import com.gemstone.gemfire.pdx.PdxSerializationException;
import com.gemstone.gemfire.pdx.PdxSerializer;
import com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer;


public class TypeRegistry {
  private static final Logger logger = LogService.getLogger();
  
  private static final boolean DISABLE_TYPE_REGISTRY 
      = Boolean.getBoolean("gemfire.TypeRegistry.DISABLE_PDX_REGISTRY"); 

  private final Map<Integer, PdxType> idToType = new CopyOnWriteHashMap<Integer, PdxType>();
  private final Map<PdxType, Integer> typeToId = new CopyOnWriteHashMap<PdxType, Integer>();
  private final Map<Class<?>, PdxType> localTypeIds = new CopyOnWriteWeakHashMap<Class<?>, PdxType>();
  private final Map<Class<?>, Map<Integer, UnreadPdxType>> localTypeIdMaps = new CopyOnWriteWeakHashMap<Class<?>, Map<Integer, UnreadPdxType>>();
  private final WeakConcurrentIdentityHashMap<Object, PdxUnreadData> unreadDataMap = WeakConcurrentIdentityHashMap.make();
  private final Map<Integer, EnumInfo> idToEnum = new CopyOnWriteHashMap<Integer, EnumInfo>();
  private final Map<EnumInfo, Integer> enumInfoToId = new CopyOnWriteHashMap<EnumInfo, Integer>();
  private final Map<Enum<?>, Integer> localEnumIds = new CopyOnWriteWeakHashMap<Enum<?>, Integer>();
  private final TypeRegistration distributedTypeRegistry;
  private final GemFireCacheImpl cache;
  
  public TypeRegistry(GemFireCacheImpl cache, boolean disableTypeRegistry) {
    this.cache = cache;

    if(DISABLE_TYPE_REGISTRY || disableTypeRegistry) {
      distributedTypeRegistry = new NullTypeRegistration();
    } else if (cache.hasPool()) {
      distributedTypeRegistry = new ClientTypeRegistration(cache);
    } else if (LonerTypeRegistration.isIndeterminateLoner(cache)) {
      distributedTypeRegistry = new LonerTypeRegistration(cache);
    } else {
      distributedTypeRegistry = new PeerTypeRegistration(cache);
    }
  }
 
  /*
   * Test Hook to clear the type registry
   */
  public void testClearTypeRegistry(){
    this.typeToId.clear();
    this.idToType.clear();
    this.idToEnum.clear();
    this.enumInfoToId.clear();
    distributedTypeRegistry.testClearRegistry();
  }
  public void testClearLocalTypeRegistry() {
    this.localTypeIds.clear();
    this.localTypeIdMaps.clear();
    this.localEnumIds.clear();
  }
 
  public static boolean mayNeedDiskStore(GemFireCacheImpl cache) {
    if (DISABLE_TYPE_REGISTRY) {
      return false;
    } else if (cache.hasPool()) {
      return false;
    } else {
      return cache.getPdxPersistent();
    }
  }
  public static String getPdxDiskStoreName(GemFireCacheImpl cache) {
    if (!mayNeedDiskStore(cache)) {
      return null;
    } else {
      String result = cache.getPdxDiskStore();
      if (result == null) {
        result = DiskStoreFactory.DEFAULT_DISK_STORE_NAME;
      }
      return result;
    }
  }
  
  public void initialize() {
    if(!cache.getPdxPersistent() 
        || cache.getPdxDiskStore() == null 
        || cache.findDiskStore(cache.getPdxDiskStore()) != null) {
      distributedTypeRegistry.initialize();
    }
  }

  public void flushCache() {
    InternalDataSerializer.flushClassCache();
    for (EnumInfo ei: this.idToEnum.values()) {
      ei.flushCache();
    }
  }
  
  public PdxType getType(int typeId) {
    PdxType pdxType = this.idToType.get(typeId);
    if(pdxType != null) {
      return pdxType;
    }
    
    synchronized (this) {
      pdxType = this.distributedTypeRegistry.getType(typeId);
      if(pdxType != null) {
        this.idToType.put(typeId, pdxType);
        this.typeToId.put(pdxType, typeId);
        if (logger.isDebugEnabled()) {
          logger.debug("Adding entry into pdx type registry, typeId: {}  {}", typeId, pdxType);
        }
        return pdxType;
      }
    }
    
    return null;
  }


  public PdxType getExistingType(Object o) {
    return getExistingTypeForClass(o.getClass());
  }

  public PdxType getExistingTypeForClass(Class<?> c) {
      return this.localTypeIds.get(c);
  }
  
  /**
   * Returns the local type that should be used for deserializing
   * blobs of the given typeId for the given local class.
   * Returns null if no such local type exists.
   */
  public UnreadPdxType getExistingTypeForClass(Class<?> c, int typeId) {
    Map<Integer, UnreadPdxType> m = this.localTypeIdMaps.get(c);
    if (m != null) {
      return m.get(typeId);
    } else {
      return null;
    }
  }
  public void defineUnreadType(Class<?> c, UnreadPdxType unreadPdxType) {
    int typeId = unreadPdxType.getTypeId();
    // even though localTypeIdMaps is copy on write we need to sync it
    // during write to safely update the nested map.
    // We make the nested map copy-on-write so that readers don't need to sync.
    synchronized (this.localTypeIdMaps) {
      Map<Integer, UnreadPdxType> m = this.localTypeIdMaps.get(c);
      if (m == null) {
        m = new CopyOnWriteHashMap<Integer, UnreadPdxType>();
        this.localTypeIdMaps.put(c, m);
      }
      m.put(typeId, unreadPdxType);
    }
  }

  /**
   * Create a type id for a type that may come locally, or
   * from a remote member.
   */
  public int defineType(PdxType newType) {
    Integer existingId = this.typeToId.get(newType);
    if (existingId != null) {
      int eid = existingId.intValue();
      newType.setTypeId(eid);
      return eid;
    }
    int id = distributedTypeRegistry.defineType(newType);
    newType.setTypeId(id);
    PdxType oldType = this.idToType.get(id);
    if(oldType == null) {
      this.idToType.put(id, newType);
      this.typeToId.put(newType, id);
      if (logger.isDebugEnabled()) {
        logger.debug("Defining: {}", newType.toFormattedString());
      }
    } else {
      //TODO - this might be overkill, but type definition should be rare enough.
      if(!oldType.equals(newType)) {
        Assert.fail("Old type does not equal new type for the same id. oldType=" + oldType + " new type=" + newType);
      }
    }
    
    return id;
  }
  
  public void addRemoteType(int typeId, PdxType newType) {
    PdxType oldType = this.idToType.get(typeId);
    if(oldType == null) {
      this.distributedTypeRegistry.addRemoteType(typeId, newType);
      this.idToType.put(typeId, newType);
      this.typeToId.put(newType, typeId);
      if (logger.isDebugEnabled()) {
        logger.debug("Adding, from remote WAN: {}", newType.toFormattedString());
      }
    } else {
    //TODO - this might be overkill, but type definition should be rare enough.
      if(!oldType.equals(newType)) {
        Assert.fail("Old type does not equal new type for the same id. oldType=" + oldType + " new type=" + newType);
      }
    }
  }

  /**
   * Create a type id for a type that was generated locally.
   */
  public PdxType defineLocalType(Object o, PdxType newType) {
    if (o != null) {
      PdxType t = getExistingType(o);
      if (t != null) {
        return t;
      }
      defineType(newType);
      this.localTypeIds.put(o.getClass(), newType);
    } else {
      // Defining a type for PdxInstanceFactory.
      defineType(newType);
    }
    
    return newType;
  }


  /**
   * Test hook that returns the most recently allocated type id
   * 
   * Note that this method will not work on clients.
   * 
   * @return the most recently allocated type id
   */
  public int getLastAllocatedTypeId() {
    return distributedTypeRegistry.getLastAllocatedTypeId();
  }
  
  public TypeRegistration getTypeRegistration() {
    return distributedTypeRegistry;
  }

  public void gatewaySenderStarted(GatewaySender gatewaySender) {
    if(distributedTypeRegistry != null) {
      distributedTypeRegistry.gatewaySenderStarted(gatewaySender);
    }
  }
  
  public void creatingDiskStore(DiskStore dsi) {
    if(cache.getPdxDiskStore() != null && dsi.getName().equals(cache.getPdxDiskStore())) {
      distributedTypeRegistry.initialize();
    }
  }
  
  public void creatingPersistentRegion() {
    distributedTypeRegistry.creatingPersistentRegion();
  }
  
  public void creatingPool() {
    distributedTypeRegistry.creatingPool();
  }
  
  // test hook
  public void removeLocal(Object o) {
    this.localTypeIds.remove(o.getClass());
  }

  public PdxUnreadData getUnreadData(Object o) {
    return this.unreadDataMap.get(o);
  }

  public void putUnreadData(Object o, PdxUnreadData ud) {
    this.unreadDataMap.put(o, ud);
  }
  
  private static final AtomicReference<PdxSerializer> pdxSerializer = new AtomicReference<PdxSerializer>(null);
  private static final AtomicReference<AutoSerializableManager> asm = new AtomicReference<AutoSerializableManager>(null);
  /**
   * To fix bug 45116 we want any attempt to get the PdxSerializer after it has been closed to fail with an exception.
   */
  private static volatile boolean open = false;
  /**
   * If the pdxSerializer is ever set to a non-null value then set this to true.
   * It gets reset to false when init() is called.
   * This was added to fix bug 45116.
   */
  private static volatile boolean pdxSerializerWasSet = false;
  
  public static void init() {
    pdxSerializerWasSet = false;
  }
  public static void open() {
    open = true;
  }
  public static void close() {
    open = false;
  }
  
  public static PdxSerializer getPdxSerializer() {
    PdxSerializer result = pdxSerializer.get();
    if (result == null && !open && pdxSerializerWasSet) {
      throw new CacheClosedException("Could not PDX serialize because the cache was closed");
    }
    return result;
  }
  public static AutoSerializableManager getAutoSerializableManager() {
    return asm.get();
  }
  public static void setPdxSerializer(PdxSerializer v) {
    if (v == null) {
      PdxSerializer oldValue = pdxSerializer.getAndSet(null);
      if (oldValue instanceof ReflectionBasedAutoSerializer) {
        asm.compareAndSet((AutoSerializableManager) ((ReflectionBasedAutoSerializer) oldValue).getManager(), null);
      }
    } else {
      pdxSerializerWasSet = true;
      pdxSerializer.set(v);
      if (v instanceof ReflectionBasedAutoSerializer) {
        asm.set((AutoSerializableManager) ((ReflectionBasedAutoSerializer) v).getManager());
      }
    }
  }

  /**
   * Given an enum compute and return a code for it.
   */
  public int getEnumId(Enum<?> v) {
    int result = 0;
    if (v != null) {
      Integer id = this.localEnumIds.get(v);
      if (id != null) {
        result = id.intValue();
      } else {
        result = distributedTypeRegistry.getEnumId(v);
        id = Integer.valueOf(result);
        this.localEnumIds.put(v, id);
        EnumInfo ei = new EnumInfo(v);
        this.idToEnum.put(id, ei);
        this.enumInfoToId.put(ei, id);
      }
    }
    return result;
  }

  public void addRemoteEnum(int enumId, EnumInfo newInfo) {
    EnumInfo oldInfo = this.idToEnum.get(enumId);
    if(oldInfo == null) {
      this.distributedTypeRegistry.addRemoteEnum(enumId, newInfo);
      this.idToEnum.put(enumId, newInfo);
      this.enumInfoToId.put(newInfo, enumId);
    } else {
    //TODO - this might be overkill, but enum definition should be rare enough.
      if(!oldInfo.equals(newInfo)) {
        Assert.fail("Old enum does not equal new enum for the same id. oldEnum=" + oldInfo + " new enum=" + newInfo);
      }
    }
  }

  public int defineEnum(EnumInfo newInfo) {
    Integer existingId = this.enumInfoToId.get(newInfo);
    if (existingId != null) {
      return existingId.intValue();
    }
    int id = distributedTypeRegistry.defineEnum(newInfo);
    EnumInfo oldInfo = this.idToEnum.get(id);
    if(oldInfo == null) {
      this.idToEnum.put(id, newInfo);
      this.enumInfoToId.put(newInfo, id);
    } else {
      //TODO - this might be overkill, but type definition should be rare enough.
      if(!oldInfo.equals(newInfo)) {
        Assert.fail("Old enum does not equal new enum for the same id. oldEnum=" + oldInfo + " newEnum=" + newInfo);
      }
    }
    return id;
  }

  public Object getEnumById(int enumId) {
    if (enumId == 0) {
      return null;
    }
    EnumInfo ei = getEnumInfoById(enumId);
    if (ei == null) {
      throw new PdxSerializationException("Could not find a PDX registration for the enum with id " + enumId);
    }
    if (this.cache.getPdxReadSerializedByAnyGemFireServices()) {
      return ei.getPdxInstance(enumId);
    } else {
      try {
        return ei.getEnum();
      } catch (ClassNotFoundException ex) {
        throw new PdxSerializationException("PDX enum field could not be read because the enum class could not be loaded", ex);
      }
    }
  }
  public EnumInfo getEnumInfoById(int enumId) {
    if (enumId == 0) {
      return null;
    }
    
    EnumInfo ei = this.idToEnum.get(enumId);
    if (ei == null) {
      ei = this.distributedTypeRegistry.getEnumById(enumId);
      if (ei != null) {
        this.idToEnum.put(enumId, ei);
        this.enumInfoToId.put(ei, enumId);
      }
    }
    return ei;
  }
  
  /**
   * Clear all of the cached PDX types in this registry. This method
   * is used on a client when the server side distributed system
   * is cycled
   */
  public void clear() {
    if(distributedTypeRegistry.isClient())  {
      idToType.clear();
      typeToId.clear();
      localTypeIds.clear();
      localTypeIdMaps.clear();
      unreadDataMap.clear();
      idToEnum.clear();
      enumInfoToId.clear();
      localEnumIds.clear();
      AutoSerializableManager autoSerializer = getAutoSerializableManager();
      if(autoSerializer != null) {
        autoSerializer.resetCachedTypes();
      }
    }
    
  }

  /**
   * Returns the currently defined types.
   * @return the types
   */
  public Map<Integer, PdxType> typeMap() {
    return distributedTypeRegistry.types();
  }
  
  /**
   * Returns the currently defined enums.
   * @return the enums
   */
  public Map<Integer, EnumInfo> enumMap() {
    return distributedTypeRegistry.enums();
  }
  
  /**
   * searches a field in different versions (PdxTypes) of a class in 
   * the distributed type registry 
   * 
   * @param fieldName
   *          the field to look for in the PdxTypes
   * @param className
   *          the PdxTypes for this class would be searched
   * @return PdxType having the field or null if not found
   * 
   */
  public PdxType getPdxTypeForField(String fieldName, String className) {
    return distributedTypeRegistry.getPdxTypeForField(fieldName, className);
  }
  
  public void addImportedType(int typeId, PdxType importedType) {
    PdxType existing = getType(typeId);
    if (existing != null && !existing.equals(importedType)) {
      throw new PdxSerializationException(LocalizedStrings.Snapshot_PDX_CONFLICT_0_1.toLocalizedString(importedType, existing));
    }
    
    this.distributedTypeRegistry.addImportedType(typeId, importedType);
    this.idToType.put(typeId, importedType);
    this.typeToId.put(importedType, typeId);
    if (logger.isDebugEnabled()) {
      logger.debug("Importing type: {}", importedType.toFormattedString());
    }
  }

  public void addImportedEnum(int enumId, EnumInfo importedEnum) {
    EnumInfo existing = getEnumInfoById(enumId);
    if (existing != null && !existing.equals(importedEnum)) {
      throw new PdxSerializationException(LocalizedStrings.Snapshot_PDX_CONFLICT_0_1.toLocalizedString(importedEnum, existing));
    }
    
    this.distributedTypeRegistry.addImportedEnum(enumId, importedEnum);
    this.idToEnum.put(enumId, importedEnum);
    this.enumInfoToId.put(importedEnum, enumId);
  }

  /**
   * Get the size of the the type registry in this local member
   */
  public int getLocalSize() {
    int result = distributedTypeRegistry.getLocalSize();
    if(result == 0) {
      //If this is the client, go ahead and return the number of cached types we have
      return idToType.size();
    }
    return result;
  }
}
