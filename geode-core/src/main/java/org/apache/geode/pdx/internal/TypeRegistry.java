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

import static java.lang.Integer.valueOf;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.util.concurrent.CopyOnWriteHashMap;
import org.apache.geode.internal.util.concurrent.CopyOnWriteWeakHashMap;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.util.internal.GeodeGlossary;

public class TypeRegistry {
  private static final Logger logger = LogService.getLogger();

  private static final boolean DISABLE_TYPE_REGISTRY =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "TypeRegistry.DISABLE_PDX_REGISTRY");

  private final Map<Integer, PdxType> idToType = new CopyOnWriteHashMap<>();

  private final Map<PdxType, Integer> typeToId = new CopyOnWriteHashMap<>();

  private final Map<Class<?>, PdxType> localTypeIds = new CopyOnWriteWeakHashMap<>();

  private final Map<Class<?>, Map<Integer, UnreadPdxType>> localTypeIdMaps =
      new CopyOnWriteWeakHashMap<>();

  private final WeakConcurrentIdentityHashMap<Object, PdxUnreadData> unreadDataMap =
      WeakConcurrentIdentityHashMap.make();

  private final Map<Integer, EnumInfo> idToEnum = new CopyOnWriteHashMap<>();

  private final Map<EnumInfo, Integer> enumInfoToId = new CopyOnWriteHashMap<>();

  private final Map<Enum<?>, Integer> localEnumIds = new CopyOnWriteWeakHashMap<>();

  private final TypeRegistration distributedTypeRegistry;

  private final InternalCache cache;

  private final ThreadLocal<Boolean> pdxReadSerializedOverride =
      ThreadLocal.withInitial(() -> Boolean.FALSE);

  public TypeRegistry(InternalCache cache, boolean disableTypeRegistry) {
    this.cache = cache;

    if (DISABLE_TYPE_REGISTRY || disableTypeRegistry) {
      distributedTypeRegistry = new NullTypeRegistration();
    } else if (cache.hasPool()) {
      distributedTypeRegistry = new ClientTypeRegistration(cache);
    } else if (LonerTypeRegistration.isIndeterminateLoner(cache)) {
      distributedTypeRegistry = new LonerTypeRegistration(cache);
    } else {
      distributedTypeRegistry = new PeerTypeRegistration(cache);
    }
  }

  TypeRegistry(InternalCache cache, TypeRegistration distributedTypeRegistry) {
    this.cache = cache;
    this.distributedTypeRegistry = distributedTypeRegistry;
  }

  public void testClearLocalTypeRegistry() {
    localTypeIds.clear();
    localTypeIdMaps.clear();
    localEnumIds.clear();
  }

  public static boolean mayNeedDiskStore(InternalCache cache) {
    return !DISABLE_TYPE_REGISTRY && !cache.hasPool() && cache.getPdxPersistent();
  }

  public static String getPdxDiskStoreName(InternalCache cache) {
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
    if (!cache.getPdxPersistent() || cache.getPdxDiskStore() == null
        || cache.findDiskStore(cache.getPdxDiskStore()) != null) {
      distributedTypeRegistry.initialize();
    }
  }

  public void flushCache() {
    InternalDataSerializer.flushClassCache();
    for (EnumInfo ei : idToEnum.values()) {
      ei.flushCache();
    }
  }

  public PdxType getType(int typeId) {
    PdxType pdxType = idToType.get(typeId);
    if (pdxType != null) {
      return pdxType;
    }

    synchronized (this) {
      pdxType = distributedTypeRegistry.getType(typeId);
      if (pdxType != null) {
        idToType.put(typeId, pdxType);
        typeToId.put(pdxType, typeId);
        if (logger.isInfoEnabled()) {
          logger.info("Adding: {}", pdxType.toFormattedString());
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Adding entry into pdx type registry, typeId: {}  {}", typeId, pdxType);
        }
        return pdxType;
      }
    }

    return null;
  }

  PdxType getExistingType(Object o) {
    return getExistingTypeForClass(o.getClass());
  }

  public PdxType getExistingTypeForClass(Class<?> aClass) {
    PdxType result = localTypeIds.get(aClass);
    return result;
  }

  /**
   * Returns the local type that should be used for deserializing blobs of the given typeId for the
   * given local class. Returns null if no such local type exists.
   */
  UnreadPdxType getExistingTypeForClass(Class<?> aClass, int typeId) {
    Map<Integer, UnreadPdxType> map = localTypeIdMaps.get(aClass);
    if (map != null) {
      return map.get(typeId);
    } else {
      return null;
    }
  }

  void defineUnreadType(Class<?> aClass, UnreadPdxType unreadPdxType) {
    int typeId = unreadPdxType.getTypeId();
    // even though localTypeIdMaps is copy on write we need to sync it
    // during write to safely update the nested map.
    // We make the nested map copy-on-write so that readers don't need to sync.
    synchronized (localTypeIdMaps) {
      Map<Integer, UnreadPdxType> map = localTypeIdMaps.get(aClass);
      if (map == null) {
        map = new CopyOnWriteHashMap<Integer, UnreadPdxType>();
        localTypeIdMaps.put(aClass, map);
      }
      map.put(typeId, unreadPdxType);
    }
  }

  /**
   * Create a type id for a type that may come locally, or from a remote member.
   *
   * @return the existing type or the new type
   */
  public PdxType defineType(PdxType newType) {
    Integer existingId = typeToId.get(newType);
    if (existingId != null) {
      PdxType existingType = idToType.get(existingId);
      if (existingType != null) {
        return existingType;
      }
    }

    int id = distributedTypeRegistry.defineType(newType);
    PdxType oldType = idToType.get(id);
    if (oldType == null) {
      newType.setTypeId(id);
      idToType.put(id, newType);
      typeToId.put(newType, id);
      if (logger.isInfoEnabled()) {
        logger.info("Caching {}", newType.toFormattedString());
      }
      return newType;
    } else {
      if (!oldType.equals(newType)) {
        Assert.fail("Old type does not equal new type for the same id. oldType=" + oldType
            + " new type=" + newType);
      }
      return oldType;
    }
  }

  public void addRemoteType(int typeId, PdxType newType) {
    PdxType oldType = idToType.get(typeId);
    if (oldType == null) {
      distributedTypeRegistry.addRemoteType(typeId, newType);
      idToType.put(typeId, newType);
      typeToId.put(newType, typeId);
      if (logger.isInfoEnabled()) {
        logger.info("Adding, from remote WAN: {}", newType.toFormattedString());
      }
    } else if (!oldType.equals(newType)) {
      Assert.fail("Old type does not equal new type for the same id. oldType=" + oldType
          + " new type=" + newType);
    }
  }

  /**
   * Create a type id for a type that was generated locally.
   */
  PdxType defineLocalType(Object o, PdxType newType) {
    if (o != null) {
      PdxType t = getExistingType(o);
      if (t != null) {
        return t;
      }
      PdxType existingType = defineType(newType);
      localTypeIds.put(o.getClass(), existingType);
      return existingType;
    } else {
      // Defining a type for PdxInstanceFactory.
      return defineType(newType);
    }
  }

  public TypeRegistration getTypeRegistration() {
    return distributedTypeRegistry;
  }

  public void gatewaySenderStarted(GatewaySender gatewaySender) {
    if (distributedTypeRegistry != null) {
      distributedTypeRegistry.gatewaySenderStarted(gatewaySender);
    }
  }

  public void creatingDiskStore(DiskStore dsi) {
    if (cache.getPdxDiskStore() != null
        && dsi.getName().equals(cache.getPdxDiskStore())) {
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
    localTypeIds.remove(o.getClass());
  }

  PdxUnreadData getUnreadData(Object o) {
    return unreadDataMap.get(o);
  }

  void putUnreadData(Object o, PdxUnreadData ud) {
    unreadDataMap.put(o, ud);
  }

  @MakeNotStatic
  private static final AtomicReference<PdxSerializer> pdxSerializer = new AtomicReference<>(null);

  @MakeNotStatic
  private static final AtomicReference<AutoSerializableManager> asm = new AtomicReference<>(null);

  /**
   * To fix bug 45116 we want any attempt to get the PdxSerializer after it has been closed to fail
   * with an exception.
   */
  @MakeNotStatic
  private static volatile boolean open = false;

  /**
   * If the pdxSerializer is ever set to a non-null value then set this to true. It gets reset to
   * false when init() is called. This was added to fix bug 45116.
   */
  @MakeNotStatic
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
        asm.compareAndSet(
            (AutoSerializableManager) ((ReflectionBasedAutoSerializer) oldValue).getManager(),
            null);
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
      Integer id = localEnumIds.get(v);
      if (id != null) {
        result = id;
      } else {
        result = distributedTypeRegistry.getEnumId(v);
        id = valueOf(result);
        localEnumIds.put(v, id);
        EnumInfo ei = new EnumInfo(v);
        idToEnum.put(id, ei);
        enumInfoToId.put(ei, id);
      }
    }
    return result;
  }

  public void addRemoteEnum(int enumId, EnumInfo newInfo) {
    EnumInfo oldInfo = idToEnum.get(enumId);
    if (oldInfo == null) {
      distributedTypeRegistry.addRemoteEnum(enumId, newInfo);
      idToEnum.put(enumId, newInfo);
      enumInfoToId.put(newInfo, enumId);
    } else if (!oldInfo.equals(newInfo)) {
      Assert.fail("Old enum does not equal new enum for the same id. oldEnum=" + oldInfo
          + " new enum=" + newInfo);
    }
  }

  public int defineEnum(EnumInfo newInfo) {
    Integer existingId = enumInfoToId.get(newInfo);
    if (existingId != null) {
      return existingId;
    }
    int id = distributedTypeRegistry.defineEnum(newInfo);
    EnumInfo oldInfo = idToEnum.get(id);
    if (oldInfo == null) {
      idToEnum.put(id, newInfo);
      enumInfoToId.put(newInfo, id);
      if (logger.isInfoEnabled()) {
        logger.info("Caching PDX Enum: {}, dsid={} typenum={}", newInfo, id >> 24, id & 0xFFFFFF);
      }
    } else if (!oldInfo.equals(newInfo)) {
      Assert.fail("Old enum does not equal new enum for the same id. oldEnum=" + oldInfo
          + " newEnum=" + newInfo);
    }
    return id;
  }

  public Object getEnumById(int enumId) {
    if (enumId == 0) {
      return null;
    }
    EnumInfo ei = getEnumInfoById(enumId);
    if (ei == null) {
      throw new PdxSerializationException(
          "Could not find a PDX registration for the enum with id " + enumId);
    }
    if (cache.getPdxReadSerializedByAnyGemFireServices()) {
      return ei.getPdxInstance(enumId);
    } else {
      try {
        return ei.getEnum();
      } catch (ClassNotFoundException ex) {
        throw new PdxSerializationException(
            "PDX enum field could not be read because the enum class could not be loaded", ex);
      }
    }
  }

  public EnumInfo getEnumInfoById(int enumId) {
    if (enumId == 0) {
      return null;
    }

    EnumInfo ei = idToEnum.get(enumId);
    if (ei == null) {
      ei = distributedTypeRegistry.getEnumById(enumId);
      if (ei != null) {
        idToEnum.put(enumId, ei);
        enumInfoToId.put(ei, enumId);
      }
    }
    return ei;
  }

  /**
   * Clear all of the cached PDX types in this registry. This method is used on a client when the
   * server side distributed system is cycled
   */
  public void clear() {
    if (distributedTypeRegistry.isClient()) {
      idToType.clear();
      typeToId.clear();
      localTypeIds.clear();
      localTypeIdMaps.clear();
      unreadDataMap.clear();
      idToEnum.clear();
      enumInfoToId.clear();
      localEnumIds.clear();
      AutoSerializableManager autoSerializer = getAutoSerializableManager();
      if (autoSerializer != null) {
        autoSerializer.resetCachedTypes();
      }
    }
  }

  /**
   * Returns the currently defined types.
   *
   * @return the types
   */
  public Map<Integer, PdxType> typeMap() {
    return distributedTypeRegistry.types();
  }

  /**
   * Returns the currently defined enums.
   *
   * @return the enums
   */
  public Map<Integer, EnumInfo> enumMap() {
    return distributedTypeRegistry.enums();
  }

  /**
   * searches a field in different versions (PdxTypes) of a class in the distributed type registry
   *
   * @param fieldName the field to look for in the PdxTypes
   * @param className the PdxTypes for this class would be searched
   * @return PdxType having the field or null if not found
   */
  public PdxType getPdxTypeForField(String fieldName, String className) {
    return distributedTypeRegistry.getPdxTypeForField(fieldName, className);
  }

  /**
   * Returns all the PdxTypes for the given class name.
   * An empty set will be returned if no types exist.
   */
  public Set<PdxType> getPdxTypesForClassName(String className) {
    return distributedTypeRegistry.getPdxTypesForClassName(className);
  }

  public void addImportedType(int typeId, PdxType importedType) {
    PdxType existing = getType(typeId);
    if (existing != null && !existing.equals(importedType)) {
      throw new PdxSerializationException(
          String.format(
              "Detected conflicting PDX types during import:%s%sSnapshot data containing PDX types must be imported into an empty cache with no pre-existing type definitions. Allow the import to complete prior to inserting additional data into the cache.",
              importedType, existing));
    }

    distributedTypeRegistry.addImportedType(typeId, importedType);
    idToType.put(typeId, importedType);
    typeToId.put(importedType, typeId);
    if (logger.isInfoEnabled()) {
      logger.info("Importing type: {}", importedType.toFormattedString());
    }
  }

  public void addImportedEnum(int enumId, EnumInfo importedEnum) {
    EnumInfo existing = getEnumInfoById(enumId);
    if (existing != null && !existing.equals(importedEnum)) {
      throw new PdxSerializationException(
          String.format(
              "Detected conflicting PDX types during import:%s%sSnapshot data containing PDX types must be imported into an empty cache with no pre-existing type definitions. Allow the import to complete prior to inserting additional data into the cache.",
              importedEnum, existing));
    }

    distributedTypeRegistry.addImportedEnum(enumId, importedEnum);
    idToEnum.put(enumId, importedEnum);
    enumInfoToId.put(importedEnum, enumId);
  }

  /**
   * Get the size of the the type registry in this local member
   */
  int getLocalSize() {
    int result = distributedTypeRegistry.getLocalSize();
    if (result == 0) {
      // If this is the client, go ahead and return the number of cached types we have
      return idToType.size();
    }
    return result;
  }

  public Boolean getPdxReadSerializedOverride() {
    return pdxReadSerializedOverride.get();
  }

  public void setPdxReadSerializedOverride(boolean overridePdxReadSerialized) {
    pdxReadSerializedOverride.set(overridePdxReadSerialized);
  }

  // accessors for unit test

  Map<Integer, PdxType> getIdToType() {
    return idToType;
  }

  Map<PdxType, Integer> getTypeToId() {
    return typeToId;
  }

  Map<Class<?>, PdxType> getLocalTypeIds() {
    return localTypeIds;
  }
}
