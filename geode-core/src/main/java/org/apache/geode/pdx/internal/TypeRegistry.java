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

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.concurrent.CopyOnWriteHashMap;
import org.apache.geode.internal.util.concurrent.CopyOnWriteWeakHashMap;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

public class TypeRegistry {
  private static final Logger logger = LogService.getLogger();

  private static final boolean DISABLE_TYPE_REGISTRY =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "TypeRegistry.DISABLE_PDX_REGISTRY");

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
      this.distributedTypeRegistry = new NullTypeRegistration();
    } else if (cache.hasPool()) {
      this.distributedTypeRegistry = new ClientTypeRegistration(cache);
    } else if (LonerTypeRegistration.isIndeterminateLoner(cache)) {
      this.distributedTypeRegistry = new LonerTypeRegistration(cache);
    } else {
      this.distributedTypeRegistry = new PeerTypeRegistration(cache);
    }
  }

  TypeRegistry(InternalCache cache, TypeRegistration distributedTypeRegistry) {
    this.cache = cache;
    this.distributedTypeRegistry = distributedTypeRegistry;
  }

  public void testClearLocalTypeRegistry() {
    this.localTypeIds.clear();
    this.localTypeIdMaps.clear();
    this.localEnumIds.clear();
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
    if (!this.cache.getPdxPersistent() || this.cache.getPdxDiskStore() == null
        || this.cache.findDiskStore(this.cache.getPdxDiskStore()) != null) {
      this.distributedTypeRegistry.initialize();
    }
  }

  public void flushCache() {
    InternalDataSerializer.flushClassCache();
    for (EnumInfo ei : this.idToEnum.values()) {
      ei.flushCache();
    }
  }

  public PdxType getType(int typeId) {
    PdxType pdxType = this.idToType.get(typeId);
    if (pdxType != null) {
      return pdxType;
    }

    synchronized (this) {
      pdxType = this.distributedTypeRegistry.getType(typeId);
      if (pdxType != null) {
        this.idToType.put(typeId, pdxType);
        this.typeToId.put(pdxType, typeId);
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
    return this.localTypeIds.get(aClass);
  }

  /**
   * Returns the local type that should be used for deserializing blobs of the given typeId for the
   * given local class. Returns null if no such local type exists.
   */
  UnreadPdxType getExistingTypeForClass(Class<?> aClass, int typeId) {
    Map<Integer, UnreadPdxType> map = this.localTypeIdMaps.get(aClass);
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
    synchronized (this.localTypeIdMaps) {
      Map<Integer, UnreadPdxType> map = this.localTypeIdMaps.get(aClass);
      if (map == null) {
        map = new CopyOnWriteHashMap<Integer, UnreadPdxType>();
        this.localTypeIdMaps.put(aClass, map);
      }
      map.put(typeId, unreadPdxType);
    }
  }

  /**
   * Create a type id for a type that may come locally, or from a remote member.
   */
  public int defineType(PdxType newType) {
    Integer existingId = this.typeToId.get(newType);
    if (existingId != null) {
      int eid = existingId;
      newType.setTypeId(eid);
      return eid;
    }

    int id = this.distributedTypeRegistry.defineType(newType);
    newType.setTypeId(id);
    PdxType oldType = this.idToType.get(id);
    if (oldType == null) {
      this.idToType.put(id, newType);
      this.typeToId.put(newType, id);
      if (logger.isInfoEnabled()) {
        logger.info("Caching {}", newType.toFormattedString());
      }
    } else if (!oldType.equals(newType)) {
      Assert.fail("Old type does not equal new type for the same id. oldType=" + oldType
          + " new type=" + newType);
    }

    return id;
  }

  public void addRemoteType(int typeId, PdxType newType) {
    PdxType oldType = this.idToType.get(typeId);
    if (oldType == null) {
      this.distributedTypeRegistry.addRemoteType(typeId, newType);
      this.idToType.put(typeId, newType);
      this.typeToId.put(newType, typeId);
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
      defineType(newType);
      this.localTypeIds.put(o.getClass(), newType);
    } else {
      // Defining a type for PdxInstanceFactory.
      defineType(newType);
    }

    return newType;
  }

  public TypeRegistration getTypeRegistration() {
    return this.distributedTypeRegistry;
  }

  public void gatewaySenderStarted(GatewaySender gatewaySender) {
    if (this.distributedTypeRegistry != null) {
      this.distributedTypeRegistry.gatewaySenderStarted(gatewaySender);
    }
  }

  public void creatingDiskStore(DiskStore dsi) {
    if (this.cache.getPdxDiskStore() != null
        && dsi.getName().equals(this.cache.getPdxDiskStore())) {
      this.distributedTypeRegistry.initialize();
    }
  }

  public void creatingPersistentRegion() {
    this.distributedTypeRegistry.creatingPersistentRegion();
  }

  public void creatingPool() {
    this.distributedTypeRegistry.creatingPool();
  }

  // test hook
  public void removeLocal(Object o) {
    this.localTypeIds.remove(o.getClass());
  }

  PdxUnreadData getUnreadData(Object o) {
    return this.unreadDataMap.get(o);
  }

  void putUnreadData(Object o, PdxUnreadData ud) {
    this.unreadDataMap.put(o, ud);
  }

  private static final AtomicReference<PdxSerializer> pdxSerializer = new AtomicReference<>(null);

  private static final AtomicReference<AutoSerializableManager> asm = new AtomicReference<>(null);

  /**
   * To fix bug 45116 we want any attempt to get the PdxSerializer after it has been closed to fail
   * with an exception.
   */
  private static volatile boolean open = false;

  /**
   * If the pdxSerializer is ever set to a non-null value then set this to true. It gets reset to
   * false when init() is called. This was added to fix bug 45116.
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
      Integer id = this.localEnumIds.get(v);
      if (id != null) {
        result = id;
      } else {
        result = this.distributedTypeRegistry.getEnumId(v);
        id = valueOf(result);
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
    if (oldInfo == null) {
      this.distributedTypeRegistry.addRemoteEnum(enumId, newInfo);
      this.idToEnum.put(enumId, newInfo);
      this.enumInfoToId.put(newInfo, enumId);
    } else if (!oldInfo.equals(newInfo)) {
      Assert.fail("Old enum does not equal new enum for the same id. oldEnum=" + oldInfo
          + " new enum=" + newInfo);
    }
  }

  public int defineEnum(EnumInfo newInfo) {
    Integer existingId = this.enumInfoToId.get(newInfo);
    if (existingId != null) {
      return existingId;
    }
    int id = this.distributedTypeRegistry.defineEnum(newInfo);
    EnumInfo oldInfo = this.idToEnum.get(id);
    if (oldInfo == null) {
      this.idToEnum.put(id, newInfo);
      this.enumInfoToId.put(newInfo, id);
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
    if (this.cache.getPdxReadSerializedByAnyGemFireServices()) {
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
   * Clear all of the cached PDX types in this registry. This method is used on a client when the
   * server side distributed system is cycled
   */
  public void clear() {
    if (this.distributedTypeRegistry.isClient()) {
      this.idToType.clear();
      this.typeToId.clear();
      this.localTypeIds.clear();
      this.localTypeIdMaps.clear();
      this.unreadDataMap.clear();
      this.idToEnum.clear();
      this.enumInfoToId.clear();
      this.localEnumIds.clear();
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
    return this.distributedTypeRegistry.types();
  }

  /**
   * Returns the currently defined enums.
   *
   * @return the enums
   */
  public Map<Integer, EnumInfo> enumMap() {
    return this.distributedTypeRegistry.enums();
  }

  /**
   * searches a field in different versions (PdxTypes) of a class in the distributed type registry
   *
   * @param fieldName the field to look for in the PdxTypes
   * @param className the PdxTypes for this class would be searched
   * @return PdxType having the field or null if not found
   */
  public PdxType getPdxTypeForField(String fieldName, String className) {
    return this.distributedTypeRegistry.getPdxTypeForField(fieldName, className);
  }

  /**
   * Returns all the PdxTypes for the given class name.
   * An empty set will be returned if no types exist.
   */
  public Set<PdxType> getPdxTypesForClassName(String className) {
    return this.distributedTypeRegistry.getPdxTypesForClassName(className);
  }

  public void addImportedType(int typeId, PdxType importedType) {
    PdxType existing = getType(typeId);
    if (existing != null && !existing.equals(importedType)) {
      throw new PdxSerializationException(
          LocalizedStrings.Snapshot_PDX_CONFLICT_0_1.toLocalizedString(importedType, existing));
    }

    this.distributedTypeRegistry.addImportedType(typeId, importedType);
    this.idToType.put(typeId, importedType);
    this.typeToId.put(importedType, typeId);
    if (logger.isInfoEnabled()) {
      logger.info("Importing type: {}", importedType.toFormattedString());
    }
  }

  public void addImportedEnum(int enumId, EnumInfo importedEnum) {
    EnumInfo existing = getEnumInfoById(enumId);
    if (existing != null && !existing.equals(importedEnum)) {
      throw new PdxSerializationException(
          LocalizedStrings.Snapshot_PDX_CONFLICT_0_1.toLocalizedString(importedEnum, existing));
    }

    this.distributedTypeRegistry.addImportedEnum(enumId, importedEnum);
    this.idToEnum.put(enumId, importedEnum);
    this.enumInfoToId.put(importedEnum, enumId);
  }

  /**
   * Get the size of the the type registry in this local member
   */
  int getLocalSize() {
    int result = this.distributedTypeRegistry.getLocalSize();
    if (result == 0) {
      // If this is the client, go ahead and return the number of cached types we have
      return this.idToType.size();
    }
    return result;
  }

  public Boolean getPdxReadSerializedOverride() {
    return pdxReadSerializedOverride.get();
  }

  public void setPdxReadSerializedOverride(boolean overridePdxReadSerialized) {
    pdxReadSerializedOverride.set(overridePdxReadSerialized);
  }
}
