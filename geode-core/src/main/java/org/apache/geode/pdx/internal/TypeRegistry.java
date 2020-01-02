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

  private final Map<Class<?>, PdxType> localTypeIds = new CopyOnWriteWeakHashMap<>();

  private final Map<Class<?>, Map<Integer, UnreadPdxType>> localTypeIdMaps =
      new CopyOnWriteWeakHashMap<>();

  private final WeakConcurrentIdentityHashMap<Object, PdxUnreadData> unreadDataMap =
      WeakConcurrentIdentityHashMap.make();

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
    this.distributedTypeRegistry.flushCache();
  }

  public PdxType getType(int typeId) {
    synchronized (this) {
      PdxType pdxType = this.distributedTypeRegistry.getType(typeId);
      if (pdxType != null) {
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
        map = new CopyOnWriteHashMap<>();
        this.localTypeIdMaps.put(aClass, map);
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
    int id = this.distributedTypeRegistry.defineType(newType);
    PdxType oldType = this.distributedTypeRegistry.getType(id);
    if (!oldType.equals(newType)) {
      Assert.fail("Old type does not equal new type for the same id. oldType=" + oldType
          + " new type=" + newType);
    }
    return oldType;
  }

  public void addRemoteType(int typeId, PdxType newType) {
    PdxType oldType = distributedTypeRegistry.getType(typeId);
    if (oldType == null) {
      this.distributedTypeRegistry.addRemoteType(typeId, newType);
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
      PdxType existingType = getExistingType(o);
      if (existingType != null) {
        return existingType;
      }
      existingType = defineType(newType);
      this.localTypeIds.put(o.getClass(), existingType);
      return existingType;
    } else {
      // Defining a type for PdxInstanceFactory.
      return defineType(newType);
    }
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

  public static void setPdxSerializer(PdxSerializer serializer) {
    if (serializer == null) {
      PdxSerializer oldValue = pdxSerializer.getAndSet(null);
      if (oldValue instanceof ReflectionBasedAutoSerializer) {
        asm.compareAndSet(
            (AutoSerializableManager) ((ReflectionBasedAutoSerializer) oldValue).getManager(),
            null);
      }
    } else {
      pdxSerializerWasSet = true;
      pdxSerializer.set(serializer);
      if (serializer instanceof ReflectionBasedAutoSerializer) {
        asm.set(
            (AutoSerializableManager) ((ReflectionBasedAutoSerializer) serializer).getManager());
      }
    }
  }

  /**
   * Given an enum compute and return a code for it.
   */
  public int getEnumId(Enum<?> anEnum) {
    int result = 0;
    if (anEnum != null) {
      Integer id = this.localEnumIds.get(anEnum);
      if (id != null) {
        result = id;
      } else {
        result = this.distributedTypeRegistry.getEnumId(anEnum);
        id = result;
        this.localEnumIds.put(anEnum, id);
      }
    }
    return result;
  }

  public void addRemoteEnum(int enumId, EnumInfo newInfo) {
    EnumInfo oldInfo = this.distributedTypeRegistry.getEnumById(enumId);
    if (oldInfo == null) {
      this.distributedTypeRegistry.addRemoteEnum(enumId, newInfo);
      if (logger.isInfoEnabled()) {
        logger.info("Adding, from remote WAN: {}", newInfo.toFormattedString());
      }
    } else if (!oldInfo.equals(newInfo)) {
      Assert.fail("Old enum does not equal new enum for the same id. oldEnum=" + oldInfo
          + " new enum=" + newInfo);
    }
  }

  public int defineEnum(EnumInfo newInfo) {
    int id = this.distributedTypeRegistry.defineEnum(newInfo);
    EnumInfo oldInfo = this.distributedTypeRegistry.getEnumById(id);
    if (!oldInfo.equals(newInfo)) {
      Assert.fail("Old enum does not equal new enum for the same id. oldEnum=" + oldInfo
          + " newEnum=" + newInfo);
    }
    return id;
  }

  public Object getEnumById(int enumId) {
    if (enumId == 0) {
      return null;
    }
    EnumInfo enumInfo = getEnumInfoById(enumId);
    if (enumInfo == null) {
      throw new PdxSerializationException(
          "Could not find a PDX registration for the enum with id " + enumId);
    }
    if (this.cache.getPdxReadSerializedByAnyGemFireServices()) {
      return enumInfo.getPdxInstance(enumId);
    } else {
      try {
        return enumInfo.getEnum();
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
    return this.distributedTypeRegistry.getEnumById(enumId);
  }

  /**
   * Clear all of the cached PDX types in this registry. This method is used on a client when the
   * server side distributed system is cycled
   */
  public void clear() {
    if (this.distributedTypeRegistry.isClient()) {
      this.distributedTypeRegistry.clearLocalMaps();
      this.localTypeIds.clear();
      this.localTypeIdMaps.clear();
      this.unreadDataMap.clear();
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
  @SuppressWarnings("unused")
  public Set<PdxType> getPdxTypesForClassName(String className) {
    return this.distributedTypeRegistry.getPdxTypesForClassName(className);
  }

  public void addImportedType(int typeId, PdxType importedType) {
    PdxType existing = getType(typeId);
    if (existing != null && !existing.equals(importedType)) {
      throw new PdxSerializationException(
          String.format(
              "Detected conflicting PDX types during import:%s%sSnapshot data containing PDX types must be imported into an empty cache with no pre-existing type definitions. Allow the import to complete prior to inserting additional data into the cache.",
              importedType, existing));
    }

    this.distributedTypeRegistry.addImportedType(typeId, importedType);
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

    this.distributedTypeRegistry.addImportedEnum(enumId, importedEnum);
    if (logger.isInfoEnabled()) {
      logger.info("Importing type: {}", importedEnum.toFormattedString());
    }
  }

  /**
   * Get the size of the the type registry in this local member
   */
  int getLocalSize() {
    return this.distributedTypeRegistry.getLocalSize();
  }

  public Boolean getPdxReadSerializedOverride() {
    return pdxReadSerializedOverride.get();
  }

  public void setPdxReadSerializedOverride(boolean overridePdxReadSerialized) {
    pdxReadSerializedOverride.set(overridePdxReadSerialized);
  }

  // accessors for unit test
  Map<Enum<?>, Integer> getLocalEnumIds() {
    return localEnumIds;
  }

  Map<Class<?>, PdxType> getLocalTypeIds() {
    return localTypeIds;
  }
}
