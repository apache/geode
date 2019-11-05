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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.AddPDXEnumOp;
import org.apache.geode.cache.client.internal.AddPDXTypeOp;
import org.apache.geode.cache.client.internal.ExecutablePool;
import org.apache.geode.cache.client.internal.GetPDXEnumByIdOp;
import org.apache.geode.cache.client.internal.GetPDXEnumsOp;
import org.apache.geode.cache.client.internal.GetPDXIdForEnumOp;
import org.apache.geode.cache.client.internal.GetPDXIdForTypeOp;
import org.apache.geode.cache.client.internal.GetPDXTypeByIdOp;
import org.apache.geode.cache.client.internal.GetPDXTypesOp;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ClientTypeRegistration implements TypeRegistration {

  private static final Logger logger = LogService.getLogger();

  private final Map<Integer, PdxType> idToType = Collections.synchronizedMap(new HashMap<>());
  private final Map<EnumId, EnumInfo> idToEnum = Collections.synchronizedMap(new HashMap<>());
  private final TypeRegistryReverseMap reverseMap = new TypeRegistryReverseMap();

  private final InternalCache cache;

  public ClientTypeRegistration(InternalCache cache) {
    this.cache = cache;

    // See GEODE-5771: Even when set, PDX persistence is internally ignored.
    if (cache.getPdxPersistent() || StringUtils.isNotBlank(cache.getPdxDiskStore())) {
      logger.warn("PDX persistence is not supported on client side.");
    }
  }

  @Override
  public int defineType(PdxType newType) {
    Integer existingId = reverseMap.typeToId.get(newType);
    if (existingId != null) {
      return existingId;
    }
    Collection<Pool> pools = getAllPools();

    ServerConnectivityException lastException = null;
    int newTypeId = -1;
    for (Pool pool : pools) {
      try {
        newTypeId = GetPDXIdForTypeOp.execute((ExecutablePool) pool, newType);
        newType.setTypeId(newTypeId);
        idToType.put(newTypeId, newType);
        reverseMap.save(newTypeId, newType);
        copyTypeToOtherPools(newType, newTypeId, pool);
        return newTypeId;
      } catch (ServerConnectivityException e) {
        // ignore, try the next pool.
        lastException = e;
      }
    }
    throw returnCorrectExceptionForFailure(newTypeId, lastException);
  }

  /**
   * Send a type to all pools. This used to make sure that any types
   * used by this client make it to all clusters this client is connected to.
   */
  private void copyTypeToOtherPools(PdxType newType, int newTypeId, Pool exception) {
    Collection<Pool> pools = getAllPoolsExcept(exception);
    for (Pool pool : pools) {
      try {
        sendTypeToPool(newType, newTypeId, pool);
      } catch (ServerConnectivityException e) {
        logger.debug("Received an exception sending pdx type to pool {}, {}", pool, e.getMessage(),
            e);
      }
    }
  }

  private Collection<Pool> getAllPoolsExcept(Pool pool) {
    Collection<Pool> targetPools = new ArrayList<>(getAllPools());
    targetPools.remove(pool);
    return targetPools;
  }

  private void sendTypeToPool(PdxType type, int id, Pool pool) {
    try {
      AddPDXTypeOp.execute((ExecutablePool) pool, id, type);
    } catch (ServerConnectivityException serverConnectivityException) {
      logger.debug("Received an exception sending pdx type to pool {}, {}", pool,
          serverConnectivityException.getMessage(), serverConnectivityException);
      throw serverConnectivityException;
    }
  }

  @Override
  public PdxType getType(int typeId) {
    PdxType existingType = idToType.get(typeId);
    if (existingType != null) {
      return existingType;
    }
    Collection<Pool> pools = getAllPools();

    ServerConnectivityException lastException = null;
    for (Pool pool : pools) {
      try {
        PdxType type = GetPDXTypeByIdOp.execute((ExecutablePool) pool, typeId);
        if (type != null) {
          idToType.put(typeId, type);
          reverseMap.save(typeId, type);
          return type;
        }
      } catch (ServerConnectivityException e) {
        logger.debug("Received an exception getting pdx type from pool {}, {}", pool,
            e.getMessage(), e);
        // ignore, try the next pool.
        lastException = e;
      }
    }
    throw returnCorrectExceptionForFailure(typeId, lastException);
  }

  private Collection<Pool> getAllPools() {
    Collection<Pool> pools = PoolManagerImpl.getPMI().getMap().values();

    for (Iterator<Pool> itr = pools.iterator(); itr.hasNext();) {
      PoolImpl pool = (PoolImpl) itr.next();
      if (pool.isUsedByGateway()) {
        itr.remove();
      }
    }

    if (pools.isEmpty()) {
      if (this.cache.isClosed()) {
        throw cache.getCacheClosedException("PDX detected cache was closed");
      }
      throw cache.getCacheClosedException(
          "Client pools have been closed so the PDX type registry is not available.");
    }
    return pools;
  }

  @Override
  public void addRemoteType(int typeId, PdxType type) {
    throw new UnsupportedOperationException("Clients will not be asked to add remote types");
  }

  @SuppressWarnings("unused")
  public int getLastAllocatedTypeId() {
    throw new UnsupportedOperationException("Clients does not keep track of last allocated id");
  }

  @Override
  public void initialize() {
    // do nothing
  }

  @Override
  public void gatewaySenderStarted(GatewaySender gatewaySender) {
    // do nothing
  }

  @Override
  public void creatingPersistentRegion() {
    // do nothing
  }

  @Override
  public void creatingPool() {
    // do nothing
  }

  @Override
  public int getEnumId(Enum<?> v) {
    EnumInfo enumInfo = new EnumInfo(v);
    return processEnumInfoForEnumId(enumInfo);
  }

  private int processEnumInfoForEnumId(EnumInfo enumInfo) {
    EnumId existingId = reverseMap.enumToId.get(enumInfo);
    if (existingId != null) {
      return existingId.intValue();
    }

    Collection<Pool> pools = getAllPools();
    ServerConnectivityException lastException = null;
    for (Pool pool : pools) {
      try {
        int result = GetPDXIdForEnumOp.execute((ExecutablePool) pool, enumInfo);
        EnumId newId = new EnumId(result);
        idToEnum.put(newId, enumInfo);
        reverseMap.save(newId, enumInfo);
        copyEnumToOtherPools(enumInfo, result, pool);
        return result;
      } catch (ServerConnectivityException e) {
        // ignore, try the next pool.
        lastException = e;
      }
    }
    throw returnCorrectExceptionForFailure(-1, lastException);
  }

  /**
   * Send an enum to all pools. This used to make sure that any enums
   * used by this client make it to all clusters this client is connected to.
   */
  private void copyEnumToOtherPools(EnumInfo enumInfo, int newTypeId, Pool exception) {
    Collection<Pool> pools = getAllPoolsExcept(exception);
    for (Pool pool : pools) {
      try {
        sendEnumIdToPool(enumInfo, newTypeId, pool);
      } catch (ServerConnectivityException e) {
        logger.debug("Received an exception sending pdx enum to pool {}, {}", pool, e.getMessage());
      }
    }
  }

  private void sendEnumIdToPool(EnumInfo enumInfo, int id, Pool pool) {
    try {
      AddPDXEnumOp.execute((ExecutablePool) pool, id, enumInfo);
    } catch (ServerConnectivityException serverConnectivityException) {
      logger.debug("Received an exception sending pdx type to pool {}, {}", pool,
          serverConnectivityException.getMessage(), serverConnectivityException);
      throw serverConnectivityException;
    }
  }

  @Override
  public void addRemoteEnum(int enumId, EnumInfo newInfo) {
    throw new UnsupportedOperationException("Clients will not be asked to add remote enums");
  }

  @Override
  public int defineEnum(EnumInfo newInfo) {
    return processEnumInfoForEnumId(newInfo);
  }

  @Override
  public EnumInfo getEnumById(int enumId) {
    EnumId id = new EnumId(enumId);
    EnumInfo existingEnum = idToEnum.get(id);
    if (existingEnum != null) {
      return existingEnum;
    }
    Collection<Pool> pools = getAllPools();

    ServerConnectivityException lastException = null;
    for (Pool pool : pools) {
      try {
        EnumInfo result = GetPDXEnumByIdOp.execute((ExecutablePool) pool, enumId);
        if (result != null) {
          idToEnum.put(id, result);
          reverseMap.save(id, result);

          return result;
        }
      } catch (ServerConnectivityException e) {
        logger.debug("Received an exception getting pdx type from pool {}, {}", pool,
            e.getMessage(), e);
        // ignore, try the next pool.
        lastException = e;
      }
    }

    throw returnCorrectExceptionForFailure(enumId, lastException);
  }

  @Override
  public Map<Integer, PdxType> types() {
    Collection<Pool> pools = getAllPools();

    Map<Integer, PdxType> types = new HashMap<>();
    for (Pool p : pools) {
      try {
        types.putAll(GetPDXTypesOp.execute((ExecutablePool) p));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return types;
  }

  @Override
  public Map<Integer, EnumInfo> enums() {
    Collection<Pool> pools = getAllPools();

    Map<Integer, EnumInfo> enums = new HashMap<>();
    for (Pool p : pools) {
      enums.putAll(GetPDXEnumsOp.execute((ExecutablePool) p));
    }
    return enums;
  }


  @Override
  public PdxType getPdxTypeForField(String fieldName, String className) {
    for (Object value : types().values()) {
      if (value instanceof PdxType) {
        PdxType pdxType = (PdxType) value;
        if (pdxType.getClassName().equals(className) && pdxType.getPdxField(fieldName) != null) {
          return pdxType;
        }
      }
    }
    return null;
  }

  @Override
  public Set<PdxType> getPdxTypesForClassName(String className) {
    Set<PdxType> result = new HashSet<>();
    for (Object value : types().values()) {
      if (value instanceof PdxType) {
        PdxType pdxType = (PdxType) value;
        if (pdxType.getClassName().equals(className)) {
          result.add(pdxType);
        }
      }
    }
    return result;
  }

  @Override
  public boolean isClient() {
    return true;
  }

  /**
   * Add an type as part of an import. The type is sent to all pools in case
   * the pools are connected to different clusters, but if one pool fails
   * the import will fail.
   */
  @Override
  public void addImportedType(int typeId, PdxType importedType) {
    Collection<Pool> pools = getAllPools();
    for (Pool pool : pools) {
      try {
        sendTypeToPool(importedType, typeId, pool);
      } catch (ServerConnectivityException e) {
        throw returnCorrectExceptionForFailure(typeId, e);
      }
    }
    idToType.put(typeId, importedType);
    reverseMap.save(typeId, importedType);
  }

  /**
   * Add an enum as part of an import. The enum is sent to all pools in case
   * the pools are connected to different clusters, but if one pool fails
   * the import will fail.
   */
  @Override
  public void addImportedEnum(int enumId, EnumInfo importedInfo) {
    Collection<Pool> pools = getAllPools();

    for (Pool pool : pools) {
      try {
        sendEnumIdToPool(importedInfo, enumId, pool);
      } catch (ServerConnectivityException e) {
        throw returnCorrectExceptionForFailure(enumId, e);
      }
    }
    EnumId id = new EnumId(enumId);
    idToEnum.put(id, importedInfo);
    reverseMap.save(id, importedInfo);
  }

  private RuntimeException returnCorrectExceptionForFailure(final int typeId,
      final ServerConnectivityException lastException) {
    if (lastException != null) {
      throw lastException;
    } else {
      throw new InternalGemFireError("Unable to determine PDXType for id " + typeId);
    }
  }

  @Override
  public int getLocalSize() {
    return idToType.size() + idToEnum.size();
  }

  @Override
  public Map<PdxType, Integer> getTypeToIdMap() {
    return reverseMap.typeToId;
  }

  @Override
  public Map<EnumInfo, EnumId> getEnumToIdMap() {
    return reverseMap.enumToId;
  }

  @Override
  public void clearLocalMaps() {
    idToType.clear();
    idToEnum.clear();
    reverseMap.clear();
  }

  @Override
  public void flushCache() {
    idToEnum.values().forEach(EnumInfo::flushCache);
    reverseMap.enumToId.keySet().forEach(EnumInfo::flushCache);
  }
}
