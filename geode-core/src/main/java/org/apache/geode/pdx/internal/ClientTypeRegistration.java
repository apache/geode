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
package org.apache.geode.pdx.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.CacheClosedException;
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
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

public class ClientTypeRegistration implements TypeRegistration {

  private static final Logger logger = LogService.getLogger();

  private final GemFireCacheImpl cache;

  public ClientTypeRegistration(GemFireCacheImpl cache) {
    this.cache = cache;
  }

  public int defineType(PdxType newType) {
    Collection<Pool> pools = getAllPools();

    ServerConnectivityException lastException = null;
    int newTypeId = -1;
    for (Pool pool : pools) {
      try {
        newTypeId = GetPDXIdForTypeOp.execute((ExecutablePool) pool, newType);
        newType.setTypeId(newTypeId);
        sendTypeToPool(newType, newTypeId, pool);
        return newTypeId;
      } catch (ServerConnectivityException e) {
        //ignore, try the next pool.
        lastException = e;
      }
    }
    throw returnCorrectExceptionForFailure(pools, newTypeId, lastException);
  }

  private void sendTypeToPool(PdxType type, int id, Pool pool) {
    try {
      AddPDXTypeOp.execute((ExecutablePool) pool, id, type);
    } catch (ServerConnectivityException serverConnectivityException) {
      logger.debug("Received an exception sending pdx type to pool {}, {}", pool, serverConnectivityException.getMessage(), serverConnectivityException);
      throw serverConnectivityException;
    }
  }

  public PdxType getType(int typeId) {
    Collection<Pool> pools = getAllPools();

    ServerConnectivityException lastException = null;
    for (Pool pool : pools) {
      try {
        PdxType type = GetPDXTypeByIdOp.execute((ExecutablePool) pool, typeId);
        if (type != null) {
          return type;
        }
      } catch (ServerConnectivityException e) {
        logger.debug("Received an exception getting pdx type from pool {}, {}", pool, e.getMessage(), e);
        //ignore, try the next pool.
        lastException = e;
      }
    }

    if (lastException != null) {
      throw lastException;
    } else {
      throw returnCorrectExceptionForFailure(pools, typeId, lastException);
    }
  }

  private Collection<Pool> getAllPools() {
    Collection<Pool> pools = PoolManagerImpl.getPMI().getMap().values();

    for (Iterator<Pool> itr = pools.iterator(); itr.hasNext(); ) {
      PoolImpl pool = (PoolImpl) itr.next();
      if (pool.isUsedByGateway()) {
        itr.remove();
      }
    }

    if (pools.isEmpty()) {
      if (this.cache.isClosed()) {
        throw new CacheClosedException("PDX detected cache was closed");
      }
      throw new CacheClosedException("Client pools have been closed so the PDX type registry is not available.");
    }
    return pools;
  }

  public void addRemoteType(int typeId, PdxType type) {
    throw new UnsupportedOperationException("Clients will not be asked to add remote types");
  }

  public int getLastAllocatedTypeId() {
    throw new UnsupportedOperationException("Clients does not keep track of last allocated id");
  }

  public void initialize() {
    //do nothing
  }

  public void gatewaySenderStarted(GatewaySender gatewaySender) {
    //do nothing
  }

  public void creatingPersistentRegion() {
    //do nothing
  }

  public void creatingPool() {
    //do nothing
  }

  public int getEnumId(Enum<?> v) {
    EnumInfo enumInfo = new EnumInfo(v);
    return processEnumInfoForEnumId(enumInfo);
  }

  private int processEnumInfoForEnumId(EnumInfo enumInfo) {
    Collection<Pool> pools = getAllPools();
    ServerConnectivityException lastException = null;
    for (Pool pool : pools) {
      try {
        int result = GetPDXIdForEnumOp.execute((ExecutablePool) pool, enumInfo);
        sendEnumIdToPool(enumInfo, result, pool);
        return result;
      } catch (ServerConnectivityException e) {
        //ignore, try the next pool.
        lastException = e;
      }
    }
    throw returnCorrectExceptionForFailure(pools, -1, lastException);
  }

  private void sendEnumIdToPool(EnumInfo enumInfo, int id, Pool pool) {
    try {
      AddPDXEnumOp.execute((ExecutablePool) pool, id, enumInfo);
    } catch (ServerConnectivityException serverConnectivityException) {
      logger.debug("Received an exception sending pdx type to pool {}, {}", pool, serverConnectivityException.getMessage(), serverConnectivityException);
      throw serverConnectivityException;
    }
  }

  public void addRemoteEnum(int enumId, EnumInfo newInfo) {
    throw new UnsupportedOperationException("Clients will not be asked to add remote enums");
  }

  public int defineEnum(EnumInfo newInfo) {
    return processEnumInfoForEnumId(newInfo);
  }

  public EnumInfo getEnumById(int enumId) {
    Collection<Pool> pools = getAllPools();

    ServerConnectivityException lastException = null;
    for (Pool pool : pools) {
      try {
        EnumInfo result = GetPDXEnumByIdOp.execute((ExecutablePool) pool, enumId);
        if (result != null) {
          return result;
        }
      } catch (ServerConnectivityException e) {
        logger.debug("Received an exception getting pdx type from pool {}, {}", pool, e.getMessage(), e);
        //ignore, try the next pool.
        lastException = e;
      }
    }

    throw returnCorrectExceptionForFailure(pools, enumId, lastException);
  }

  @SuppressWarnings({ "unchecked", "serial" })
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

  @SuppressWarnings({ "unchecked", "serial" })
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
  public void testClearRegistry() {
  }

  @Override
  public boolean isClient() {
    return true;
  }

  @Override
  public void addImportedType(int typeId, PdxType importedType) {
    Collection<Pool> pools = getAllPools();

    ServerConnectivityException lastException = null;
    for (Pool pool : pools) {
      try {
        sendTypeToPool(importedType, typeId, pool);
        return;
      } catch (ServerConnectivityException e) {
        //ignore, try the next pool.
        lastException = e;
      }
    }
    throw returnCorrectExceptionForFailure(pools, typeId, lastException);
  }

  @Override
  public void addImportedEnum(int enumId, EnumInfo importedInfo) {
    Collection<Pool> pools = getAllPools();

    ServerConnectivityException lastException = null;
    for (Pool pool : pools) {
      try {
        sendEnumIdToPool(importedInfo, enumId, pool);
      } catch (ServerConnectivityException e) {
        //ignore, try the next pool.
        lastException = e;
      }
    }

    throw returnCorrectExceptionForFailure(pools, enumId, lastException);
  }

  private RuntimeException returnCorrectExceptionForFailure(final Collection<Pool> pools, final int typeId, final ServerConnectivityException lastException) {
    if (lastException != null) {
      throw lastException;
    } else {
      throw new InternalGemFireError("Unable to determine PDXType for id " + typeId);
    }
  }

  @Override
  public int getLocalSize() {
    return 0;
  }
}
