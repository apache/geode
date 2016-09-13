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

import org.apache.logging.log4j.Logger;

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
import org.apache.geode.pdx.PdxInitializationException;

/**
 *
 */
public class ClientTypeRegistration implements TypeRegistration {
  private static final Logger logger = LogService.getLogger();
  
  private final GemFireCacheImpl cache;
  
  private volatile boolean typeRegistryInUse = false;

  public ClientTypeRegistration(GemFireCacheImpl cache) {
    this.cache = cache;
  }
  
  public int defineType(PdxType newType) {
    verifyConfiguration(); 
    Collection<Pool> pools = getAllPools();
    
    ServerConnectivityException lastException = null;
    for(Pool pool: pools) {
      try {
        int result = GetPDXIdForTypeOp.execute((ExecutablePool) pool, newType);
        newType.setTypeId(result);
        sendTypeToAllPools(newType, result, pools, pool);
        return result;
      } catch(ServerConnectivityException e) {
        //ignore, try the next pool.
        lastException = e;
      }
    }
    if(lastException != null) {
      throw lastException;
    } else {
      if (this.cache.isClosed()) {
        throw new CacheClosedException("PDX detected cache was closed");
      }
      throw new CacheClosedException("Client pools have been closed so the PDX type registry can not define a type.");
    }
  }
  
  private void sendTypeToAllPools(PdxType type, int id,
      Collection<Pool> pools, Pool definingPool) {
    
    for(Pool pool: pools) {
      if(pool.equals(definingPool)) {
        continue;
      }
      
      try {
        AddPDXTypeOp.execute((ExecutablePool) pool, id, type);
      } catch(ServerConnectivityException ignore) {
        logger.debug("Received an exception sending pdx type to pool {}, {}", pool, ignore.getMessage(), ignore);
        //TODO DAN - is it really safe to ignore this? What if this is the pool
        //we're about to do a put on? I think maybe we really should pass the context
        //down to this point, if it is available. Maybe just an optional thread local?
        //Then we could go straight to that pool to register the type and bail otherwise.
      }
    }
    
  }

  public PdxType getType(int typeId) {
    verifyConfiguration();
    Collection<Pool> pools = getAllPools();
    
    ServerConnectivityException lastException = null;
    for(Pool pool: pools) {
      try {
        PdxType type = GetPDXTypeByIdOp.execute((ExecutablePool) pool, typeId);
        if(type != null) {
          return type;
        }
      } catch(ServerConnectivityException e) {
        logger.debug("Received an exception getting pdx type from pool {}, {}", pool, e.getMessage(), e);
        //ignore, try the next pool.
        lastException = e;
      }
    }
    
    if(lastException != null) {
      throw lastException;
    } else {
      if(pools.isEmpty()) {
        if (this.cache.isClosed()) {
          throw this.cache.getCacheClosedException("PDX detected cache was closed", null);
        }
        throw new CacheClosedException("Client pools have been closed so the PDX type registry can not lookup a type.");
      } else {
        throw new InternalGemFireError("getType: Unable to determine PDXType for id " + typeId + " from existing client to server pools " + pools);
      }
    }
  }
  
  private Collection<Pool> getAllPools() {
    return getAllPools(cache);
  }
  
  private static Collection<Pool> getAllPools(GemFireCacheImpl cache) {
    Collection<Pool> pools = PoolManagerImpl.getPMI().getMap().values();
    for(Iterator<Pool> itr= pools.iterator(); itr.hasNext(); ) {
      PoolImpl pool = (PoolImpl) itr.next();
      if(pool.isUsedByGateway()) {
        itr.remove();
      }
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
    checkAllowed();
  }
  
  public void creatingPersistentRegion() {
    //do nothing
  }

  public void creatingPool() {
    //do nothing
  }

  public int getEnumId(Enum<?> v) {
    EnumInfo ei = new EnumInfo(v);
    Collection<Pool> pools = getAllPools();

    ServerConnectivityException lastException = null;
    for(Pool pool: pools) {
      try {
        int result = GetPDXIdForEnumOp.execute((ExecutablePool) pool, ei);
        sendEnumIdToAllPools(ei, result, pools, pool);
        return result;
      } catch(ServerConnectivityException e) {
        //ignore, try the next pool.
        lastException = e;
      }
    }
    if (lastException != null) {
      throw lastException;
    } else {
      if (this.cache.isClosed()) {
        throw new CacheClosedException("PDX detected cache was closed");
      }
      throw new CacheClosedException("Client pools have been closed so the PDX type registry can not define a type.");
    }
  }
  
  private void sendEnumIdToAllPools(EnumInfo enumInfo, int id,
      Collection<Pool> pools, Pool definingPool) {

    for (Pool pool: pools) {
      if (pool.equals(definingPool)) {
        continue;
      }

      try {
        AddPDXEnumOp.execute((ExecutablePool) pool, id, enumInfo);
      } catch(ServerConnectivityException ignore) {
        logger.debug("Received an exception sending pdx type to pool {}, {}", pool, ignore.getMessage(), ignore);
        //TODO DAN - is it really safe to ignore this? What if this is the pool
        //we're about to do a put on? I think maybe we really should pass the context
        //down to this point, if it is available. Maybe just an optional thread local?
        //Then we could go straight to that pool to register the type and bail otherwise.
      }
    }
  }

  public void addRemoteEnum(int enumId, EnumInfo newInfo) {
    throw new UnsupportedOperationException("Clients will not be asked to add remote enums");
  }

  public int defineEnum(EnumInfo newInfo) {
    Collection<Pool> pools = getAllPools();
    
    ServerConnectivityException lastException = null;
    for(Pool pool: pools) {
      try {
        int result = GetPDXIdForEnumOp.execute((ExecutablePool) pool, newInfo);
        sendEnumIdToAllPools(newInfo, result, pools, pool);
        return result;
      } catch(ServerConnectivityException e) {
        //ignore, try the next pool.
        lastException = e;
      }
    }
    
    
    if(lastException != null) {
      throw lastException;
    } else {
      if (this.cache.isClosed()) {
        throw new CacheClosedException("PDX detected cache was closed");
      }
      throw new CacheClosedException("Client pools have been closed so the PDX type registry can not define a type.");
    }
   }

  public EnumInfo getEnumById(int enumId) {
    Collection<Pool> pools = getAllPools();
    
    ServerConnectivityException lastException = null;
    for(Pool pool: pools) {
      try {
        EnumInfo result = GetPDXEnumByIdOp.execute((ExecutablePool) pool, enumId);
        if(result != null) {
          return result;
        }
      } catch(ServerConnectivityException e) {
        logger.debug("Received an exception getting pdx type from pool {}, {}", pool, e.getMessage(), e);
        //ignore, try the next pool.
        lastException = e;
      }
    }
    
    if(lastException != null) {
      throw lastException;
    } else {
      if(pools.isEmpty()) {
        if (this.cache.isClosed()) {
          throw this.cache.getCacheClosedException("PDX detected cache was closed", null);
        }
        throw new CacheClosedException("Client pools have been closed so the PDX type registry can not lookup an enum.");
      } else {
        throw new InternalGemFireError("getEnum: Unable to determine pdx enum for id " + enumId + " from existing client to server pools " + pools);
      }
    }
  }
  
  private void verifyConfiguration() {
    if(typeRegistryInUse) {
      return;
    } else {
      typeRegistryInUse = true;
      checkAllowed();
    }
  }
  
  private void checkAllowed() {
    //Anything is allowed until the registry is in use.
    if(!typeRegistryInUse) {
      return;
    }
  }

  @SuppressWarnings({ "unchecked", "serial" })
  @Override
  public Map<Integer, PdxType> types() {
    Collection<Pool> pools = getAllPools();
    if (pools.isEmpty()) {
      if (this.cache.isClosed()) {
        throw new CacheClosedException("PDX detected cache was closed");
      }
      throw new CacheClosedException("Client pools have been closed so the PDX type registry is not available.");
    }
    
    Map<Integer, PdxType> types = new HashMap<Integer, PdxType>();
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
    if (pools.isEmpty()) {
      if (this.cache.isClosed()) {
        throw new CacheClosedException("PDX detected cache was closed");
      }
      throw new CacheClosedException("Client pools have been closed so the PDX type registry is not available.");
    }
    
    Map<Integer, EnumInfo> enums = new HashMap<Integer, EnumInfo>();
    for (Pool p : pools) {
      enums.putAll(GetPDXEnumsOp.execute((ExecutablePool) p));
    }
    return enums;
  }
  

  @Override
  public PdxType getPdxTypeForField(String fieldName, String className) {
    for (Object value : types().values()) {
      if (value instanceof PdxType){
        PdxType pdxType = (PdxType) value;
        if(pdxType.getClassName().equals(className) && pdxType.getPdxField(fieldName) != null){
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
    for(Pool pool: pools) {
      try {
        sendTypeToAllPools(importedType, typeId, pools, pool);
      } catch(ServerConnectivityException e) {
        //ignore, try the next pool.
        lastException = e;
      }
    }
    if(lastException != null) {
      throw lastException;
    } else {
      if (this.cache.isClosed()) {
        throw new CacheClosedException("PDX detected cache was closed");
      }
      throw new CacheClosedException("Client pools have been closed so the PDX type registry can not define a type.");
    }
  }

  @Override
  public void addImportedEnum(int enumId, EnumInfo importedInfo) {
    Collection<Pool> pools = getAllPools();
    
    ServerConnectivityException lastException = null;
    for(Pool pool: pools) {
      try {
        sendEnumIdToAllPools(importedInfo, enumId, pools, pool);
      } catch(ServerConnectivityException e) {
        //ignore, try the next pool.
        lastException = e;
      }
    }
    
    if(lastException != null) {
      throw lastException;
    } else {
      if (this.cache.isClosed()) {
        throw new CacheClosedException("PDX detected cache was closed");
      }
      throw new CacheClosedException("Client pools have been closed so the PDX type registry can not define a type.");
    }
  }
  
  @Override
  public int getLocalSize() {
    return 0;
  }
}
