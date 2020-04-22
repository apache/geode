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

package org.apache.geode.internal.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ExecutablePool;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.RegisterDataSerializersOp;
import org.apache.geode.cache.client.internal.RegisterInstantiatorsOp;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalDataSerializer.SerializerAttributesHolder;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.InternalInstantiator.InstantiatorAttributesHolder;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Implementation used by PoolManager.
 *
 * @since GemFire 5.7
 */
public class PoolManagerImpl {
  private static final Logger logger = LogService.getLogger();

  @MakeNotStatic
  private static final PoolManagerImpl impl = new PoolManagerImpl(true);

  public static PoolManagerImpl getPMI() {
    PoolManagerImpl result = CacheCreation.getCurrentPoolManager();
    if (result == null) {
      result = impl;
    }
    return result;
  }

  private volatile Map<String, Pool> pools = Collections.emptyMap();
  private volatile Optional<Iterator<Pool>> itrForEmergencyClose = Optional.empty();
  private final Object poolLock = new Object();
  /**
   * True if this manager is a normal one owned by the PoolManager. False if this is a special one
   * owned by a xml CacheCreation.
   */
  private final boolean normalManager;

  /**
   * @param addListener will be true if the is a real manager that needs to register a connect
   *        listener. False if it is a fake manager used internally by the XML code.
   */
  public PoolManagerImpl(boolean addListener) {
    normalManager = addListener;
  }

  /**
   * Returns true if this is a normal manager; false if it is a fake one used for xml parsing.
   */
  public boolean isNormal() {
    return normalManager;
  }

  /**
   * Creates a new {@link PoolFactory pool factory}, which is used to configure and create new
   * {@link Pool}s.
   *
   * @return the new pool factory
   */
  public PoolFactory createFactory() {
    return new PoolFactoryImpl(this);
  }

  /**
   * Find by name an existing connection pool returning the existing pool or <code>null</code> if it
   * does not exist.
   *
   * @param name the name of the connection pool
   * @return the existing connection pool or <code>null</code> if it does not exist.
   */
  public Pool find(String name) {
    return pools.get(name);
  }

  /**
   * Destroys all created pool in this manager.
   */
  public void close(boolean keepAlive) {
    // destroying connection pools
    boolean foundClientPool = false;
    synchronized (poolLock) {
      for (Entry<String, Pool> entry : pools.entrySet()) {
        Pool pool = entry.getValue();
        if (pool instanceof PoolImpl) {
          ((PoolImpl) pool).basicDestroy(keepAlive);
          foundClientPool = true;
        }

      }
      pools = Collections.emptyMap();
      itrForEmergencyClose = Optional.empty();
      if (foundClientPool) {
        // Now that the client has all the pools destroyed free up the pooled comm buffers
        ServerConnection.emptyCommBufferPool();
      }
    }
  }

  /**
   * @return a copy of the Pools Map
   */
  public Map<String, Pool> getMap() {
    return new HashMap<>(pools);
  }

  /**
   * This is called by {@link PoolImpl#create}
   *
   * @throws IllegalStateException if a pool with same name is already registered.
   */
  public void register(Pool pool) {
    synchronized (poolLock) {
      Map<String, Pool> copy = new HashMap<>(pools);
      String name = pool.getName();
      // debugStack("register pool=" + name);
      Object old = copy.put(name, pool);
      if (old != null) {
        throw new IllegalStateException(
            String.format("A pool named %s already exists", name));
      }
      // Boolean specialCase=Boolean.getBoolean("gemfire.SPECIAL_DURABLE");
      // if(specialCase && copy.size()>1){
      // throw new IllegalStateException("Using SPECIAL_DURABLE system property"
      // + " and more than one pool already exists in client.");
      // }
      pools = Collections.unmodifiableMap(copy);
      itrForEmergencyClose = Optional.of(copy.values().iterator());
    }
  }

  /**
   * This is called by {@link Pool#destroy(boolean)}
   *
   * @return true if pool unregistered from cache; false if someone else already did it
   */
  public boolean unregister(Pool pool) {
    // Continue only if the pool is not currently being used by any region and/or service.
    int attachCount = 0;
    if (pool instanceof PoolImpl) {
      attachCount = ((PoolImpl) pool).getAttachCount();
    }
    if (attachCount > 0) {
      throw new IllegalStateException(String.format(
          "Pool could not be destroyed because it is still in use by %s regions", attachCount));
    }

    synchronized (poolLock) {
      Map<String, Pool> copy = new HashMap<>(pools);
      String name = pool.getName();
      // debugStack("unregister pool=" + name);
      Object rmPool = copy.remove(name);
      if (rmPool == null || rmPool != pool) {
        return false;
      } else {
        pools = Collections.unmodifiableMap(copy);
        itrForEmergencyClose = Optional.of(copy.values().iterator());
        return true;
      }
    }
  }

  @Override
  public String toString() {
    return super.toString() + "-" + (normalManager ? "normal" : "xml");
  }

  /**
   * @param xmlPoolsOnly if true then only call readyForEvents on pools declared in XML.
   */
  public static void readyForEvents(InternalDistributedSystem system, boolean xmlPoolsOnly) {
    boolean foundDurablePool = false;
    Map<String, Pool> pools = PoolManager.getAll();
    for (Pool pool : pools.values()) {
      if (pool instanceof PoolImpl) {
        PoolImpl p = (PoolImpl) pool;
        if (p.isDurableClient()) {
          // TODO - handle an exception and attempt on all pools?
          foundDurablePool = true;
          if (!xmlPoolsOnly) {
            p.readyForEvents(system);
          }
        }
      }
    }
    if (pools.size() > 0 && !foundDurablePool) {
      throw new IllegalStateException(
          "Only durable clients should call readyForEvents()");
    }
  }

  public static void allPoolsRegisterInstantiator(Instantiator instantiator) {
    Instantiator[] instantiators = new Instantiator[] {instantiator};
    for (Pool pool : PoolManager.getAll().values()) {
      try {
        EventID eventId = InternalInstantiator.generateEventId();
        if (eventId != null) {
          RegisterInstantiatorsOp.execute((ExecutablePool) pool, instantiators, eventId);
        }
      } catch (RuntimeException e) {
        logger.warn("Error registering instantiator on pool:", e);
      }
    }
  }

  public static void allPoolsRegisterInstantiator(InstantiatorAttributesHolder holder) {
    InstantiatorAttributesHolder[] holders = new InstantiatorAttributesHolder[] {holder};
    for (Pool pool : PoolManager.getAll().values()) {
      try {
        EventID eventId = InternalInstantiator.generateEventId();
        if (eventId != null) {
          RegisterInstantiatorsOp.execute((ExecutablePool) pool, holders, eventId);
        }
      } catch (RuntimeException e) {
        logger.warn("Error registering instantiator on pool:", e);
      }
    }
  }

  public static void allPoolsRegisterDataSerializers(DataSerializer dataSerializer) {
    DataSerializer[] dataSerializers = new DataSerializer[] {dataSerializer};
    for (Pool pool : PoolManager.getAll().values()) {
      try {
        EventID eventId = (EventID) dataSerializer.getEventId();
        if (eventId == null) {
          eventId = InternalDataSerializer.generateEventId();
        }
        if (eventId != null) {
          RegisterDataSerializersOp.execute((ExecutablePool) pool, dataSerializers, eventId);
        }
      } catch (RuntimeException e) {
        logger.warn("Error registering instantiator on pool:", e);
      }
    }
  }

  public static void allPoolsRegisterDataSerializers(SerializerAttributesHolder holder) {
    SerializerAttributesHolder[] holders = new SerializerAttributesHolder[] {holder};
    for (Pool pool : PoolManager.getAll().values()) {
      try {
        EventID eventId = holder.getEventId();
        if (eventId == null) {
          eventId = InternalDataSerializer.generateEventId();
        }
        if (eventId != null) {
          RegisterDataSerializersOp.execute((ExecutablePool) pool, holders, eventId);
        }
      } catch (RuntimeException e) {
        logger.warn("Error registering instantiator on pool:", e);
      }
    }
  }

  public static void emergencyClose() {
    impl.itrForEmergencyClose.ifPresent(poolIterator -> {
      while (poolIterator.hasNext()) {
        Pool pool = poolIterator.next();
        if (pool instanceof PoolImpl) {
          ((PoolImpl) pool).emergencyClose();
        }
      }
    });
  }

  public Pool find(Region<?, ?> region) {
    return find(region.getAttributes().getPoolName());
  }
}
