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

package org.apache.geode.cache.client;
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.distributed.DistributedSystem; // for javadocs

/**
 * Manages creation and access to {@link Pool connection pools} for clients.
 * <p>
 * To create a pool get a factory by calling {@link #createFactory}.
 * <p>
 * To find an existing pool by name call {@link #find(String)}.
 * <p>
 * To get rid of all created pool call {@link #close()}.
 *
 * @since GemFire 5.7
 *
 */
public final class PoolManager {
  
  private PoolManager() {
    // no instances allowed!
  }
  
  /**
   * Creates a new {@link PoolFactory pool factory},
   * which is used to configure and create new {@link Pool}s.
   * @return the new pool factory
   */
  public static PoolFactory createFactory() {
    return PoolManagerImpl.getPMI().createFactory();
  }

  /**
   * Find by name an existing connection pool returning
   * the existing pool or <code>null</code> if it does not exist.
   * @param name the name of the connection pool
   * @return the existing connection pool or <code>null</code> if it does not exist.
   */
  public static Pool find(String name) {
    return PoolManagerImpl.getPMI().find(name);
  }
  /**
   * Returns a map containing all the pools in this manager.
   * The keys are pool names
   * and the values are {@link Pool} instances.
   * <p> The map contains the pools that this manager knows of at the time of this call.
   * The map is free to be changed without affecting this manager.
   * @return a Map that is a snapshot of all the pools currently known to this manager.
   */
  public static Map<String,Pool> getAll() { 
    return PoolManagerImpl.getPMI().getMap();
  }
  /**
   * Unconditionally destroys all created pools that are in this manager.
   * @param keepAlive whether the server should keep the durable client's subscriptions alive for the <code>durable-client-timeout</code>.
   * @see DistributedSystem#connect for a description of <code>durable-client-timeout</code>.
   */
  public static void close(boolean keepAlive) {
    PoolManagerImpl.getPMI().close(keepAlive);
  }
  
  /**
   * Find the pool used by the given region.
   * @param region The region that is using the pool.
   * @return the pool used by that region or <code>null</code> if the region does
   * not have a pool. 
   */
  public static Pool find(Region<?,?> region) {
    return PoolManagerImpl.getPMI().find(region);
  }
  
  /**
   * Unconditionally destroys all created pools that are in this manager.
   */
  public static void close() {
    close(false);
  }
}
