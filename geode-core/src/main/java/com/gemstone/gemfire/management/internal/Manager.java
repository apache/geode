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
package com.gemstone.gemfire.management.internal;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;

/**
 * The Manager is a 7.0 JMX Agent which is hosted within a GemFire process.
 * Only one instance is allowed per DistributedSystem connection (or loner).
 * It's responsible for defining JMX server endpoints for servicing JMX 
 * clients.
 *
 * @since GemFire 7.0
 */
public abstract class Manager {
  
  
  /**
   * GemFire Cache implementation
   */

  protected GemFireCacheImpl cache;


  /**
   * depicts whether this node is a Managing node or not
   */
  protected volatile boolean running = false;
  
  
  /**
   * depicts whether this node is a Managing node or not
   */
  protected volatile boolean stopCacheOps = false;



  /**
   * This is a single window to manipulate region resources for management
   */
  protected ManagementResourceRepo repo;
  

  /**
   * The concrete implementation of DistributedSystem that provides
   * internal-only functionality.
   */

  protected InternalDistributedSystem system;
  

  
  public Manager(ManagementResourceRepo repo , InternalDistributedSystem system, Cache cache){
    this.repo = repo;
    this.cache = (GemFireCacheImpl)cache;
    this.system = system;
  }
  public abstract boolean isRunning();
  public abstract void startManager();
  public abstract void stopManager() ;
  
  /**
   * For internal use only
   * 
   */

  public ManagementResourceRepo getManagementResourceRepo() {
    return repo;
  }


}
