/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author VMware, Inc.
 * @since 7.0
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
