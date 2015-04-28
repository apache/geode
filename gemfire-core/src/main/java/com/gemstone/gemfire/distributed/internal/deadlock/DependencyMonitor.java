/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.io.Serializable;
import java.util.Set;

/**
 * This interface defines the contract between the {@link DependencyMonitorManager} class
 * and classes that monitor a particular type of dependency. Classes implementing
 * this interface should register themselves with the dependency monitor using
 * the {@link DependencyMonitorManager#addMonitor(DependencyMonitor)} method.
 * 
 * @author dsmith
 */
public interface DependencyMonitor {
  /**
   * Return a map of resource identifiers to the threads that are blocked
   * waiting for those resources.
   */
  public Set<Dependency<Thread, Serializable>> getBlockedThreads(
      Thread[] allThreads);

  /**
   * Return a map of resource indentifiers to the threads that hold that
   * particular resource.
   */
  public Set<Dependency<Serializable, Thread>> getHeldResources(
      Thread[] allThreads);

}
