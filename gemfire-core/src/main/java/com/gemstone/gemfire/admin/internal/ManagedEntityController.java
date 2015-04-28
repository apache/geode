/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.ManagedEntity;
import com.gemstone.gemfire.admin.ManagedEntityConfig;

/**
 * Defines the actual administration (starting, stopping, etc.) of
 * GemFire {@link ManagedEntity}s.
 * 
 * @author Kirk Lund
 */
interface ManagedEntityController {
  /**
   * Starts a managed entity.
   */
  public void start(final InternalManagedEntity entity);

  /**
   * Stops a managed entity.
   */
  public void stop(final InternalManagedEntity entity);

  /**
   * Returns whether or not a managed entity is running
   */
  public boolean isRunning(InternalManagedEntity entity);
  
  /**
   * Returns the contents of a locator's log file.  Other APIs are
   * used to get the log file of managed entities that are also system
   * members.
   */
  public String getLog(DistributionLocatorImpl locator);
  
  /**
   * Returns the full path to the executable in
   * <code>$GEMFIRE/bin</code> taking into account the {@linkplain
   * ManagedEntityConfig#getProductDirectory product directory} and the
   * platform's file separator.
   *
   * <P>
   *
   * Note: we should probably do a better job of determine whether or
   * not the machine on which the entity runs is Windows or Linux.
   *
   * @param executable
   *        The name of the executable that resides in
   *        <code>$GEMFIRE/bin</code>.
   */
  public String getProductExecutable(InternalManagedEntity entity, String executable);
  
  /**
   * Builds optional SSL command-line arguments.  Returns null if SSL is not
   * enabled for the distributed system.
   */
  public String buildSSLArguments(DistributedSystemConfig config);
}
