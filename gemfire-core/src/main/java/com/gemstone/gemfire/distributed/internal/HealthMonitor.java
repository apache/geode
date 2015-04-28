/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.admin.GemFireHealth;

/**
 * Represents a thread that monitor the health of the vm it lives in.
 * @author Darrel Schneider
 * @since 3.5
 */
public interface HealthMonitor {
  /**
   * Returns the id of this monitor instance. Each instance is given
   * a unique id when it is created.
   */
  public int getId();
  /**
   * Resets the current health status to zero.
   */
  public void resetStatus();
  /**
   * Returns the diagnosis of the desired status.
   */
  public String[] getDiagnosis(GemFireHealth.Health healthCode);
  /**
   * Stops the monitor so it no longer checks for health.
   * Once stopped a monitor can not be started.
   */
  public void stop();
}
