/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
package com.gemstone.gemfire.internal.admin;

import com.gemstone.gemfire.admin.GemFireHealth;

/**
 * Interface for those who want to be alerted of a change in a GemFireVM's
 * health.
 *
 * @see com.gemstone.gemfire.admin.GemFireHealthConfig
 *
 * @author Darrel Schneider
 * @since 3.5
 */
public interface HealthListener {

  /**
   * Called by a GemFireVM whenever a health status change is detected.
   *
   * @param member
   *        The member whose health has changed
   * @param status
   *        the new health status
   */
  public void healthChanged(GemFireVM member,
                            GemFireHealth.Health status);

}
