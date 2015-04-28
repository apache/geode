/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
package com.gemstone.gemfire.internal.admin;

import com.gemstone.gemfire.internal.admin.remote.*;

/**
 * A factory for GfManagerAgent instances.  This is the main entry
 * point for the admin API.
 *
 * @author    Pete Matern
 * @author    Darrel Schneider
 * @author    Kirk Lund
 *
 */
public class GfManagerAgentFactory {
 
  /**
   * Creates a GfManagerAgent for managing a distributed system.
   *
   * @param config  definition of the distributed system to manage
   */
  public static GfManagerAgent getManagerAgent(GfManagerAgentConfig config) {
    return new RemoteGfManagerAgent(config);
  }

}
