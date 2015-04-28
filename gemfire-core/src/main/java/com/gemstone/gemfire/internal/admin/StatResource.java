/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin;

//import java.util.List;

/**
 * Interface to represent one statistic resource
 *
 * @author Darrel Schneider
 * @author Kirk Lund
 */
public interface StatResource extends GfObject {
  
  public long getResourceID();
  public long getResourceUniqueID();
  public String getSystemName();
  public GemFireVM getGemFireVM();
  public Stat[] getStats();
  public Stat getStatByName(String name);
  public String getName();
  
  /**
   * @return the full description of this statistic resource
   */
  public String getDescription();
}
