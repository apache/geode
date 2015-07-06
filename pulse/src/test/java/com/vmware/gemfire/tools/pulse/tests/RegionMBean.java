/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

import javax.management.openmbean.CompositeData;

public interface RegionMBean {
  public static final String OBJECT_NAME = "GemFire:service=Region,type=Distributed";

  public String[] getMembers();

  public String getFullPath();

  public float getDiskReadsRate();

  public float getDiskWritesRate();

  public int getEmptyNodes();

  public float getGetsRate();

  public float getLruEvictionRate();

  public float getPutsRate();

  public String getRegionType();

  public long getEntrySize();

  public long getSystemRegionEntryCount();

  public int getMemberCount();

  public boolean getPersistentEnabled();

  public String getName();

  public boolean getGatewayEnabled();

  public long getDiskUsage();

  public CompositeData listRegionAttributes();
}
