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
  String OBJECT_NAME = "GemFire:service=Region,type=Distributed";

  String[] getMembers();

  String getFullPath();

  float getDiskReadsRate();

  float getDiskWritesRate();

  int getEmptyNodes();

  float getGetsRate();

  float getLruEvictionRate();

  float getPutsRate();

  String getRegionType();

  long getEntrySize();

  long getSystemRegionEntryCount();

  int getMemberCount();

  boolean getPersistentEnabled();

  String getName();

  boolean getGatewayEnabled();

  long getDiskUsage();

  CompositeData listRegionAttributes();
}
