/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

/**
 * Region on member mbean
 *
 *
 * @author rbhandekar
 *
 */
public class RegionOnMember extends JMXBaseBean implements RegionOnMemberMBean {
  private String fullPath = null;

  public RegionOnMember(String fullPath) {
    this.fullPath = fullPath;
  }

  @Override
  protected String getKey(String propName) {
    return "regionOnMember." + fullPath + "." + propName;
  }

  @Override
  public String getRegionFullPath(){
    return this.fullPath;
  }

  @Override
  public String getMemberName(){
    return getString("memberName");
  }

  @Override
  public long getEntrySize(){
    return getLong("entrySize");
  }

  @Override
  public long getEntryCount(){
    return getLong("entryCount");
  }

  @Override
  public float getGetsRate(){
    return getFloat("getsRate");
  }

  @Override
  public float getPutsRate(){
    return getFloat("putsRate");
  }

  @Override
  public float getDiskGetsRate(){
    return getFloat("diskReadsRate");
  }

  @Override
  public float getDiskPutsRate(){
    return getFloat("diskWritesRate");
  }

  @Override
  public int getLocalMaxMemory(){
    return getInt("localMaxMemory");
  }
}
