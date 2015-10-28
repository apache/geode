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
  private String member = null;

  public RegionOnMember(String fullPath, String member) {
    this.fullPath = fullPath;
    this.member = member;
  }

  @Override
  protected String getKey(String propName) {
    return "regionOnMember." + fullPath + "." + member + "." + propName;
  }

  @Override
  public String getFullPath(){
    return this.fullPath;
  }

  @Override
  public String getMember(){
    return this.member;
  }

  @Override
  public String getName(){
    return getString("name");
  }

  @Override
  public String getRegionType(){
    return getString("regionType");
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
  public float getDiskReadsRate(){
    return getFloat("diskGetsRate");
  }

  @Override
  public float getDiskWritesRate(){
    return getFloat("diskPutsRate");
  }

  @Override
  public int getLocalMaxMemory(){
    return getInt("localMaxMemory");
  }
}
