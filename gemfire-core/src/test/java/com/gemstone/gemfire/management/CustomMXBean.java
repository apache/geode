/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;
/**
 * Interface for A simple MBean to test various aspects of federation
 * @author rishim
 *
 */
public interface CustomMXBean {
  
  public long getSystemTime();

  public void setSystemTime(long systemTime) ;
  
  public String getName();

  public void setName(String staticField) ;
  
  public String fetchName();
  public void  writeName(String staticField);

}
