/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

public interface GemFireXDAggregateTableMBean {
  public static final String OBJECT_NAME = "GemFireXD:service=Table,type=Aggregate,table=";
  
  public long getEntrySize();

  public int getNumberOfRows();

}
