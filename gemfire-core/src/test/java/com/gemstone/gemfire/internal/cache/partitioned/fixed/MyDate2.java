/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned.fixed;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionResolver;

public class MyDate2 extends Date implements PartitionResolver{

  public MyDate2(long time) {
    super(time);
  }
  
  public String getName() {
    return "MyDate2";
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    Date date = (Date)opDetails.getKey();
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int month = cal.get(Calendar.MONTH);
    return month;
  
  }

  public void close() {
    // TODO Auto-generated method stub
  }
  
  public String toString(){
    return "MyDate2";
  }
}
