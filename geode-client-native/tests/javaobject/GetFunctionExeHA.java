/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;


import java.util.*;
import java.io.*;
import com.gemstone.gemfire.*; // for DataSerializable
import com.gemstone.gemfire.cache.Declarable;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

public class GetFunctionExeHA extends FunctionAdapter implements Declarable{

  public void execute(FunctionContext fc) {
    RegionFunctionContext context = (RegionFunctionContext)fc;
    System.out.println("Data set :: " + context.getDataSet());
    Region region = PartitionRegionHelper.getLocalDataForContext(context);
    Set keys = region.keys();
    Iterator itr = keys.iterator();
    ResultSender sender = context.getResultSender();
    Object k = null;
    while (itr.hasNext()) {
      k = itr.next();
      if(itr.hasNext())
        sender.sendResult((String)k);
      try {
        Thread.sleep(10);
      } catch (Exception e){}
    }
    sender.lastResult((String)k);
  }

  public String getId() {
    return "GetFunctionExeHA";
  }

  public void init(Properties arg0) {
  }
  public boolean isHA() {
    return true;
  }

}
