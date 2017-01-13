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
import org.apache.geode.*; // for DataSerializable
import org.apache.geode.cache.Declarable;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;

public class MultiGetFunction extends FunctionAdapter implements Declarable{

  public void execute(FunctionContext fc) {
    RegionFunctionContext context = (RegionFunctionContext)fc;
    System.out.println("Data set :: " + context.getDataSet());
    Region region = PartitionRegionHelper.getLocalDataForContext(context);
    Set keys = context.getFilter();
    System.out.println("Context " + keys);
    ArrayList vals = new ArrayList();
    Iterator itr = keys.iterator();
    while (itr.hasNext()) {
      Object k = itr.next();
      vals.add(region.get(k));
      System.out.println("vals " + vals);
    }
    ResultSender sender = context.getResultSender();
    //test sendResult
    sender.sendResult(vals);
    //test lastResult
    sender.lastResult(vals);
  }

  public String getId() {
    return "MultiGetFunction";
  }

  public void init(Properties arg0) {

  }
}
