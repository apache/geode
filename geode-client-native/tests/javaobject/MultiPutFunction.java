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
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

public class MultiPutFunction extends FunctionAdapter implements Declarable{

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
      region.put(k, k);
      vals.add(region.get(k));
      System.out.println("vals " + vals);
    }
  }

  public String getId() {
    return "MultiPutFunction";
  }

  public void init(Properties arg0) {

  }
  
  public boolean hasResult() {
	return false;
  }
  
  public boolean isHA() {
    return false;
  }
}
