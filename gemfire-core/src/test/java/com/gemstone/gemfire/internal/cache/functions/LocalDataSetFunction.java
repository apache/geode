/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.functions;

import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class LocalDataSetFunction extends FunctionAdapter {

  private volatile boolean optimizeForWrite;

  public LocalDataSetFunction(boolean optimizeForWrite) {
    this.optimizeForWrite = optimizeForWrite;
  }

  public void execute(FunctionContext context) {
    RegionFunctionContext rContext = (RegionFunctionContext)context;
    Region cust = rContext.getDataSet();
    LocalDataSet localCust = (LocalDataSet)PartitionRegionHelper.getLocalDataForContext(rContext);
    Map <String, Region<?,?>> colocatedRegions = PartitionRegionHelper.getColocatedRegions(cust);
    Map <String, Region<?,?>> localColocatedRegions = PartitionRegionHelper.getLocalColocatedRegions(rContext);

    Assert.assertTrue(colocatedRegions.size()==2); 
    Set custKeySet = cust.keySet();
    Set localCustKeySet = localCust.keySet();
    
    Region ord = colocatedRegions.get("/OrderPR") ;
    LocalDataSet localOrd = (LocalDataSet)localColocatedRegions.get("/OrderPR");
    Set ordKeySet = ord.keySet();
    Set localOrdKeySet = localOrd.keySet();
    
    Region ship = colocatedRegions.get("/ShipmentPR");
    LocalDataSet localShip = (LocalDataSet)localColocatedRegions.get("/ShipmentPR");
    Set shipKeySet = ship.keySet();
    Set localShipKeySet = localShip.keySet();
    
    Assert.assertTrue(localCust.getBucketSet().size() == localOrd.getBucketSet().size());
    Assert.assertTrue(localCust.getBucketSet().size() == localShip.getBucketSet().size());
    
    Assert.assertTrue(custKeySet.size() == 120);
    Assert.assertTrue(ordKeySet.size() == 120);
    Assert.assertTrue(shipKeySet.size() == 120);    
    
    Assert.assertTrue(localCustKeySet.size() == localOrdKeySet.size());
    Assert.assertTrue(localCustKeySet.size() == localShipKeySet.size());
    
    context.getResultSender().lastResult(null);
  }

  public String getId() {
    return "LocalDataSetFunction" + optimizeForWrite;
  }

  public boolean hasResult() {
    return true;
  }

  public boolean optimizeForWrite() {
    return optimizeForWrite;
  }

  public boolean isHA() {
    return false;
  }
}
