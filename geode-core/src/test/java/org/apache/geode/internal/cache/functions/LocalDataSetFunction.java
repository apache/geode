/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.functions;

import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.LocalDataSet;

public class LocalDataSetFunction extends FunctionAdapter {

  private volatile boolean optimizeForWrite;

  public LocalDataSetFunction(boolean optimizeForWrite) {
    this.optimizeForWrite = optimizeForWrite;
  }

  public void execute(FunctionContext context) {
    RegionFunctionContext rContext = (RegionFunctionContext) context;
    Region cust = rContext.getDataSet();
    LocalDataSet localCust = (LocalDataSet) PartitionRegionHelper.getLocalDataForContext(rContext);
    Map<String, Region<?, ?>> colocatedRegions = PartitionRegionHelper.getColocatedRegions(cust);
    Map<String, Region<?, ?>> localColocatedRegions =
        PartitionRegionHelper.getLocalColocatedRegions(rContext);

    Assert.assertTrue(colocatedRegions.size() == 2);
    Set custKeySet = cust.keySet();
    Set localCustKeySet = localCust.keySet();

    Region ord = colocatedRegions.get("/OrderPR");
    LocalDataSet localOrd = (LocalDataSet) localColocatedRegions.get("/OrderPR");
    Set ordKeySet = ord.keySet();
    Set localOrdKeySet = localOrd.keySet();

    Region ship = colocatedRegions.get("/ShipmentPR");
    LocalDataSet localShip = (LocalDataSet) localColocatedRegions.get("/ShipmentPR");
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
