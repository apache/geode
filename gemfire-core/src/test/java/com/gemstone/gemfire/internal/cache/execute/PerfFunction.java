/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.internal.cache.execute.data.Order;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;
import com.gemstone.gemfire.internal.cache.execute.data.Shipment;
import com.gemstone.gemfire.internal.cache.execute.data.ShipmentId;

public class PerfFunction implements Function {

  public void execute(FunctionContext context) {
    RegionFunctionContext ctx = (RegionFunctionContext)context;
    Region customerPR = ctx.getDataSet();
    Region orderPR = customerPR.getCache().getRegion(
        PRColocationDUnitTest.OrderPartitionedRegionName);
    Region shipmentPR = customerPR.getCache().getRegion(
        PRColocationDUnitTest.ShipmentPartitionedRegionName);
    ArrayList args = (ArrayList)ctx.getArguments();
    for (int i = 0; i < args.size() / 4; i++) {
      OrderId orderId = (OrderId)args.get(i * 4);
      Order order = (Order)args.get(i * 4 + 1);
      ShipmentId shipmentId = (ShipmentId)args.get(i * 4 + 2);
      Shipment shipment = (Shipment)args.get(i * 4 + 3);
      orderPR.put(orderId, order);
      shipmentPR.put(shipmentId, shipment);
    }
    context.getResultSender().lastResult(null);
  }

  public boolean optimizeForWrite() {
    return true;
  }

  public String getId() {
    return "perfFunction";
  }

  public boolean hasResult() {
    return true;
  }

  public boolean isHA() {
    return false;
  }

}
