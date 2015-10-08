/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
