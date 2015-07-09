/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;
import com.gemstone.gemfire.internal.cache.execute.data.ShipmentId;

import java.io.Serializable;

public class CustomerIDPartitionResolver implements PartitionResolver {

  private static CustomerIDPartitionResolver customerIDPartitionResolver = null;

  private String id;


  private String resolverName;

  public CustomerIDPartitionResolver(){
  }
  
  public CustomerIDPartitionResolver(String resolverID) {
    id = resolverID;
  }

  public String getName() {
    return this.resolverName;
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {

    Serializable routingbject = null;

    if (opDetails.getKey() instanceof ShipmentId) {
      ShipmentId shipmentId = (ShipmentId)opDetails.getKey();
      routingbject = shipmentId.getOrderId().getCustId();
    }
    if (opDetails.getKey() instanceof OrderId) {
      OrderId orderId = (OrderId)opDetails.getKey();
      routingbject = orderId.getCustId();
    }
    else if (opDetails.getKey() instanceof CustId) {
      CustId custId = (CustId)opDetails.getKey();
      routingbject = custId.getCustId();
    }
    return routingbject;
  }

  public void close() {}

  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (!(o instanceof CustomerIDPartitionResolver))
      return false;

    CustomerIDPartitionResolver otherCustomerIDPartitionResolver = (CustomerIDPartitionResolver)o;
    return otherCustomerIDPartitionResolver.id.equals(this.id);

  }

}



