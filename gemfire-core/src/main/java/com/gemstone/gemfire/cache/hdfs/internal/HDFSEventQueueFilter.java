/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;
import com.gemstone.gemfire.i18n.LogWriterI18n;

/**
 * Current use of this class is limited to ignoring the Bulk DML operations. 
 * 
 * @author hemantb
 *
 */
public class HDFSEventQueueFilter implements GatewayEventFilter{
  private LogWriterI18n logger;
  
  public HDFSEventQueueFilter(LogWriterI18n logger) {
    this.logger = logger; 
  }
  @Override
  public void close() {
    
  }

  @Override
  public boolean beforeEnqueue(GatewayQueueEvent event) {
    Operation op = event.getOperation();
    
    
    /* MergeGemXDHDFSToGFE - Disabled as it is gemxd specific 
    if (op == Operation.BULK_DML_OP) {
     // On accessors there are no parallel queues, so with the 
     // current logic, isSerialWanEnabled function in LocalRegion 
     // always returns true on an accessor. So when a bulk dml 
     // op is fired on accessor, this behavior results in distribution 
     // of the bulk dml operation to other members. To avoid putting 
     // of this bulk dml in parallel queues, added this filter. This 
     // is not the efficient way as the filters are used before inserting 
     // in the queue. The bulk dmls should be blocked before they are distributed.
     if (logger.fineEnabled())
       logger.fine( "HDFSEventQueueFilter:beforeEnqueue: Disallowing insertion of a bulk DML in HDFS queue.");
      return false;
    }*/
    
    return true;
  }

  @Override
  public boolean beforeTransmit(GatewayQueueEvent event) {
   // No op
   return true;
  }

  @Override
  public void afterAcknowledgement(GatewayQueueEvent event) {
    // No op
  }
}
