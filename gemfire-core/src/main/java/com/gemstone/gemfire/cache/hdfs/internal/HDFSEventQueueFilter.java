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
