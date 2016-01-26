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
package com.gemstone.gemfire.cache.query.cq.dunit;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqStatusListener;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;

/**
 * @author rmadduri
 *
 */
public class CqQueryTestListener implements CqStatusListener {
  protected final LogWriter logger;
  protected volatile int eventCreateCount = 0;
  protected volatile int eventUpdateCount = 0;
  protected volatile int eventDeleteCount = 0;
  protected volatile int eventInvalidateCount = 0;
  protected volatile int eventErrorCount = 0;

  protected volatile int totalEventCount = 0;
  protected volatile int eventQueryInsertCount = 0;
  protected volatile int eventQueryUpdateCount = 0;
  protected volatile int eventQueryDeleteCount = 0;
  protected volatile int eventQueryInvalidateCount = 0;
  
  protected volatile int cqsConnectedCount = 0;
  protected volatile int cqsDisconnectedCount = 0;

  protected volatile boolean eventClose = false;
  protected volatile boolean eventRegionClear = false;
  protected volatile boolean eventRegionInvalidate = false;

  final public Set destroys = Collections.synchronizedSet(new HashSet());
  final public Set creates = Collections.synchronizedSet(new HashSet());
  final public Set invalidates = Collections.synchronizedSet(new HashSet());
  final public Set updates = Collections.synchronizedSet(new HashSet());
  final public Set errors = Collections.synchronizedSet(new HashSet());

  static private final String WAIT_PROPERTY = "CqQueryTestListener.maxWaitTime";

  static private final int WAIT_DEFAULT = (20 * 1000);
  
  public static final long MAX_TIME = Integer.getInteger(WAIT_PROPERTY, 
      WAIT_DEFAULT).intValue();;

  public String cqName;
  public String userName;

  // This is to avoid reference to PerUserRequestSecurityTest which will fail to
  // initialize in a non Hydra environment.
  static public boolean usedForUnitTests = true;
    
  public ConcurrentLinkedQueue events = new ConcurrentLinkedQueue();
  
  public ConcurrentLinkedQueue cqEvents = new ConcurrentLinkedQueue();
  
  public CqQueryTestListener(LogWriter logger) {
    this.logger = logger;
  }

  public void onEvent(CqEvent cqEvent) {
    this.totalEventCount++;
    
    Operation baseOperation = cqEvent.getBaseOperation();
    Operation queryOperation = cqEvent.getQueryOperation();
    Object key = cqEvent.getKey();
    
//    logger.info("CqEvent for the CQ: " + this.cqName  +
//                "; Key=" + key +
//                "; baseOp=" + baseOperation +
//                "; queryOp=" + queryOperation + 
//                "; totalEventCount=" + this.totalEventCount
//                );
    
    if (key != null) {
      events.add(key);
      cqEvents.add(cqEvent);
    }
    
    if (baseOperation.isUpdate()) {
      this.eventUpdateCount++;
      this.updates.add(key);
    }
    else if (baseOperation.isCreate()) {
      this.eventCreateCount++;
      this.creates.add(key);
    }
    else if (baseOperation.isDestroy()) {
      this.eventDeleteCount++;
      this.destroys.add(key);
    }
    else if (baseOperation.isInvalidate()) {
      this.eventDeleteCount++;
      this.invalidates.add(key);
    }

    if (queryOperation.isUpdate()) {
      this.eventQueryUpdateCount++;
    }
    else if (queryOperation.isCreate()) {
      this.eventQueryInsertCount++;
    }
    else if (queryOperation.isDestroy()) {
      this.eventQueryDeleteCount++;
    }
    else if (queryOperation.isInvalidate()) {
      this.eventQueryInvalidateCount++;
    }
    else if (queryOperation.isClear()) {
      this.eventRegionClear = true;
    }
    else if (queryOperation.isRegionInvalidate()) {
      this.eventRegionInvalidate = true;
    }
  }

  public void onError(CqEvent cqEvent) {
    this.eventErrorCount++;
    this.errors.add(cqEvent.getThrowable().getMessage());
  }
  
  public void onCqDisconnected() {
    this.cqsDisconnectedCount++;
  }
  
  public void onCqConnected() {
    this.cqsConnectedCount++;
  }
  
  public int getErrorEventCount() {
    return this.eventErrorCount;
  }

  public int getTotalEventCount() {
    return this.totalEventCount;
  }

  public int getCreateEventCount() {
    return this.eventCreateCount;
  }

  public int getUpdateEventCount() {
    return this.eventUpdateCount;
  }
  
  public int getDeleteEventCount() {
    return this.eventDeleteCount;
  }

  public int getInvalidateEventCount() {
    return this.eventInvalidateCount;
  }

  public int getQueryInsertEventCount() {
    return this.eventQueryInsertCount;
  }
  
  public int getQueryUpdateEventCount() {
    return this.eventQueryUpdateCount;
  }
  
  public int getQueryDeleteEventCount() {
    return this.eventQueryDeleteCount;
  }

  public int getQueryInvalidateEventCount() {
    return this.eventQueryInvalidateCount;
  }

  public Object[] getEvents() {
    return this.cqEvents.toArray();  
  }
  
  public void close() {
    this.eventClose = true;
  }

  public void printInfo(final boolean printKeys) {
    logger.info("####" + this.cqName + ": " + 
      " Events Total :" + this.getTotalEventCount() +
      " Events Created :" + this.eventCreateCount +
      " Events Updated :" + this.eventUpdateCount +
      " Events Deleted :" + this.eventDeleteCount +
      " Events Invalidated :" + this.eventInvalidateCount +
      " Query Inserts :" + this.eventQueryInsertCount +
      " Query Updates :" + this.eventQueryUpdateCount +
      " Query Deletes :" + this.eventQueryDeleteCount +
      " Query Invalidates :" + this.eventQueryInvalidateCount +
      " Total Events :" + this.totalEventCount);
    if (printKeys) {
    	// for debugging on failuers ...
      logger.info("Number of Insert for key : " + this.creates.size()
				+ " and updates : " + this.updates.size()
				+ " and number of destroys : " + this.destroys.size() +" and number of invalidates : "+this.invalidates.size());
      
      logger.info("Keys in created sets : "+this.creates.toString());
      logger.info("Key in updates sets : "+this.updates.toString());
      logger.info("Key in destorys sets : "+this.destroys.toString());
      logger.info("Key in invalidates sets : "+this.invalidates.toString());
    }
    
  }

  public boolean waitForCreated(final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CqQueryTestListener.this.creates.contains(key);
      }
      public String description() {
        return "never got create event for CQ " + CqQueryTestListener.this.cqName + " key " + key;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }

  
  public boolean waitForTotalEvents(final int total) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return (CqQueryTestListener.this.totalEventCount == total);
      }
      public String description() {
        return "Did not receive expected number of events " + CqQueryTestListener.this.cqName + 
          " expected: " + total + " receieved: " + CqQueryTestListener.this.totalEventCount;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForDestroyed(final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CqQueryTestListener.this.destroys.contains(key);
      }
      public String description() {
        return "never got destroy event for key " + key + " in CQ " + CqQueryTestListener.this.cqName;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForInvalidated(final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CqQueryTestListener.this.invalidates.contains(key);
      }
      public String description() {
        return "never got invalidate event for CQ " + CqQueryTestListener.this.cqName;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForUpdated(final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CqQueryTestListener.this.updates.contains(key);
      }
      public String description() {
        return "never got update event for CQ " + CqQueryTestListener.this.cqName;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }

  public boolean waitForClose() {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CqQueryTestListener.this.eventClose;
      }
      public String description() {
        return "never got close event for CQ " + CqQueryTestListener.this.cqName;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForRegionClear() {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CqQueryTestListener.this.eventRegionClear;
      }
      public String description() {
        return "never got region clear event for CQ " + CqQueryTestListener.this.cqName;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }

  public boolean waitForRegionInvalidate() {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CqQueryTestListener.this.eventRegionInvalidate;
      }
      public String description() {
        return "never got region invalidate event for CQ " + CqQueryTestListener.this.cqName;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForError(final String expectedMessage) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        Iterator iterator = CqQueryTestListener.this.errors.iterator();
        while (iterator.hasNext()) {
          String errorMessage = (String) iterator.next();
          if (errorMessage.equals(expectedMessage)) {
            return true;
          }
          else {
            logger.fine("errors that exist:" + errorMessage);
          }
        }
        return false;
      }
      public String description() {
        return "never got create error for CQ " + CqQueryTestListener.this.cqName + " messaged " + expectedMessage;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForCqsDisconnectedEvents(final int total) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return (CqQueryTestListener.this.cqsDisconnectedCount == total);
      }
      public String description() {
        return "Did not receive expected number of calls to cqsDisconnected() " + CqQueryTestListener.this.cqName + 
          " expected: " + total + " received: " + CqQueryTestListener.this.cqsDisconnectedCount;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForCqsConnectedEvents(final int total) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return (CqQueryTestListener.this.cqsConnectedCount == total);
      }
      public String description() {
        return "Did not receive expected number of calls to cqsConnected() " + CqQueryTestListener.this.cqName + 
          " expected: " + total + " receieved: " + CqQueryTestListener.this.cqsConnectedCount;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  

  public void getEventHistory() {
    destroys.clear();
    creates.clear();
    invalidates.clear();
    updates.clear();
    this.eventClose = false;
  }

}
