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
package com.gemstone.gemfire.cache.query.dunit;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;

/**
 * @author anil.
 *
 */
public class CqTimeTestListener implements CqListener {
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

  protected volatile long eventQueryInsertTime = 0;
  protected volatile long eventQueryUpdateTime = 0;

  protected volatile boolean eventClose = false;

  final public Set destroys = Collections.synchronizedSet(new HashSet());
  final public Set creates = Collections.synchronizedSet(new HashSet());
  final public Set invalidates = Collections.synchronizedSet(new HashSet());
  final public Set updates = Collections.synchronizedSet(new HashSet());

  static private final String WAIT_PROPERTY = "CQueryTestListener.maxWaitTime";

  static private final int WAIT_DEFAULT = (20 * 1000);
  
  public static final long MAX_TIME = Integer.getInteger(WAIT_PROPERTY, 
      WAIT_DEFAULT).intValue();;

  public String cqName;
  
  public CqTimeTestListener(LogWriter logger) {
    this.logger = logger;
  }

  public void onEvent(CqEvent cqEvent) {
    this.totalEventCount++;
    
    long currentTime = System.currentTimeMillis();
    
    Operation baseOperation = cqEvent.getBaseOperation();
    Operation queryOperation = cqEvent.getQueryOperation();
    Object key = cqEvent.getKey();
//    logger.info("### Got CQ Event ###; baseOp=" + baseOperation
//                + ";queryOp=" + queryOperation);
//
//    logger.info("Number of events for the CQ: " +this.cqName  + " : " 
//                       + this.totalEventCount
//                       + " Key : " + key);
    
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
      long createTime = ((Portfolio)cqEvent.getNewValue()).getCreateTime();
      this.eventQueryUpdateTime += (currentTime - createTime);
    }
    else if (queryOperation.isCreate()) {
      this.eventQueryInsertCount++;
      long createTime = ((Portfolio)cqEvent.getNewValue()).getCreateTime();
      this.eventQueryInsertTime += (currentTime - createTime);
    }
    else if (queryOperation.isDestroy()) {
      this.eventQueryDeleteCount++;
    }
    else if (queryOperation.isInvalidate()) {
      this.eventQueryInvalidateCount++;
    }

  }

  public void onError(CqEvent cqEvent) {
    this.eventErrorCount++;
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

  public long getTotalQueryUpdateTime() {
    return this.eventQueryUpdateTime;
  }

  public long getTotalQueryCreateTime() {
    return this.eventQueryInsertTime;
  }

  public void close() {
    this.eventClose = true;
  }

  public void printInfo() {
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
  }

  public boolean waitForCreated(final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CqTimeTestListener.this.creates.contains(key);
      }
      public String description() {
        return "never got create event for CQ " + CqTimeTestListener.this.cqName;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForDestroyed(final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CqTimeTestListener.this.destroys.contains(key);
      }
      public String description() {
        return "never got destroy event for CQ " + CqTimeTestListener.this.cqName;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForInvalidated(final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CqTimeTestListener.this.invalidates.contains(key);
      }
      public String description() {
        return "never got invalidate event for CQ " + CqTimeTestListener.this.cqName;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForUpdated(final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CqTimeTestListener.this.updates.contains(key);
      }
      public String description() {
        return "never got update event for CQ " + CqTimeTestListener.this.cqName;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }

  public boolean waitForClose() {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CqTimeTestListener.this.eventClose;
      }
      public String description() {
        return "never got close event for CQ " + CqTimeTestListener.this.cqName;
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
