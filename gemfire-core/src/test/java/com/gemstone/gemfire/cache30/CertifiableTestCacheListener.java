/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * Test class used to certify that a particular key has arrived in the cache
 * This class is a great way to reduce the liklihood of a race condition 
 */
package com.gemstone.gemfire.cache30;

import java.util.*;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

import dunit.DistributedTestCase;
import dunit.DistributedTestCase.WaitCriterion;

public class CertifiableTestCacheListener extends TestCacheListener implements Declarable2 {
  final public Set destroys = Collections.synchronizedSet(new HashSet());
  final public Set creates = Collections.synchronizedSet(new HashSet());
  final public Set invalidates = Collections.synchronizedSet(new HashSet());
  final public Set updates = Collections.synchronizedSet(new HashSet());
  
  final LogWriter logger;
  
  public CertifiableTestCacheListener(LogWriter l) {
    this.logger = l;
  }

  /**
   * Clears the state of the listener, for consistent behavior
   * this should only be called when there is no activity on the
   * Region
   */
  public void clearState() {
    this.destroys.clear();
    this.creates.clear();
    this.invalidates.clear();
    this.updates.clear();
  }
      
  public List getEventHistory() {
    destroys.clear();
    creates.clear();
    invalidates.clear();
    updates.clear();
    return super.getEventHistory();
  }

  public void afterCreate2(EntryEvent event)
  {
    this.creates.add(event.getKey());
  }

  public void afterDestroy2(EntryEvent event)
  {
    this.destroys.add(event.getKey());
  }

  public void afterInvalidate2(EntryEvent event)
  {
    Object key = event.getKey();
//    logger.fine("got invalidate for " + key);
    this.invalidates.add(key);
  }

  public void afterUpdate2(EntryEvent event)
  {
    this.updates.add(event.getKey());
  }
  
  static private final String WAIT_PROPERTY = 
    "CertifiableTestCacheListener.maxWaitTime";
  static private final int WAIT_DEFAULT = 30000;
  
  public static final long MAX_TIME = Integer.getInteger(WAIT_PROPERTY, 
      WAIT_DEFAULT).intValue();;
  
  
  public boolean waitForCreated(final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CertifiableTestCacheListener.this.creates.contains(key);
      }
      public String description() {
        return "Waiting for key creation: " + key;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForDestroyed(final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CertifiableTestCacheListener.this.destroys.contains(key);
      }
      public String description() {
        return "Waiting for key destroy: " + key;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForInvalidated(final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CertifiableTestCacheListener.this.invalidates.contains(key);
      }
      public String description() {
        return "Waiting for key invalidate: " + key;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }
  
  public boolean waitForUpdated(final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return CertifiableTestCacheListener.this.updates.contains(key);
      }
      public String description() {
        return "Waiting for key update: " + key;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
    return true;
  }

  public Properties getConfig()  {    return null;  }
  public void init(Properties props) {}
}