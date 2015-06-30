/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.catalina;


import com.gemstone.gemfire.modules.session.bootstrap.AbstractCache;
import com.gemstone.gemfire.modules.session.bootstrap.LifecycleTypeAdapter;
import org.apache.catalina.LifecycleEvent;
import org.apache.catalina.LifecycleListener;


public abstract class AbstractCacheLifecycleListener implements LifecycleListener {
  protected AbstractCache cache;
    
  @Override
  public void lifecycleEvent(LifecycleEvent le) {
    cache.lifecycleEvent(LifecycleTypeAdapter.valueOf(le.getType().toUpperCase()));
  }

  /**
   * This is called by Tomcat to set properties on the Listener.
   */
  public void setProperty(String name, String value) {
    cache.setProperty(name, value);
  }

  /*
   * These getters and setters are also called by Tomcat and just passed on to
   * the cache.
   */
  public float getEvictionHeapPercentage() {
    return cache.getEvictionHeapPercentage();
  }

  public void setEvictionHeapPercentage(String evictionHeapPercentage) {
    cache.setEvictionHeapPercentage(evictionHeapPercentage);
  }

  public float getCriticalHeapPercentage() {
    return cache.getCriticalHeapPercentage();
  }

  public void setCriticalHeapPercentage(String criticalHeapPercentage) {
    cache.setCriticalHeapPercentage(criticalHeapPercentage);
  }
  
  public void setRebalance(boolean rebalance) {
    cache.setRebalance(rebalance);
  }
  
  public boolean getRebalance() {
    return cache.getRebalance();
  }
}
