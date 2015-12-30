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
