/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.util;

import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

@SuppressWarnings("unchecked")
public class DebugCacheListener extends CacheListenerAdapter implements Declarable {

  public void afterCreate(EntryEvent event) {
    log(event);
  }

  public void afterUpdate(EntryEvent event) {
    log(event);
  }

  public void afterInvalidate(EntryEvent event) {
    log(event);
  }
  
  public void afterDestroy(EntryEvent event) {
    log(event);
  }
  
  private void log(EntryEvent event) {
    StringBuilder builder = new StringBuilder();
    builder
      .append("DebugCacheListener: Received ")
      .append(event.getOperation())
      .append(" for key=")
      .append(event.getKey());
    if (event.getNewValue() != null) {
      builder
        .append("; value=")
        .append(event.getNewValue());
    }
    event.getRegion().getCache().getLogger().info(builder.toString());
  }

  public void init(Properties p) {}
  
  public boolean equals(Object obj) {
    // This method is only implemented so that RegionCreator.validateRegion works properly.
    // The CacheListener comparison fails because two of these instances are not equal.
    if (this == obj) {
      return true;
    }

    if (obj == null || !(obj instanceof DebugCacheListener)) {
      return false;
    }
    
    return true;
  }
}
