/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaclient;

import com.gemstone.gemfire.cache.*;

/**
 * A <code>CacheListener</code> that logs information about the events
 * it receives.
 *
 * @author GemStone Systems, Inc.
 * @since Brandywine
 */
public class LoggingCacheListener extends LoggingCacheCallback
  implements CacheListener {

  public final void afterCreate(EntryEvent event) {
    log("CacheListener.afterCreate", event);
  }

  public final void afterUpdate(EntryEvent event) {
    log("CacheListener.afterUpdate", event);
  }

  public final void afterInvalidate(EntryEvent event) {
    log("CacheListener.afterInvalidate", event);
  }

  public final void afterDestroy(EntryEvent event) {
    log("CacheListener.afterDestroy", event);
  }

  public final void afterRegionInvalidate(RegionEvent event) {
    log("CacheListener.afterRegionInvalidate", event);
  }

  public final void afterRegionDestroy(RegionEvent event) {
    log("CacheListener.afterRegionDestroy", event);
  }
  public final void afterRegionClear(RegionEvent event) {
    log("CacheListener.afterRegionClear", event);
  }
  
  public void afterRegionCreate(RegionEvent event) {
      log("CacheListener.afterRegionCreate", event);
  }

  public void afterRegionLive(RegionEvent event) {
      log("CacheListener.afterRegionLive", event);
  }


}
