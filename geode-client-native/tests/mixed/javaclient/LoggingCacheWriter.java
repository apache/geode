/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaclient;

import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.cache.*;

/**
 * A <code>CacheWriter</code> 
 *
 * @author GemStone Systems, Inc.
 * @since Brandywine
 */
public class LoggingCacheWriter extends LoggingCacheCallback
  implements CacheWriter {

  /**
   * Zero-argument constructor required for declarative caching
   */
  public LoggingCacheWriter() {
    super();
  }

  public final void beforeUpdate(EntryEvent event)
    throws CacheWriterException {

    log("CacheWriter.beforeUpdate", event);
  }

  public final void beforeCreate(EntryEvent event)
    throws CacheWriterException {

    log( "CacheWriter.beforeCreate", event);
  }

  public final void beforeDestroy(EntryEvent event)
    throws CacheWriterException {

    log("CacheWriter.beforeDestroy", event);
  }

  public final void beforeRegionDestroy(RegionEvent event)
    throws CacheWriterException {

    log("CacheWriter.beforeRegionDestroy", event);
  }
  public final void beforeRegionClear(RegionEvent event)
    throws CacheWriterException {

    log("CacheWriter.beforeRegionClear", event);
  }

}
