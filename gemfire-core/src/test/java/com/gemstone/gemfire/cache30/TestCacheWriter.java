/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

//import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.cache.*;

/**
 * A <code>CacheWriter</code> used in testing.  Its callback methods
 * are implemented to thrown {@link UnsupportedOperationException}
 * unless the user overrides the "2" methods.
 *
 * @see #wasInvoked
 *
 * @author David Whitlock
 *
 * @since 3.0
 */
public abstract class TestCacheWriter extends TestCacheCallback
  implements CacheWriter {


  public final void beforeUpdate(EntryEvent event)
    throws CacheWriterException {

    this.invoked = true;
    beforeUpdate2(event);
  }

  public void beforeUpdate2(EntryEvent event)
    throws CacheWriterException {

    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  public final void beforeUpdate2(EntryEvent event, Object arg)
    throws CacheWriterException {

    String s = "Shouldn't be invoked";
    throw new UnsupportedOperationException(s);
  }

  public final void beforeCreate(EntryEvent event)
    throws CacheWriterException {

    this.invoked = true;
    beforeCreate2(event);
  }

  public void beforeCreate2(EntryEvent event)
    throws CacheWriterException {

    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  /**
   * Causes code that uses the old API to not compile
   */
  public final void beforeCreate2(EntryEvent event, Object arg)
    throws CacheWriterException {

    String s = "Shouldn't be invoked";
    throw new UnsupportedOperationException(s);
  }

  public final void beforeDestroy(EntryEvent event)
    throws CacheWriterException {

    this.invoked = true;
    beforeDestroy2(event);
  }

  public void beforeDestroy2(EntryEvent event)
    throws CacheWriterException {

    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  public final void beforeDestroy2(EntryEvent event, Object arg)
    throws CacheWriterException {

    String s = "Shouldn't be invoked";
    throw new UnsupportedOperationException(s);
  }

  public final void beforeRegionDestroy(RegionEvent event)
    throws CacheWriterException {

    // check argument to see if this is during tearDown
    if ("teardown".equals(event.getCallbackArgument())) return;

    this.invoked = true;
    beforeRegionDestroy2(event);
  }

  public void beforeRegionDestroy2(RegionEvent event)
    throws CacheWriterException {

    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  public final void beforeRegionDestroy2(RegionEvent event, Object arg)
    throws CacheWriterException {

    String s = "Shouldn't be invoked";
    throw new UnsupportedOperationException(s);
  }
  public final void beforeRegionClear(RegionEvent event)
    throws CacheWriterException {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }
}
