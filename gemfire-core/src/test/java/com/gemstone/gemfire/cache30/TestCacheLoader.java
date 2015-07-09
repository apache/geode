/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.*;

/**
 * A <code>CacheLoader</code> used in testing.  Users should override
 * the "2" method.
 *
 * @see #wasInvoked
 * @see TestCacheWriter
 *
 * @author David Whitlock
 *
 * @since 3.0
 */
public abstract class TestCacheLoader extends TestCacheCallback
  implements CacheLoader {

  public final Object load(LoaderHelper helper)
    throws CacheLoaderException {

    this.invoked = true;
    return load2(helper);
  }

  public abstract Object load2(LoaderHelper helper)
    throws CacheLoaderException;

}
