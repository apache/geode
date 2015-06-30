/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.company.data;

import com.gemstone.gemfire.cache.*;

/**
 * A <code>CacheLoader</code> that is <code>Declarable</code>
 *
 * @author David Whitlock
 * @since 3.2.1
 */
public class DatabaseLoader implements CacheLoader, Declarable {

  public Object load(LoaderHelper helper)
    throws CacheLoaderException {

    throw new UnsupportedOperationException("I do NOTHING");
  }

  public void init(java.util.Properties props) {

  }

  public void close() {

  }

}
