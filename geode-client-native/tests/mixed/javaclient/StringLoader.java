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
 * A <code>CacheLoader</code> that simply returns the {@link
 * String#valueOf string} value of the <code>key</code> that is
 * loaded. 
 *
 * @author GemStone Systems, Inc.
 * @since 3.5
 */
public class StringLoader implements CacheLoader, Declarable {

  /**
   * Simply return the string value of the key being loaded
   *
   * @see LoaderHelper#getKey
   * @see String#valueOf(Object)
   */
  public Object load(LoaderHelper helper)
    throws CacheLoaderException {

    return String.valueOf(helper.getKey());
  }

  /**
   * Nothing to initialize
   */
  public void init(java.util.Properties props) {

  }

  public void close() {

  }
}
