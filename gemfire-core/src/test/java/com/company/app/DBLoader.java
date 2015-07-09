/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.company.app;

import java.util.Properties;

import com.gemstone.gemfire.cache.*;

/**
 * A <code>CacheLoader</code> that is <code>Declarable</code>
 *
 * @author David Whitlock
 * @since 3.2.1
 */
public class DBLoader implements CacheLoader, Declarable {

  private Properties props = new Properties();
  
  public Object load(LoaderHelper helper)
    throws CacheLoaderException {

    throw new UnsupportedOperationException("I do NOTHING");
  }

  public void init(java.util.Properties props) {
    this.props = props; 
  }

  public void close() {
  }

  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }

    if (! (obj instanceof DBLoader)) {
      return false;
    }
    
    DBLoader other = (DBLoader) obj;
    if (! this.props.equals(other.props)) {
      return false;
    }
    
    return true;
  }

}
