/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;


import java.io.Serializable;
import java.util.Properties;


import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

public class CacheXMLPartitionResolver implements PartitionResolver, Serializable, Declarable2 {
  private final Properties resolveProps;

  public CacheXMLPartitionResolver() {
    this.resolveProps = new Properties();
    this.resolveProps.setProperty("routingType", "key");
  }

  public String getName() {
    return getClass().getName();
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    return null;
  }

  public void close() {}

  // @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (! obj.getClass().equals(this.getClass())) {
      return false;
    }
    CacheXMLPartitionResolver other = (CacheXMLPartitionResolver)obj; 
    if (!this.resolveProps.equals(other.getConfig())) {
      return false;
    }

    return true;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.xmlcache.Declarable2#getConfig()
   */
  public Properties getConfig() {
    return this.resolveProps;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
   */
  public void init(Properties props) {
    this.resolveProps.putAll(props);
  }
}
