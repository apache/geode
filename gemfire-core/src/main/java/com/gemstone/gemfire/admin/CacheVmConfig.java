/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

/**
 * Configuration for a GemFire cache server VM that is managed by the
 * administration API.  The VM may or may not be running.
 *
 * @see AdminDistributedSystem#addCacheVm()
 *
 * @author darrel
 * @since 5.7
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface CacheVmConfig extends ManagedEntityConfig {
  /**
   * Returns the <code>cache.xml</code> declarative caching
   * initialization file used to configure this cache server VM.  By
   * default, a cache server VM is started without an XML file.
   */
  public String getCacheXMLFile();

  /**
   * Sets the <code>cache.xml</code> declarative caching
   * initialization file used to configure this cache server VM.
   */
  public void setCacheXMLFile(String cacheXml);

  /**
   * Returns the location(s) of user classes (such as cache loaders)
   * required by the cache server VM.
   */
  public String getClassPath();

  /**
   * Sets the location(s) of user classes (such as cache loaders)
   * required by the cache server VM.
   */
  public void setClassPath(String classpath);
}
