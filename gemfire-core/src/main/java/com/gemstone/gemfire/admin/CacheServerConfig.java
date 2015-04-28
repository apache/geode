/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

/**
 * Configuration for a GemFire cache server that is managed by the
 * administration API.  The cache server may or may not be running.
 *
 * @see AdminDistributedSystem#addCacheServer()
 *
 * @author David Whitlock
 * @since 4.0
 * @deprecated as of 5.7 use {@link CacheVmConfig} instead.
 */
@Deprecated
public interface CacheServerConfig extends CacheVmConfig {
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
