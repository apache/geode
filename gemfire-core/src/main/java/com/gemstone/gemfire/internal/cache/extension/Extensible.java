/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.extension;

import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;

/**
 * Provides marker for extensibility and access to {@link ExtensionPoint}.
 * Objects should implement this interface to support modular config and
 * extensions.
 * 
 * Used in {@link CacheXml} to read and write cache xml configurations.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public interface Extensible<T> {

  /**
   * Get {@link ExtensionPoint} for this object.
   * 
   * @return {@link ExtensionPoint} for this object.
   * @since 8.1
   */
  public ExtensionPoint<T> getExtensionPoint();

}
