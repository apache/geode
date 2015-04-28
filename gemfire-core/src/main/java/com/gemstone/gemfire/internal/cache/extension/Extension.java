/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.extension;

import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;

/**
 * Interface used for objects wishing to extend and {@link Extensible} object.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public interface Extension<T> {

  /**
   * Get {@link XmlGenerator} capable of serializing this object's
   * configuration.
   * 
   * @return {@link XmlGenerator} for this object's configuration.
   * @since 8.1
   */
  XmlGenerator<T> getXmlGenerator();

  /**
   * Called by {@link CacheXml} objects that are {@link Extensible} to create
   * this extension.
   * 
   * @param source
   *          source object this extension is currently attached to.
   * @param target
   *          target object to attach any created extensions to.
   * @since 8.1
   */
  void onCreate(Extensible<T> source, Extensible<T> target);

}
