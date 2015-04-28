/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.xmlcache;

import org.xml.sax.SAXException;

/**
 * Interface for configuration XML generators. Used by {@link CacheXmlGenerator}
 * to generate entities defined in the XML Namespace returned by
 * {@link #getNamspaceUri()} .
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public interface XmlGenerator<T> {

  /**
   * Get XML Namespace this parser is responsible for.
   * 
   * @return XML Namespace.
   * @since 8.1
   */
  String getNamspaceUri();

  // TODO jbarrett - investigate new logging.
  // /**
  // * Sets the XML config {@link LogWriter} on this parser.
  // *
  // * @param logWriter
  // * current XML config {@link LogWriter}.
  // * @since 8.1
  // */
  // void setLogWriter(LogWriterI18n logWriter);
  //

  /**
   * Generate XML configuration to the given {@link CacheXmlGenerator}.
   * 
   * @param cacheXmlGenerator
   *          to generate configuration to.
   * @throws SAXException
   * @since 8.1
   */
  void generate(CacheXmlGenerator cacheXmlGenerator) throws SAXException;

}
