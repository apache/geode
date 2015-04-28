/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.ServiceLoader;
import java.util.Stack;

import org.xml.sax.ContentHandler;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.i18n.LogWriterI18n;

/**
 * Interface for configuration XML parsers. Used by {@link CacheXmlParser} to
 * parse entities defined in the XML Namespace returned by
 * {@link #getNamspaceUri()} .
 * 
 * Loaded by {@link ServiceLoader} on {@link XmlParser} class. See file
 * <code>META-INF/services/com.gemstone.gemfire.internal.cache.xmlcache.XmlParser</code>
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public interface XmlParser extends ContentHandler {

  /**
   * Get XML Namespace this parser is responsible for.
   * 
   * @return XML Namespace.
   * @since 8.1
   */
  String getNamspaceUri();

  /**
   * Sets the XML config stack on this parser.
   * 
   * @param stack
   *          current XML config stack.
   * @since 8.1
   */
  void setStack(Stack<Object> stack);
}
