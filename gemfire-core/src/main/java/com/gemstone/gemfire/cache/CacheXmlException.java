/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * Thrown when a problem is encountered while parsing a <A
 * href="package-summary.html#declarative">declarative caching XML
 * file</A>.  Examples of such problems are a malformed XML file or
 * the inability to load a {@link Declarable} class.
 *
 * @see CacheFactory#create
 *
 * @author David Whitlock
 *
 * @since 3.0
 */
public class CacheXmlException extends CacheRuntimeException {
private static final long serialVersionUID = -4343870964883131754L;

  /**
   * Creates a new <code>CacheXmlException</code> with the given
   * description and cause.
   */
  public CacheXmlException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new <code>CacheXmlException</code> with the given
   * description.
   */
  public CacheXmlException(String message) {
    super(message);
  }

}
