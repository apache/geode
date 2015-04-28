/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.management.internal.configuration.utils;

import javax.xml.XMLConstants;

/**
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public final class XmlConstants {

  /**
   * Standard prefix for {@link XMLConstants#W3C_XML_SCHEMA_INSTANCE_NS_URI}
   * (http://www.w3.org/2001/XMLSchema-instance) namespace.
   * 
   * @since 8.1
   */
  public static final String W3C_XML_SCHEMA_INSTANCE_PREFIX = "xsi";

  /**
   * Schema location attribute local name, "schemaLocation", in
   * {@link XMLConstants#W3C_XML_SCHEMA_INSTANCE_NS_URI}
   * (http://www.w3.org/2001/XMLSchema-instance) namespace.
   * 
   * @since 8.1
   */
  public static final String W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION = "schemaLocation";

  /**
   * Default prefix. Effectively no prefix.
   * 
   * @since 8.1
   */
  public static final String DEFAULT_PREFIX = "";

  private XmlConstants() {
    // statics only
  }
}
