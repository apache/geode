/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.xmlcache;

import javax.xml.XMLConstants;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 * Utilities for use in {@link XmlGenerator} implementation to provide common
 * helper methods.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
// UnitTest XmlGeneratorUtilsTest
public final class XmlGeneratorUtils {

  private XmlGeneratorUtils() {
    // statics only
  }

  /**
   * Adds attribute with <code>localName</code> to <code>attributes</code> if
   * value is not null. Follows the same rules as
   * {@link AttributesImpl#addAttribute(String, String, String, String, String)}
   * .
   * 
   * @param attributes
   *          to add to.
   * @param localName
   *          of attribute to add.
   * @param value
   *          to add to attribute.
   * @since 8.1
   */
  static public void addAttribute(final AttributesImpl attributes, final String localName, final Object value) {
    if (null != value) {
      attributes.addAttribute(XMLConstants.NULL_NS_URI, localName, localName, "", value.toString());
    }
  }

  /**
   * Adds attribute with <code>prefix</code> and <code>localName</code> to
   * <code>attributes</code> if value is not null. Follows the same rules as
   * {@link AttributesImpl#addAttribute(String, String, String, String, String)}
   * .
   * 
   * @param attributes
   *          to add to.
   * @param prefix
   *          of the attribute.
   * @param localName
   *          of attribute to add.
   * @param value
   *          to add to attribute.
   * @since 8.1
   */
  static public void addAttribute(final AttributesImpl attributes, final String prefix, final String localName, final Object value) {
    if (null != value) {
      attributes.addAttribute(XMLConstants.NULL_NS_URI, localName, prefix + ":" + localName, "", value.toString());
    }
  }

  /**
   * Start new element on <code>contentHandler</code> with given
   * <code>prefix</code>, <code>localName</code> and <code>attributes</code>.
   * 
   * @param contentHandler
   *          to start element on.
   * @param prefix
   *          of element
   * @param localName
   *          of element
   * @param attributes
   *          of element
   * @throws SAXException
   *           if
   *           {@link ContentHandler#startElement(String, String, String, Attributes)}
   *           throws {@link SAXException}.
   * @since 8.1
   */
  static public void startElement(final ContentHandler contentHandler, final String prefix, final String localName, final AttributesImpl attributes)
      throws SAXException {
    contentHandler.startElement(XMLConstants.NULL_NS_URI, localName, prefix + ":" + localName, attributes);
  }

  /**
   * End element on <code>contentHandler</code> with given <code>prefix</code>
   * and <code>localName</code>.
   * 
   * @param contentHandler
   *          to start element on.
   * @param prefix
   *          of element
   * @param localName
   *          of element
   * @throws SAXException
   *           if {@link ContentHandler#endElement(String, String, String)}
   *           throws {@link SAXException}.
   * @since 8.1
   */
  static public void endElement(final ContentHandler contentHandler, final String prefix, final String localName) throws SAXException {
    contentHandler.endElement(XMLConstants.NULL_NS_URI, localName, prefix + ":" + localName);
  }

  /**
   * Creates new empty element on <code>contentHandler</code> with given
   * <code>prefix</code>, <code>localName</code> and <code>attributes</code>.
   * 
   * @param contentHandler
   *          to create empty element on.
   * @param prefix
   *          of element
   * @param localName
   *          of element
   * @param attributes
   *          of element
   * @throws SAXException
   *           if
   *           {@link ContentHandler#startElement(String, String, String, Attributes)}
   *           or {@link ContentHandler#endElement(String, String, String)}
   *           throws {@link SAXException}.
   * @since 8.1
   */
  static public void emptyElement(final ContentHandler contentHandler, final String prefix, final String localName, final AttributesImpl attributes)
      throws SAXException {
    startElement(contentHandler, prefix, localName, attributes);
    endElement(contentHandler, prefix, localName);
  }

}
