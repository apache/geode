/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.cache.xmlcache;

import static org.apache.geode.internal.cache.xmlcache.CacheXml.CLASS_NAME;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.DECLARABLE;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.NAME;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.PARAMETER;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.STRING;

import java.util.Map;
import java.util.Properties;

import javax.xml.XMLConstants;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import org.apache.geode.cache.Declarable;

/**
 * Utilities for use in {@link XmlGenerator} implementation to provide common helper methods.
 *
 *
 * @since GemFire 8.1
 */
// UnitTest XmlGeneratorUtilsTest
public class XmlGeneratorUtils {

  private XmlGeneratorUtils() {
    // statics only
  }

  /**
   * Adds attribute with <code>localName</code> to <code>attributes</code> if value is not null.
   * Follows the same rules as
   * {@link AttributesImpl#addAttribute(String, String, String, String, String)} .
   *
   * @param attributes to add to.
   * @param localName of attribute to add.
   * @param value to add to attribute.
   * @since GemFire 8.1
   */
  public static void addAttribute(final AttributesImpl attributes, final String localName,
      final Object value) {
    if (null != value) {
      attributes.addAttribute(XMLConstants.NULL_NS_URI, localName, localName, "", value.toString());
    }
  }

  /**
   * Adds attribute with <code>prefix</code> and <code>localName</code> to <code>attributes</code>
   * if value is not null. Follows the same rules as
   * {@link AttributesImpl#addAttribute(String, String, String, String, String)} .
   *
   * @param attributes to add to.
   * @param prefix of the attribute.
   * @param localName of attribute to add.
   * @param value to add to attribute.
   * @since GemFire 8.1
   */
  public static void addAttribute(final AttributesImpl attributes, final String prefix,
      final String localName, final Object value) {
    if (null != value) {
      attributes.addAttribute(XMLConstants.NULL_NS_URI, localName, prefix + ":" + localName, "",
          value.toString());
    }
  }

  /**
   * Start new element on <code>contentHandler</code> with given <code>prefix</code>,
   * <code>localName</code> and <code>attributes</code>.
   *
   * @param contentHandler to start element on.
   * @param prefix of element
   * @param localName of element
   * @param attributes of element
   * @throws SAXException if {@link ContentHandler#startElement(String, String, String, Attributes)}
   *         throws {@link SAXException}.
   * @since GemFire 8.1
   */
  public static void startElement(final ContentHandler contentHandler, final String prefix,
      final String localName, final AttributesImpl attributes) throws SAXException {
    contentHandler.startElement(XMLConstants.NULL_NS_URI, localName, prefix + ":" + localName,
        attributes);
  }

  /**
   * End element on <code>contentHandler</code> with given <code>prefix</code> and
   * <code>localName</code>.
   *
   * @param contentHandler to start element on.
   * @param prefix of element
   * @param localName of element
   * @throws SAXException if {@link ContentHandler#endElement(String, String, String)} throws
   *         {@link SAXException}.
   * @since GemFire 8.1
   */
  public static void endElement(final ContentHandler contentHandler, final String prefix,
      final String localName) throws SAXException {
    contentHandler.endElement(XMLConstants.NULL_NS_URI, localName, prefix + ":" + localName);
  }

  /**
   * Creates new empty element on <code>contentHandler</code> with given <code>prefix</code>,
   * <code>localName</code> and <code>attributes</code>.
   *
   * @param contentHandler to create empty element on.
   * @param prefix of element
   * @param localName of element
   * @param attributes of element
   * @throws SAXException if {@link ContentHandler#startElement(String, String, String, Attributes)}
   *         or {@link ContentHandler#endElement(String, String, String)} throws
   *         {@link SAXException}.
   * @since GemFire 8.1
   */
  public static void emptyElement(final ContentHandler contentHandler, final String prefix,
      final String localName, final AttributesImpl attributes) throws SAXException {
    startElement(contentHandler, prefix, localName, attributes);
    endElement(contentHandler, prefix, localName);
  }



  public static void addDeclarable(final ContentHandler handler, Declarable declarable)
      throws SAXException {
    AttributesImpl EMPTY = new AttributesImpl();

    String className = declarable.getClass().getName();
    handler.startElement("", CLASS_NAME, CLASS_NAME, EMPTY);
    handler.characters(className.toCharArray(), 0, className.length());
    handler.endElement("", CLASS_NAME, CLASS_NAME);

    if (!(declarable instanceof Declarable2)) {
      return;
    }

    Properties props = ((Declarable2) declarable).getConfig();
    if (props == null) {
      return;
    }

    for (final Map.Entry<Object, Object> objectObjectEntry : props.entrySet()) {
      Map.Entry entry = (Map.Entry) objectObjectEntry;
      String name = (String) entry.getKey();
      Object value = entry.getValue();

      AttributesImpl atts = new AttributesImpl();
      atts.addAttribute("", "", NAME, "", name);

      handler.startElement("", PARAMETER, PARAMETER, atts);

      if (value instanceof String) {
        String sValue = (String) value;
        handler.startElement("", STRING, STRING, EMPTY);
        handler.characters(sValue.toCharArray(), 0, sValue.length());
        handler.endElement("", STRING, STRING);

      } else if (value instanceof Declarable) {
        handler.startElement("", DECLARABLE, DECLARABLE, EMPTY);
        addDeclarable(handler, (Declarable) value);
        handler.endElement("", DECLARABLE, DECLARABLE);

      } else {
        // Ignore it
      }

      handler.endElement("", PARAMETER, PARAMETER);
    }
  }

}
