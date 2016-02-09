/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.configuration.domain;

import static com.gemstone.gemfire.management.internal.configuration.utils.XmlConstants.W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION;
import static com.gemstone.gemfire.management.internal.configuration.utils.XmlUtils.getAttribute;
import static javax.xml.XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI;

import java.io.IOException;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.ext.EntityResolver2;

import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlParser;
import com.gemstone.gemfire.management.internal.configuration.utils.XmlUtils;
import com.gemstone.gemfire.management.internal.configuration.utils.XmlUtils.XPathContext;

/**
 * Domain class to determine the order of an element Currently being used to
 * store order information of child elements of "cache"
 * 
 * @author bansods
 *
 */
// UnitTest CacheElementJUnitTest
public class CacheElement {
  static final String XSD_PREFIX = "xsd";
  private static final String XSD_ALL_CHILDREN = "xsd:element";
  private static final String XSD_COMPLEX_TYPE_CHILDREN = "xsd:group|xsd:all|xsd:choice|xsd:sequence";
  private static final String XSD_CHOICE_OR_SEQUENCE_CHILDREN = "xsd:element|xsd:group|xsd:choice|xsd:sequence|xsd:any";

  static final String CACHE_TYPE_EMBEDDED = "/xsd:schema/xsd:element[@name='cache']/xsd:complexType";

  private String name;
  private int order;
  private boolean multiple;

  public CacheElement(String name, int rank, boolean multiple) {
    this.name = name;
    this.order = rank;
    this.multiple = multiple;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getOrder() {
    return order;
  }

  public void setOrder(int order) {
    this.order = order;
  }

  public boolean isMultiple() {
    return multiple;
  }

  public void setMultiple(boolean multiple) {
    this.multiple = multiple;
  }

  /**
   * Build <code>cache</code> element map for given <cod>doc</code>'s
   * schemaLocation for {@link CacheXml#NAMESPACE}.
   * 
   * @param doc
   *          {@link Document} to parse schema for.
   * @return Element map
   * @throws IOException
   * @throws ParserConfigurationException 
   * @throws SAXException 
   * @throws XPathExpressionException 
   * @since 8.1
   */
  public static LinkedHashMap<String, CacheElement> buildElementMap(final Document doc) throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
    final Map<String, List<String>> schemaLocationMap = XmlUtils.buildSchemaLocationMap(
        getAttribute(doc.getFirstChild(), W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION, W3C_XML_SCHEMA_INSTANCE_NS_URI));

    final LinkedHashMap<String, CacheElement> elementMap = new LinkedHashMap<String, CacheElement>();

    buildElementMapCacheType(elementMap, resolveSchema(schemaLocationMap, CacheXml.NAMESPACE));

    // if we are ever concerned with the order of extensions or children process them here.

    return elementMap;
  }

  /**
   * Resolve schema from <code>schemaLocationsNape</code> or
   * <code>namespaceUri</code> for given <code>namespaceUri</code>.
   * 
   * @param schemaLocationMap
   *          {@link Map} of namespaceUri to URLs.
   * @param namespaceUri
   *          Namespace URI for schema.
   * @return {@link InputSource} for schema if found.
   * @throws IOException
   *           if unable to open {@link InputSource}.
   * @since 8.1
   */
  private static final InputSource resolveSchema(final Map<String, List<String>> schemaLocationMap, String namespaceUri) throws IOException {
    final EntityResolver2 entityResolver = new CacheXmlParser();

    InputSource inputSource = null;

    // Try loading schema from locations until we find one.
    final List<String> locations = schemaLocationMap.get(namespaceUri);
    for (final String location : locations) {
      try {
        inputSource = entityResolver.resolveEntity(null, location);
        if (null != inputSource) {
          break;
        }
      } catch (final SAXException e) {
        // ignore
      }
    }

    if (null == inputSource) {
      // Try getting it from the namespace, will throw if does not exist.
      inputSource = new InputSource(new URL(namespaceUri).openStream());
    }

    return inputSource;
  }

  /**
   * Build element map adding to existing <code>elementMap</code>.
   * 
   * @param elementMap
   *          to add elements to.
   * @param inputSource
   *          to parse elements from.
   * @throws SAXException
   * @throws IOException
   * @throws ParserConfigurationException 
   * @throws XPathExpressionException 
   * @since 8.1
   */
  private static final void buildElementMapCacheType(final LinkedHashMap<String, CacheElement> elementMap, final InputSource inputSource) throws SAXException,
      IOException, ParserConfigurationException, XPathExpressionException {
    final Document doc = XmlUtils.getDocumentBuilder().parse(inputSource);

    int rank = 0;

    final XPathContext xPathContext = new XPathContext(XSD_PREFIX, XMLConstants.W3C_XML_SCHEMA_NS_URI);
    final Node cacheType = XmlUtils.querySingleElement(doc, CACHE_TYPE_EMBEDDED, xPathContext);

    rank = buildElementMapXPath(elementMap, doc, cacheType, rank, XSD_COMPLEX_TYPE_CHILDREN, xPathContext);
  }

  /**
   * Build element map for elements matching <code>xPath</code> relative to
   * <code>parent</code> into <code>elementMap</code> .
   * 
   * @param elementMap
   *          to add elements to
   * @param schema
   *          {@link Document} for schema.
   * @param parent
   *          {@link Element} to query XPath.
   * @param rank
   *          current rank of elements.
   * @param xPath
   *          XPath to query for elements.
   * @param xPathContext
   *          XPath context for queries.
   * @return final rank of elements.
   * @throws XPathExpressionException 
   * @since 8.1
   */
  private static int buildElementMapXPath(final LinkedHashMap<String, CacheElement> elementMap, final Document schema, final Node parent, int rank,
      final String xPath, final XPathContext xPathContext) throws XPathExpressionException {
    final NodeList children = XmlUtils.query(parent, xPath, xPathContext);
    for (int i = 0; i < children.getLength(); i++) {
      final Element child = (Element) children.item(i);
      switch (child.getNodeName()) {
      case XSD_ALL_CHILDREN:
        final String name = getAttribute(child, "name");
        elementMap.put(name, new CacheElement(name, rank++, isMultiple(child)));
        break;
      // TODO group support as XSD matures
      // case "xsd:group":
      // buildElementMapGroup(elementMap, doc, child, rank, xPathContext);
      // break;
      case "xsd:choice":
      case "xsd:sequence":
        rank = buildElementMapXPath(elementMap, schema, child, rank, XSD_CHOICE_OR_SEQUENCE_CHILDREN, xPathContext);
        break;
      case "xsd:any":
        // ignore extensions
        break;
      default:
        // TODO jbarrett - localize
        throw new UnsupportedOperationException("Unsupported child type '" + child.getNodeName() + "'");
      }
    }

    return rank;
  }

  /**
   * Tests if element allows multiple.
   * 
   * @param element
   *          to test for multiple.
   * @return true if mulitple allowed, otherwise false.
   * @since 8.1
   */
  private static boolean isMultiple(final Element element) {
    final String maxOccurs = getAttribute(element, "maxOccurs");
    if (null != maxOccurs && !maxOccurs.equals("1")) {
      // is "unbounded" or greater than 1 if valid schema.
      return true;
    }
    return false;
  }

}
