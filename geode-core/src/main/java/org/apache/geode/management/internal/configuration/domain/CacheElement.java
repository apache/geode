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
package org.apache.geode.management.internal.configuration.domain;

import static javax.xml.XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI;
import static org.apache.geode.management.internal.configuration.utils.XmlConstants.W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION;
import static org.apache.geode.management.internal.configuration.utils.XmlUtils.getAttribute;

import java.io.IOException;
import java.net.URL;
import java.util.LinkedHashMap;
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

import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.CacheXmlParser;
import org.apache.geode.internal.cache.xmlcache.GeodeEntityResolver2;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.management.internal.configuration.utils.XmlUtils.XPathContext;
import org.apache.geode.services.module.ModuleService;

/**
 * Domain class to determine the order of an element Currently being used to store order information
 * of child elements of "cache"
 *
 *
 */
// UnitTest CacheElementJUnitTest
public class CacheElement {
  static final String XSD_PREFIX = "xsd";
  private static final String XSD_ALL_CHILDREN = "xsd:element";
  private static final String XSD_COMPLEX_TYPE_CHILDREN =
      "xsd:group|xsd:all|xsd:choice|xsd:sequence";
  private static final String XSD_CHOICE_OR_SEQUENCE_CHILDREN =
      "xsd:element|xsd:group|xsd:choice|xsd:sequence|xsd:any";

  static final String CACHE_TYPE_EMBEDDED =
      "/xsd:schema/xsd:element[@name='cache']/xsd:complexType";

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
   * Build <code>cache</code> element map for given <cod>doc</code>'s schemaLocation for
   * {@link CacheXml#GEODE_NAMESPACE}.
   *
   * @param doc {@link Document} to parse schema for.
   * @return Element map
   * @since GemFire 8.1
   */
  public static LinkedHashMap<String, CacheElement> buildElementMap(final Document doc,
      ModuleService moduleService)
      throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
    Node cacheNode = doc.getFirstChild();
    if ("#comment".equals(cacheNode.getNodeName())) {
      cacheNode = cacheNode.getNextSibling();
    }
    final Map<String, String> schemaLocationMap =
        XmlUtils.buildSchemaLocationMap(getAttribute(cacheNode,
            W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION, W3C_XML_SCHEMA_INSTANCE_NS_URI));

    final LinkedHashMap<String, CacheElement> elementMap = new LinkedHashMap<>();

    buildElementMapCacheType(elementMap,
        resolveSchema(schemaLocationMap, CacheXml.GEODE_NAMESPACE, moduleService), moduleService);

    // if we are ever concerned with the order of extensions or children process them here.

    return elementMap;
  }

  /**
   * Resolve schema from <code>schemaLocationsNape</code> or <code>namespaceUri</code> for given
   * <code>namespaceUri</code>.
   *
   * @param schemaLocationMap {@link Map} of namespaceUri to URLs.
   * @param namespaceUri Namespace URI for schema.
   * @return {@link InputSource} for schema if found.
   * @throws IOException if unable to open {@link InputSource}.
   * @since GemFire 8.1
   */
  private static InputSource resolveSchema(final Map<String, String> schemaLocationMap,
      String namespaceUri, ModuleService moduleService) throws IOException {
    final GeodeEntityResolver2 entityResolver = new CacheXmlParser();
    entityResolver.init(moduleService);
    InputSource inputSource = null;

    // Try loading schema from locations until we find one.
    final String location = schemaLocationMap.get(namespaceUri);

    try {
      inputSource = entityResolver.resolveEntity(null, location);
    } catch (final SAXException e) {
      // ignore
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
   * @param elementMap to add elements to.
   * @param inputSource to parse elements from.
   * @since GemFire 8.1
   */
  private static void buildElementMapCacheType(final LinkedHashMap<String, CacheElement> elementMap,
      final InputSource inputSource, ModuleService moduleService)
      throws SAXException, IOException, ParserConfigurationException, XPathExpressionException {
    final Document doc = XmlUtils.getDocumentBuilder(moduleService).parse(inputSource);

    int rank = 0;

    final XPathContext xPathContext =
        new XPathContext(XSD_PREFIX, XMLConstants.W3C_XML_SCHEMA_NS_URI);
    final Node cacheType = XmlUtils.querySingleElement(doc, CACHE_TYPE_EMBEDDED, xPathContext);

    rank = buildElementMapXPath(elementMap, doc, cacheType, rank, XSD_COMPLEX_TYPE_CHILDREN,
        xPathContext);
  }

  /**
   * Build element map for elements matching <code>xPath</code> relative to <code>parent</code> into
   * <code>elementMap</code> .
   *
   * @param elementMap to add elements to
   * @param schema {@link Document} for schema.
   * @param parent {@link Element} to query XPath.
   * @param rank current rank of elements.
   * @param xPath XPath to query for elements.
   * @param xPathContext XPath context for queries.
   * @return final rank of elements.
   * @since GemFire 8.1
   */
  private static int buildElementMapXPath(final LinkedHashMap<String, CacheElement> elementMap,
      final Document schema, final Node parent, int rank, final String xPath,
      final XPathContext xPathContext) throws XPathExpressionException {
    final NodeList children = XmlUtils.query(parent, xPath, xPathContext);
    for (int i = 0; i < children.getLength(); i++) {
      final Element child = (Element) children.item(i);
      switch (child.getNodeName()) {
        case XSD_ALL_CHILDREN:
          final String name = getAttribute(child, "name");
          elementMap.put(name, new CacheElement(name, rank++, isMultiple(child)));
          break;
        case "xsd:choice":
        case "xsd:sequence":
          rank = buildElementMapXPath(elementMap, schema, child, rank,
              XSD_CHOICE_OR_SEQUENCE_CHILDREN, xPathContext);
          break;
        case "xsd:any":
          // ignore extensions
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported child type '" + child.getNodeName() + "'");
      }
    }

    return rank;
  }

  /**
   * Tests if element allows multiple.
   *
   * @param element to test for multiple.
   * @return true if multiple allowed, otherwise false.
   * @since GemFire 8.1
   */
  private static boolean isMultiple(final Element element) {
    String maxOccurs = getAttribute(element, "maxOccurs");
    return null != maxOccurs && !maxOccurs.equals("1");
  }

}
