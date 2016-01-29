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
package com.gemstone.gemfire.management.internal.configuration.utils;

import static com.gemstone.gemfire.management.internal.configuration.utils.XmlConstants.*;
import static javax.xml.XMLConstants.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.management.internal.configuration.utils.XmlUtils.XPathContext;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link XmlUtils}. See Also {@link XmlUtilsAddNewNodeJUnitTest}
 * for tests related to {@link XmlUtils#addNewNode(Document, XmlEntity)}
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
@Category(UnitTest.class)
public class XmlUtilsJUnitTest {

  /**
   * Test method for {@link XmlUtils#buildSchemaLocationMap(String)}.
   * @throws IOException 
   * @throws ParserConfigurationException 
   * @throws SAXException 
   */
  @Test
  public void testBuildSchemaLocationMapAttribute() throws SAXException, ParserConfigurationException, IOException {
    final Document doc = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass().getResourceAsStream(
        "XmlUtilsJUnitTest.testBuildSchemaLocationMapAttribute.xml")));
    final String schemaLocationAttribute = XmlUtils.getAttribute(doc.getDocumentElement(), W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION,
        W3C_XML_SCHEMA_INSTANCE_NS_URI);
    final Map<String, List<String>> schemaLocationMap = XmlUtils.buildSchemaLocationMap(schemaLocationAttribute);

    assertNotNull(schemaLocationMap);
    assertEquals(2, schemaLocationMap.size());

    final List<String> locations1 = schemaLocationMap.get("http://schema.pivotal.io/gemfire/cache");
    assertNotNull(locations1);
    assertEquals(1, locations1.size());
    assertEquals("http://schema.pivotal.io/gemfire/cache/cache-8.1.xsd", locations1.get(0));

    final List<String> locations2 = schemaLocationMap.get("urn:java:com/gemstone/gemfire/management/internal/configuration/utils/XmlUtilsJUnitTest");
    assertNotNull(locations2);
    assertEquals(2, locations2.size());
    assertEquals("classpath:/com/gemstone/gemfire/management/internal/configuration/utils/XmlUtilsJUnitTest.xsd", locations2.get(0));
    assertEquals("XmlUtilsJUnitTest.xsd", locations2.get(1));

    final List<String> locations3 = schemaLocationMap.get("urn:__does_not_exist__");
    assertNull(locations3);
  }

  /**
   * Test method for {@link XmlUtils#buildSchemaLocationMap(Map, String)}.
   * @throws IOException 
   * @throws ParserConfigurationException 
   * @throws SAXException 
   */
  @Test
  public void testBuildSchemaLocationMapMapOfStringListOfStringAttribute() throws SAXException, ParserConfigurationException, IOException {
    Map<String, List<String>> schemaLocationMap = new HashMap<>();

    final Document doc1 = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass().getResourceAsStream(
        "XmlUtilsJUnitTest.testBuildSchemaLocationMapAttribute.xml")));
    final String schemaLocationAttribute1 = XmlUtils.getAttribute(doc1.getDocumentElement(), W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION,
        W3C_XML_SCHEMA_INSTANCE_NS_URI);
    schemaLocationMap = XmlUtils.buildSchemaLocationMap(schemaLocationMap, schemaLocationAttribute1);

    final Document doc2 = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass().getResourceAsStream(
        "XmlUtilsJUnitTest.testBuildSchemaLocationMapMapOfStringListOfStringAttribute.xml")));
    final String schemaLocationAttribute2 = XmlUtils.getAttribute(doc2.getDocumentElement(), W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION,
        W3C_XML_SCHEMA_INSTANCE_NS_URI);
    schemaLocationMap = XmlUtils.buildSchemaLocationMap(schemaLocationMap, schemaLocationAttribute2);

    assertNotNull(schemaLocationMap);
    assertEquals(3, schemaLocationMap.size());

    final List<String> locations1 = schemaLocationMap.get("http://schema.pivotal.io/gemfire/cache");
    assertNotNull(locations1);
    assertEquals(2, locations1.size());
    assertEquals("http://schema.pivotal.io/gemfire/cache/cache-8.1.xsd", locations1.get(0));
    assertEquals("cache-8.1.xsd", locations1.get(1));

    final List<String> locations2 = schemaLocationMap.get("urn:java:com/gemstone/gemfire/management/internal/configuration/utils/XmlUtilsJUnitTest");
    assertNotNull(locations2);
    assertEquals(2, locations2.size());
    assertEquals("classpath:/com/gemstone/gemfire/management/internal/configuration/utils/XmlUtilsJUnitTest.xsd", locations2.get(0));
    assertEquals("XmlUtilsJUnitTest.xsd", locations2.get(1));

    final List<String> locations3 = schemaLocationMap.get("urn:__does_not_exist__");
    assertNull(locations3);

    final List<String> locations4 = schemaLocationMap.get("urn:java:com/gemstone/gemfire/management/internal/configuration/utils/XmlUtilsJUnitTest2");
    assertNotNull(locations4);
    assertEquals(1, locations4.size());
    assertEquals("XmlUtilsJUnitTest2.xsd", locations4.get(0));
  }

  /**
   * Test method for {@link XmlUtils#buildSchemaLocationMap(Map, String)}.
   * Asserts map is empty if schemaLocation attribute is <code>null</code>.
   * @throws IOException 
   * @throws ParserConfigurationException 
   * @throws SAXException 
   */
  @Test
  public void testBuildSchemaLocationMapNullAttribute() throws SAXException, ParserConfigurationException, IOException {
    final Document doc = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass().getResourceAsStream(
        "XmlUtilsJUnitTest.testBuildSchemaLocationMapNullAttribute.xml")));
    final String schemaLocationAttribute = XmlUtils.getAttribute(doc.getDocumentElement(), W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION,
        W3C_XML_SCHEMA_INSTANCE_NS_URI);
    assertNull(schemaLocationAttribute);
    final Map<String, List<String>> schemaLocationMap = XmlUtils.buildSchemaLocationMap(schemaLocationAttribute);
    assertEquals(0, schemaLocationMap.size());
  }

  /**
   * Test method for {@link XmlUtils#buildSchemaLocationMap(Map, String)}.
   * Asserts map is empty if schemaLocation attribute is empty.
   * @throws IOException 
   * @throws ParserConfigurationException 
   * @throws SAXException 
   */
  @Test
  public void testBuildSchemaLocationMapEmptyAttribute() throws SAXException, ParserConfigurationException, IOException {
    final Document doc = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass().getResourceAsStream(
        "XmlUtilsJUnitTest.testBuildSchemaLocationMapEmptyAttribute.xml")));
    final String schemaLocationAttribute = XmlUtils.getAttribute(doc.getDocumentElement(), W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION,
        W3C_XML_SCHEMA_INSTANCE_NS_URI);
    assertNotNull(schemaLocationAttribute);
    final Map<String, List<String>> schemaLocationMap = XmlUtils.buildSchemaLocationMap(schemaLocationAttribute);
    assertEquals(0, schemaLocationMap.size());
  }

  /**
   * Test method for
   * {@link XmlUtils#querySingleElement(Node, String, XPathContext)}.
   * @throws XPathExpressionException 
   * @throws IOException 
   * @throws ParserConfigurationException 
   * @throws SAXException 
   */
  @Test
  public void testQuerySingleElement() throws XPathExpressionException, SAXException, ParserConfigurationException, IOException {
    final Document doc = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass().getResourceAsStream(
        "XmlUtilsJUnitTest.testQuerySingleElement.xml")));
    final Element root = doc.getDocumentElement();
    final String cacheNamespace = "http://schema.pivotal.io/gemfire/cache";
    final XPathContext cacheXPathContext = new XPathContext("cache", cacheNamespace);

    // There are mulitple region elements, this should get the first one.
    final NodeList n1 = XmlUtils.query(root, "//cache:region[1]", cacheXPathContext);
    final Node e1 = XmlUtils.querySingleElement(root, "//cache:region", cacheXPathContext);
    assertNotNull(e1);
    assertSame(root.getElementsByTagNameNS(cacheNamespace, "region").item(0), e1);
    assertSame(n1.item(0), e1);

    // This should get the second region with name "r2".
    final NodeList n2 = XmlUtils.query(root, "//cache:region[2]", cacheXPathContext);
    final Node e2 = XmlUtils.querySingleElement(root, "//cache:region[@name='r2']", cacheXPathContext);
    assertNotNull(e2);
    assertSame(root.getElementsByTagNameNS(cacheNamespace, "region").item(1), e2);
    assertSame(n2.item(0), e2);

    // This should get none since there is no r3.
    final Node e3 = XmlUtils.querySingleElement(root, "//cache:region[@name='r3']", cacheXPathContext);
    assertNull(e3);

    // Query attributes (not Elements)
    final String q4 = "//cache:region/@name";
    final NodeList n4 =XmlUtils.query(root, q4, cacheXPathContext);
    assertEquals(2, n4.getLength());
    assertEquals(Node.ATTRIBUTE_NODE, n4.item(0).getNodeType());
    // This should get none since path is to an attribute.
    try {
      XmlUtils.querySingleElement(root, q4, cacheXPathContext);
      fail("Expected XPathExpressionException");
    } catch (XPathExpressionException e) {
      // ignore
    }

  }

  /**
   * Test method for {@link XmlUtils#changeNamespace(Node, String, String)}.
   * @throws XPathExpressionException 
   * @throws ParserConfigurationException 
   */
  @Test
  public void testChangeNamespace() throws XPathExpressionException, ParserConfigurationException {
    Document doc = XmlUtils.getDocumentBuilder().newDocument();
    Element root = doc.createElement("root");
    root = (Element) doc.appendChild(root);
    final Element child = doc.createElement("child");
    root.appendChild(child);
    final String ns2 = "urn:namespace2";
    final Element childWithNamespace = doc.createElementNS(ns2, "childWithNamespace");
    root.appendChild(childWithNamespace);
    root.appendChild(doc.createTextNode("some text"));

    assertEquals(null, root.getNamespaceURI());
    assertEquals(null, child.getNamespaceURI());
    assertEquals(ns2, childWithNamespace.getNamespaceURI());

    final String ns1 = "urn:namespace1";
    root = (Element) XmlUtils.changeNamespace(root, XMLConstants.NULL_NS_URI, ns1);

    assertEquals(ns1, root.getNamespaceURI());
    assertEquals(ns1, root.getElementsByTagName("child").item(0).getNamespaceURI());
    assertEquals(ns2, root.getElementsByTagName("childWithNamespace").item(0).getNamespaceURI());
  }

}
