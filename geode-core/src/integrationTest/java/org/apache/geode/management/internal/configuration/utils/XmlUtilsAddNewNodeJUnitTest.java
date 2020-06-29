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
package org.apache.geode.management.internal.configuration.utils;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.configuration.utils.XmlUtils.XPathContext;
import org.apache.geode.services.module.ModuleService;

/**
 * Unit tests for {@link XmlUtils#addNewNode(Document, XmlEntity, ModuleService)} and
 * {@link XmlUtils#deleteNode(Document, XmlEntity)}. Simulates the
 * {@link InternalConfigurationPersistenceService} method of extracting {@link XmlEntity} from the
 * new
 * config
 * and applying it to the current shared config.
 *
 *
 * @since GemFire 8.1
 */
public class XmlUtilsAddNewNodeJUnitTest {

  private static final String TEST_PREFIX = "test";
  private static final String TEST_NAMESPACE =
      "urn:java:org/apache/geode/management/internal/configuration/utils/XmlUtilsAddNewNodeJUnitTest";
  private static final XPathContext xPathContext = new XPathContext();

  private static Cache cache;

  private Document config;

  @BeforeClass
  public static void beforeClass() {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    xPathContext.addNamespace(CacheXml.PREFIX, CacheXml.GEODE_NAMESPACE);
    xPathContext.addNamespace(TEST_PREFIX, TEST_NAMESPACE);
  }

  @Before
  public void before() throws SAXException, ParserConfigurationException, IOException {
    config = XmlUtils.createDocumentFromReader(new InputStreamReader(
        this.getClass().getResourceAsStream("XmlUtilsAddNewNodeJUnitTest.xml")));
  }

  @AfterClass
  public static void afterClass() {
    cache.close();
    cache = null;
  }

  /**
   * Tests {@link XmlUtils#addNewNode(Document, XmlEntity, ModuleService)} with {@link CacheXml}
   * element with a
   * <code>name</code> attribute, <code>region</code>. It should be added after other
   * <code>region</code> elements.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testAddNewNodeNewNamed() throws Exception {
    final String xPath = "/cache:cache/cache:region[@name='r3']";
    NodeList nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(0, nodes.getLength());

    final Document changes = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass()
        .getResourceAsStream("XmlUtilsAddNewNodeJUnitTest.testAddNewNodeNewNamed.xml")));
    nodes = XmlUtils.query(changes, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    assertEquals(CacheXml.GEODE_NAMESPACE, element.getNamespaceURI());

    final XmlEntity xmlEntity = XmlEntity.builder().withType("region").withAttribute("name", "r3")
        .withConfig(changes).build();
    XmlUtils.addNewNode(config, xmlEntity, ModuleService.DEFAULT);

    nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    element = (Element) nodes.item(0);
    assertEquals(CacheXml.GEODE_NAMESPACE, element.getNamespaceURI());

    final List<Node> childNodes = getElementNodes(config.getFirstChild().getChildNodes());

    assertEquals("r2", childNodes.get(4).getAttributes().getNamedItem("name").getNodeValue());
    assertEquals("r3", childNodes.get(5).getAttributes().getNamedItem("name").getNodeValue());
    assertEquals("test:cache", childNodes.get(6).getNodeName());
  }

  /**
   * Return just the nodes from a nodelist that are of type element.
   */
  private List<Node> getElementNodes(NodeList nodes) {
    ArrayList<Node> result = new ArrayList<Node>();
    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        result.add(node);
      }
    }

    return result;
  }

  /**
   * Tests {@link XmlUtils#addNewNode(Document, XmlEntity, ModuleService)} with {@link CacheXml}
   * element that does
   * not have a name or id attribute, <code>jndi-bindings</code>. It should be added between
   * <code>pdx</code> and <code>region</code> elements.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testAddNewNodeNewUnnamed() throws Exception {
    final String xPath = "/cache:cache/cache:jndi-bindings";
    NodeList nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(0, nodes.getLength());

    final Document changes = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass()
        .getResourceAsStream("XmlUtilsAddNewNodeJUnitTest.testAddNewNodeNewUnnamed.xml")));
    nodes = XmlUtils.query(changes, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    assertEquals(CacheXml.GEODE_NAMESPACE, element.getNamespaceURI());

    final XmlEntity xmlEntity =
        XmlEntity.builder().withType("jndi-bindings").withConfig(changes).build();
    XmlUtils.addNewNode(config, xmlEntity, ModuleService.DEFAULT);

    nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    element = (Element) nodes.item(0);
    assertEquals(CacheXml.GEODE_NAMESPACE, element.getNamespaceURI());

    final List<Node> childElements = getElementNodes(config.getFirstChild().getChildNodes());
    assertEquals("pdx", childElements.get(2).getNodeName());
    assertEquals("jndi-bindings", childElements.get(3).getNodeName());
    assertEquals("region", childElements.get(4).getNodeName());
  }

  /**
   * Tests {@link XmlUtils#addNewNode(Document, XmlEntity, ModuleService)} with an {@link Extension}
   * that does not
   * have a name or id attribute. It should be added to the end of the config xml. Attempts a name
   * collision with test:region, it should not collide with the similarly named cache:region
   * element.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testAddNewNodeNewUnnamedExtension() throws Exception {
    final String xPath = "/cache:cache/test:region";
    NodeList nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(0, nodes.getLength());

    final Document changes = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass()
        .getResourceAsStream("XmlUtilsAddNewNodeJUnitTest.testAddNewNodeNewUnnamedExtension.xml")));
    nodes = XmlUtils.query(changes, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    assertEquals(TEST_NAMESPACE, element.getNamespaceURI());
    assertEquals("test:region", element.getNodeName());

    final XmlEntity xmlEntity = XmlEntity.builder().withType("region")
        .withNamespace(TEST_PREFIX, TEST_NAMESPACE).withConfig(changes).build();
    XmlUtils.addNewNode(config, xmlEntity, ModuleService.DEFAULT);

    nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    element = (Element) nodes.item(0);
    assertEquals(TEST_NAMESPACE, element.getNamespaceURI());
    assertEquals("test:region", element.getNodeName());

    final List<Node> childElements = getElementNodes(config.getFirstChild().getChildNodes());


    assertEquals("test:cache", childElements.get(5).getNodeName());
    assertEquals("test:region", childElements.get(6).getNodeName());
    assertEquals(7, childElements.size());
  }

  /**
   * Tests {@link XmlUtils#addNewNode(Document, XmlEntity, ModuleService)} with {@link CacheXml}
   * element with a
   * <code>name</code> attribute, <code>region</code>. It should replace existing
   * <code>region</code> element with same <code>name</code>.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testAddNewNodeReplaceNamed() throws Exception {
    final String xPath = "/cache:cache/cache:region[@name='r1']";
    NodeList nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    assertEquals(1, getElementNodes(element.getChildNodes()).size());
    assertEquals(CacheXml.GEODE_NAMESPACE, element.getNamespaceURI());

    final Document changes = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass()
        .getResourceAsStream("XmlUtilsAddNewNodeJUnitTest.testAddNewNodeReplaceNamed.xml")));
    nodes = XmlUtils.query(changes, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    element = (Element) nodes.item(0);
    assertEquals(0, element.getChildNodes().getLength());
    assertEquals(CacheXml.GEODE_NAMESPACE, element.getNamespaceURI());

    final XmlEntity xmlEntity = XmlEntity.builder().withType("region").withAttribute("name", "r1")
        .withConfig(changes).build();
    XmlUtils.addNewNode(config, xmlEntity, ModuleService.DEFAULT);

    nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    element = (Element) nodes.item(0);
    assertEquals(0, element.getChildNodes().getLength());
    assertEquals(CacheXml.GEODE_NAMESPACE, element.getNamespaceURI());
  }

  /**
   * Tests {@link XmlUtils#addNewNode(Document, XmlEntity, ModuleService)} with {@link CacheXml}
   * element that does
   * not have a name or id attribute, <code>pdx</code>. It should replace <code>pdx</code> element.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testAddNewNodeReplaceUnnamed() throws Exception {
    final String xPath = "/cache:cache/cache:pdx";
    NodeList nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    assertEquals("foo", XmlUtils.getAttribute(element, "disk-store-name"));
    assertEquals(CacheXml.GEODE_NAMESPACE, element.getNamespaceURI());

    final Document changes = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass()
        .getResourceAsStream("XmlUtilsAddNewNodeJUnitTest.testAddNewNodeReplaceUnnamed.xml")));
    nodes = XmlUtils.query(changes, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    element = (Element) nodes.item(0);
    assertEquals("bar", XmlUtils.getAttribute(element, "disk-store-name"));
    assertEquals(CacheXml.GEODE_NAMESPACE, element.getNamespaceURI());

    final XmlEntity xmlEntity = XmlEntity.builder().withType("pdx").withConfig(changes).build();
    XmlUtils.addNewNode(config, xmlEntity, ModuleService.DEFAULT);

    nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    element = (Element) nodes.item(0);
    assertEquals("bar", XmlUtils.getAttribute(element, "disk-store-name"));
    assertEquals(CacheXml.GEODE_NAMESPACE, element.getNamespaceURI());
  }

  /**
   * Tests {@link XmlUtils#addNewNode(Document, XmlEntity, ModuleService)} with an {@link Extension}
   * that does not
   * have a name or id attribute, <code>test:cache</code>. It should replace the existing
   * <code>test:cache</code> element.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testAddNewNodeReplaceUnnamedExtension() throws Exception {
    final String xPath = "/cache:cache/test:cache";
    NodeList nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    assertEquals("1", XmlUtils.getAttribute(element, "value"));
    assertEquals(TEST_NAMESPACE, element.getNamespaceURI());

    final org.w3c.dom.Document changes =
        XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass().getResourceAsStream(
            "XmlUtilsAddNewNodeJUnitTest.testAddNewNodeReplaceUnnamedExtension.xml")));
    nodes = XmlUtils.query(changes, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    element = (Element) nodes.item(0);
    assertEquals("2", XmlUtils.getAttribute(element, "value"));
    assertEquals(TEST_NAMESPACE, element.getNamespaceURI());

    final XmlEntity xmlEntity = XmlEntity.builder().withType("cache")
        .withNamespace(TEST_PREFIX, TEST_NAMESPACE).withConfig(changes).build();
    XmlUtils.addNewNode(config, xmlEntity, ModuleService.DEFAULT);

    nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    element = (Element) nodes.item(0);
    assertEquals("2", XmlUtils.getAttribute(element, "value"));
    assertEquals(TEST_NAMESPACE, element.getNamespaceURI());
  }

  /**
   * Tests {@link XmlUtils#deleteNode(Document, XmlEntity)} with {@link CacheXml} element with a
   * <code>name</code> attribute, <code>region</code>. It should remove existing <code>region</code>
   * element with same <code>name</code>.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testDeleteNodeNamed() throws Exception {
    final String xPath = "/cache:cache/cache:region[@name='r1']";
    NodeList nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    assertEquals(1, getElementNodes(element.getChildNodes()).size());
    assertEquals(CacheXml.GEODE_NAMESPACE, element.getNamespaceURI());

    final Document changes = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass()
        .getResourceAsStream("XmlUtilsAddNewNodeJUnitTest.testDeleteNodeNamed.xml")));
    nodes = XmlUtils.query(changes, xPath, xPathContext);
    assertEquals(0, nodes.getLength());

    final XmlEntity xmlEntity = XmlEntity.builder().withType("region").withAttribute("name", "r1")
        .withConfig(changes).build();
    XmlUtils.deleteNode(config, xmlEntity);

    nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(0, nodes.getLength());
  }

  /**
   * Tests {@link XmlUtils#addNewNode(Document, XmlEntity, ModuleService)} with {@link CacheXml}
   * element that does
   * not have a name or id attribute, <code>pdx</code>. It should remove the existing
   * <code>pdx</code> element.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testDeleteNodeUnnamed() throws Exception {
    final String xPath = "/cache:cache/cache:pdx";
    NodeList nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    assertEquals("foo", XmlUtils.getAttribute(element, "disk-store-name"));
    assertEquals(CacheXml.GEODE_NAMESPACE, element.getNamespaceURI());

    final Document changes = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass()
        .getResourceAsStream("XmlUtilsAddNewNodeJUnitTest.testDeleteNodeUnnamed.xml")));
    nodes = XmlUtils.query(changes, xPath, xPathContext);
    assertEquals(0, nodes.getLength());

    final XmlEntity xmlEntity = XmlEntity.builder().withType("pdx").withConfig(changes).build();
    XmlUtils.deleteNode(config, xmlEntity);

    nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(0, nodes.getLength());
  }

  /**
   * Tests {@link XmlUtils#addNewNode(Document, XmlEntity, ModuleService)} with an {@link Extension}
   * that does not
   * have a name or id attribute, <code>test:cache</code>. It should remove the existing
   * <code>test:cache</code> element.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testDeleteNodeUnnamedExtension() throws Exception {
    final String xPath = "/cache:cache/test:cache";
    NodeList nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    assertEquals("1", XmlUtils.getAttribute(element, "value"));
    assertEquals(TEST_NAMESPACE, element.getNamespaceURI());

    final Document changes = XmlUtils.createDocumentFromReader(new InputStreamReader(this.getClass()
        .getResourceAsStream("XmlUtilsAddNewNodeJUnitTest.testDeleteNodeUnnamedExtension.xml")));
    nodes = XmlUtils.query(changes, xPath, xPathContext);
    assertEquals(0, nodes.getLength());

    final XmlEntity xmlEntity = XmlEntity.builder().withType("cache")
        .withNamespace(TEST_PREFIX, TEST_NAMESPACE).withConfig(changes).build();
    XmlUtils.deleteNode(config, xmlEntity);

    nodes = XmlUtils.query(config, xPath, xPathContext);
    assertEquals(0, nodes.getLength());
  }

}
