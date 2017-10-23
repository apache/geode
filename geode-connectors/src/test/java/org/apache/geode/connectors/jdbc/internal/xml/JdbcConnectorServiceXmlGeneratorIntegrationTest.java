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
package org.apache.geode.connectors.jdbc.internal.xml;

import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlGenerator.PREFIX;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.COLUMN_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.CONNECTION_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.FIELD_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.NAMESPACE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PASSWORD;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PDX_CLASS;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PRIMARY_KEY_IN_VALUE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.REGION;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.TABLE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.URL;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class JdbcConnectorServiceXmlGeneratorIntegrationTest {

  private InternalCache cache;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setup() {
    cache = (InternalCache) new CacheFactory().create();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void cacheGetServiceReturnsJdbcConnectorService() {
    assertThat(cache.getService(InternalJdbcConnectorService.class)).isNotNull();
  }

  @Test
  public void serviceWithoutInformationDoesNotPersist() throws Exception {
    cache.getService(InternalJdbcConnectorService.class);
    generateXml();
    Document document = getCacheXmlDocument();
    NodeList elements = getElementsByName(document, ElementType.CONNECTION_SERVICE);
    assertThat(elements.getLength()).isZero();
  }

  @Test
  public void serviceWithConnectionsHasCorrectXml() throws Exception {
    InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);
    ConnectionConfiguration config = new ConnectionConfigBuilder().withName("name").withUrl("url")
        .withUser("username").withPassword("secret").build();
    service.addOrUpdateConnectionConfig(config);

    generateXml();

    Document document = getCacheXmlDocument();
    NodeList serviceElements = getElementsByName(document, ElementType.CONNECTION_SERVICE);
    assertThat(serviceElements.getLength()).isEqualTo(1);

    Element serviceElement = (Element) serviceElements.item(0);
    assertThat(serviceElement.getAttribute("xmlns:" + PREFIX)).isEqualTo(NAMESPACE);

    NodeList connectionElements = getElementsByName(document, ElementType.CONNECTION);
    assertThat(connectionElements.getLength()).isEqualTo(1);

    Element connectionElement = (Element) connectionElements.item(0);
    assertThat(connectionElement.getAttribute(NAME)).isEqualTo("name");
    assertThat(connectionElement.getAttribute(URL)).isEqualTo("url");
    assertThat(connectionElement.getAttribute(USER)).isEqualTo("username");
    assertThat(connectionElement.getAttribute(PASSWORD)).isEqualTo("secret");
  }

  @Test
  public void generatesXmlContainingRegionMapping() throws Exception {
    InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);
    RegionMappingBuilder regionMappingBuilder = new RegionMappingBuilder()
        .withRegionName("regionName").withPdxClassName("pdxClassName").withTableName("tableName")
        .withConnectionConfigName("connectionConfigName").withPrimaryKeyInValue("true");
    regionMappingBuilder.withFieldToColumnMapping("fieldName1", "columnMapping1");
    regionMappingBuilder.withFieldToColumnMapping("fieldName2", "columnMapping2");
    RegionMapping regionMapping = regionMappingBuilder.build();
    service.addOrUpdateRegionMapping(regionMapping);

    generateXml();

    Document document = getCacheXmlDocument();
    NodeList serviceElements = getElementsByName(document, ElementType.CONNECTION_SERVICE);
    assertThat(serviceElements.getLength()).isEqualTo(1);

    NodeList mappingElements = getElementsByName(document, ElementType.REGION_MAPPING);
    assertThat(mappingElements.getLength()).isEqualTo(1);

    Element mappingElement = (Element) mappingElements.item(0);
    assertThat(mappingElement.getAttribute(REGION)).isEqualTo("regionName");
    assertThat(mappingElement.getAttribute(PDX_CLASS)).isEqualTo("pdxClassName");
    assertThat(mappingElement.getAttribute(TABLE)).isEqualTo("tableName");
    assertThat(mappingElement.getAttribute(CONNECTION_NAME)).isEqualTo("connectionConfigName");
    assertThat(mappingElement.getAttribute(PRIMARY_KEY_IN_VALUE)).isEqualTo("true");

    NodeList fieldMappingElements = getElementsByName(mappingElement, ElementType.FIELD_MAPPING);
    assertThat(fieldMappingElements.getLength()).isEqualTo(2);
    validatePresenceOfFieldMapping(fieldMappingElements, "fieldName1", "columnMapping1");
    validatePresenceOfFieldMapping(fieldMappingElements, "fieldName2", "columnMapping2");
  }

  private void validatePresenceOfFieldMapping(NodeList elements, String fieldName,
      String columnName) {
    for (int i = 0; i < elements.getLength(); i++) {
      Element fieldMapping = (Element) elements.item(i);
      if (fieldMapping.getAttribute(FIELD_NAME).equals(fieldName)) {
        assertThat(fieldMapping.getAttribute(COLUMN_NAME)).isEqualTo(columnName);
        return;
      }
    }
    fail("Field name '" + fieldName + "' did not match those provided");
  }

  private NodeList getElementsByName(Document document, ElementType elementType) {
    String name = getTagName(elementType);
    return document.getElementsByTagName(name);
  }

  private NodeList getElementsByName(Element element, ElementType elementType) {
    String name = getTagName(elementType);
    return element.getElementsByTagName(name);
  }

  private String getTagName(ElementType elementType) {
    return PREFIX + ":" + elementType.getTypeName();
  }

  private Document getCacheXmlDocument()
      throws IOException, SAXException, ParserConfigurationException {
    File cacheXml = new File(temporaryFolder.getRoot(), "cache.xml");
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document document = dBuilder.parse(cacheXml);
    document.getDocumentElement().normalize();
    return document;
  }

  private void generateXml() throws IOException {
    File cacheXml = new File(temporaryFolder.getRoot(), "cache.xml");
    PrintWriter printWriter = new PrintWriter(new FileWriter(cacheXml));
    CacheXmlGenerator.generate(cache, printWriter, true, false, false);
    printWriter.flush();
  }

}
