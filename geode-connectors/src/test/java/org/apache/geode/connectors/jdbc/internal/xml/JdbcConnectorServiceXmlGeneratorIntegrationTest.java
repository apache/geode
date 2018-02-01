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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
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
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.connectors.jdbc.internal.RegionMappingBuilder;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class JdbcConnectorServiceXmlGeneratorIntegrationTest {

  private InternalCache cache;
  private File cacheXmlFile;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0").create();
    cacheXmlFile = temporaryFolder.newFile("cache.xml");
  }

  @After
  public void tearDown() {
    cache.close();
    cache = null;
  }

  @Test
  public void cacheGetServiceReturnsJdbcConnectorService() {
    assertThat(cache.getService(JdbcConnectorService.class)).isNotNull();
  }

  @Test
  public void serviceWithoutInformationDoesNotPersist() throws Exception {
    cache.getService(JdbcConnectorService.class);

    generateXml();

    Document document = getCacheXmlDocument();
    NodeList elements = getElementsByName(document, ElementType.CONNECTION_SERVICE);
    assertThat(elements.getLength()).isZero();
  }

  @Test
  public void serviceWithConnectionsHasCorrectXml() throws Exception {
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    ConnectionConfiguration config = new ConnectionConfigBuilder().withName("name").withUrl("url")
        .withUser("username").withPassword("secret").build();
    service.createConnectionConfig(config);

    generateXml();

    Document document = getCacheXmlDocument();
    NodeList serviceElements = getElementsByName(document, ElementType.CONNECTION_SERVICE);
    assertThat(serviceElements.getLength()).isEqualTo(1);

    Element serviceElement = (Element) serviceElements.item(0);
    assertThat(serviceElement.getAttribute("xmlns:" + PREFIX)).isEqualTo(NAMESPACE);
    assertThat(serviceElement.getAttribute(NAME))
        .isEqualTo(ElementType.CONNECTION_SERVICE.getTypeName());

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
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    RegionMappingBuilder regionMappingBuilder = new RegionMappingBuilder()
        .withRegionName("regionName").withPdxClassName("pdxClassName").withTableName("tableName")
        .withConnectionConfigName("connectionConfigName").withPrimaryKeyInValue("true");
    regionMappingBuilder.withFieldToColumnMapping("fieldName1", "columnMapping1");
    regionMappingBuilder.withFieldToColumnMapping("fieldName2", "columnMapping2");
    RegionMapping regionMapping = regionMappingBuilder.build();
    service.createRegionMapping(regionMapping);

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

  @Test
  public void generatedXmlWithConnectionConfigurationCanBeParsed() throws Exception {
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    ConnectionConfiguration config = new ConnectionConfigBuilder().withName("name").withUrl("url")
        .withUser("username").withPassword("secret").build();
    service.createConnectionConfig(config);
    generateXml();
    cache.close();

    createCacheUsingXml();
    service = cache.getService(JdbcConnectorService.class);
    assertThat(service.getConnectionConfig("name")).isEqualTo(config);
  }

  @Test
  public void generatedXmlWithConnectionConfigurationWithNoUserNameAndPasswordCanBeParsed()
      throws Exception {
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    ConnectionConfiguration config =
        new ConnectionConfigBuilder().withName("name").withUrl("url").build();
    service.createConnectionConfig(config);
    generateXml();
    cache.close();

    createCacheUsingXml();
    service = cache.getService(JdbcConnectorService.class);
    assertThat(service.getConnectionConfig("name")).isEqualTo(config);
  }

  @Test
  public void generatedXmlWithConnectionConfigurationWithParametersCanBeParsed() throws Exception {
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    ConnectionConfiguration config = new ConnectionConfigBuilder().withName("name").withUrl("url")
        .withParameters(new String[] {"key1:value1", "key2:value2"}).build();
    service.createConnectionConfig(config);
    generateXml();
    cache.close();

    createCacheUsingXml();
    service = cache.getService(JdbcConnectorService.class);
    assertThat(service.getConnectionConfig("name")).isEqualTo(config);
  }

  @Test
  public void generatedXmlWithRegionMappingCanBeParsed() throws Exception {
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    RegionMapping mapping = new RegionMappingBuilder().withRegionName("region")
        .withPdxClassName("class").withTableName("table").withConnectionConfigName("connection")
        .withPrimaryKeyInValue(true).withFieldToColumnMapping("field1", "columnMapping1")
        .withFieldToColumnMapping("field2", "columnMapping2").build();
    service.createRegionMapping(mapping);
    generateXml();
    cache.close();

    createCacheUsingXml();
    service = cache.getService(JdbcConnectorService.class);
    assertThat(service.getMappingForRegion("region")).isEqualTo(mapping);
  }

  @Test
  public void generatedXmlWithRegionMappingWithNoOptionalParametersCanBeParsed() throws Exception {
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    RegionMapping mapping = new RegionMappingBuilder().withRegionName("region")
        .withConnectionConfigName("connection").build();
    service.createRegionMapping(mapping);
    generateXml();
    cache.close();

    createCacheUsingXml();
    service = cache.getService(JdbcConnectorService.class);
    assertThat(service.getMappingForRegion("region")).isEqualTo(mapping);
  }

  @Test
  public void generatedXmlWithEverythingCanBeParsed() throws Exception {
    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    ConnectionConfiguration config = new ConnectionConfigBuilder().withName("name").withUrl("url")
        .withUser("username").withPassword("secret").build();
    service.createConnectionConfig(config);
    RegionMapping mapping = new RegionMappingBuilder().withRegionName("region")
        .withPdxClassName("class").withTableName("table").withConnectionConfigName("connection")
        .withPrimaryKeyInValue(true).withFieldToColumnMapping("field1", "columnMapping1")
        .withFieldToColumnMapping("field2", "columnMapping2").build();
    service.createRegionMapping(mapping);
    generateXml();
    cache.close();

    createCacheUsingXml();
    service = cache.getService(JdbcConnectorService.class);
    assertThat(service.getConnectionConfig("name")).isEqualTo(config);
    assertThat(service.getMappingForRegion("region")).isEqualTo(mapping);
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
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    dbFactory.setNamespaceAware(false);
    dbFactory.setValidating(false);
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document document = dBuilder.parse(cacheXmlFile);
    document.getDocumentElement().normalize();
    return document;
  }

  private void generateXml() throws IOException {
    PrintWriter printWriter = new PrintWriter(new FileWriter(cacheXmlFile));
    CacheXmlGenerator.generate(cache, printWriter, true, false, false);
    printWriter.flush();
  }

  private void createCacheUsingXml() throws IOException {
    byte[] bytes = FileUtils.readFileToByteArray(cacheXmlFile);
    cache = (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0").create();
    cache.loadCacheXml(new ByteArrayInputStream(bytes));
  }
}
