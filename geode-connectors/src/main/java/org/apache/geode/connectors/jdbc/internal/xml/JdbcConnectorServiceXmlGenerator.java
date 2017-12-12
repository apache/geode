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

import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.COLUMN_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.CONNECTION_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.FIELD_NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.NAME;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.NAMESPACE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PARAMETERS;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PARAMS_DELIMITER;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PASSWORD;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PDX_CLASS;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PRIMARY_KEY_IN_VALUE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.REGION;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.TABLE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.URL;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.USER;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import org.apache.geode.cache.Cache;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.internal.cache.xmlcache.XmlGeneratorUtils;

public class JdbcConnectorServiceXmlGenerator implements XmlGenerator<Cache> {

  public static final String PREFIX = "jdbc";

  private static final AttributesImpl EMPTY = new AttributesImpl();

  private final Collection<ConnectionConfiguration> connections;
  private final Collection<RegionMapping> mappings;

  public JdbcConnectorServiceXmlGenerator(Collection<ConnectionConfiguration> connections,
      Collection<RegionMapping> mappings) {
    this.connections = connections != null ? connections : Collections.emptyList();
    this.mappings = mappings != null ? mappings : Collections.emptyList();
  }

  @Override
  public String getNamespaceUri() {
    return NAMESPACE;
  }

  @Override
  public void generate(CacheXmlGenerator cacheXmlGenerator) throws SAXException {
    final ContentHandler handler = cacheXmlGenerator.getContentHandler();

    handler.startPrefixMapping(PREFIX, NAMESPACE);
    XmlGeneratorUtils.startElement(handler, PREFIX, ElementType.CONNECTION_SERVICE.getTypeName(),
        EMPTY);
    for (ConnectionConfiguration connection : connections) {
      outputConnectionConfiguration(handler, connection);
    }
    for (RegionMapping mapping : mappings) {
      outputRegionMapping(handler, mapping);
    }
    XmlGeneratorUtils.endElement(handler, PREFIX, ElementType.CONNECTION_SERVICE.getTypeName());
  }

  /**
   * For testing only
   */
  Collection<ConnectionConfiguration> getConnections() {
    return connections;
  }

  /**
   * For testing only
   */
  Collection<RegionMapping> getMappings() {
    return mappings;
  }

  private void outputConnectionConfiguration(ContentHandler handler, ConnectionConfiguration config)
      throws SAXException {
    AttributesImpl attributes = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(attributes, NAME, config.getName());
    XmlGeneratorUtils.addAttribute(attributes, URL, config.getUrl());
    XmlGeneratorUtils.addAttribute(attributes, USER, config.getUser());
    XmlGeneratorUtils.addAttribute(attributes, PASSWORD, config.getPassword());
    XmlGeneratorUtils.addAttribute(attributes, PARAMETERS, createParametersString(config));
    XmlGeneratorUtils.emptyElement(handler, PREFIX, ElementType.CONNECTION.getTypeName(),
        attributes);
  }

  private void outputRegionMapping(ContentHandler handler, RegionMapping mapping)
      throws SAXException {
    AttributesImpl attributes = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(attributes, CONNECTION_NAME, mapping.getConnectionConfigName());
    XmlGeneratorUtils.addAttribute(attributes, REGION, mapping.getRegionName());
    XmlGeneratorUtils.addAttribute(attributes, TABLE, mapping.getTableName());
    XmlGeneratorUtils.addAttribute(attributes, PDX_CLASS, mapping.getPdxClassName());
    XmlGeneratorUtils.addAttribute(attributes, PRIMARY_KEY_IN_VALUE,
        Boolean.toString(mapping.isPrimaryKeyInValue()));

    XmlGeneratorUtils.startElement(handler, PREFIX, ElementType.REGION_MAPPING.getTypeName(),
        attributes);
    addFieldMappings(handler, mapping.getFieldToColumnMap());
    XmlGeneratorUtils.endElement(handler, PREFIX, ElementType.REGION_MAPPING.getTypeName());
  }

  private void addFieldMappings(ContentHandler handler, Map<String, String> fieldMappings)
      throws SAXException {
    for (Map.Entry<String, String> fieldMapping : fieldMappings.entrySet()) {
      AttributesImpl fieldAttributes = new AttributesImpl();
      XmlGeneratorUtils.addAttribute(fieldAttributes, FIELD_NAME, fieldMapping.getKey());
      XmlGeneratorUtils.addAttribute(fieldAttributes, COLUMN_NAME, fieldMapping.getValue());
      XmlGeneratorUtils.emptyElement(handler, PREFIX, ElementType.FIELD_MAPPING.getTypeName(),
          fieldAttributes);
    }
  }

  private String createParametersString(ConnectionConfiguration config) {
    Properties properties = config.getConnectionProperties();
    StringBuilder stringBuilder = new StringBuilder();
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      Object key = entry.getKey();
      if (!key.equals(USER) && !key.equals(PASSWORD)) {
        if (stringBuilder.length() > 0) {
          stringBuilder.append(",");
        }
        stringBuilder.append(key).append(PARAMS_DELIMITER).append(entry.getValue());
      }
    }
    return stringBuilder.toString();
  }
}
