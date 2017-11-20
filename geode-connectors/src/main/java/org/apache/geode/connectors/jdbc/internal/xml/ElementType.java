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

import java.util.Stack;

import org.xml.sax.Attributes;

import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;

public enum ElementType {
  CONNECTION_SERVICE("connector-service") {
    @Override
    void startElement(Stack<Object> stack, Attributes attributes) {
      if (!(stack.peek() instanceof CacheCreation)) {
        throw new CacheXmlException(
            "jdbc <connector-service> elements must occur within <cache> elements");
      }
      CacheCreation cacheCreation = (CacheCreation) stack.peek();
      JdbcServiceConfiguration serviceConfig = new JdbcServiceConfiguration();
      cacheCreation.getExtensionPoint().addExtension(serviceConfig);
      stack.push(serviceConfig);
    }

    @Override
    void endElement(Stack<Object> stack) {
      stack.pop();
    }
  },
  CONNECTION("connection") {
    @Override
    void startElement(Stack<Object> stack, Attributes attributes) {
      if (!(stack.peek() instanceof JdbcServiceConfiguration)) {
        throw new CacheXmlException(
            "jdbc <connection> elements must occur within <connector-service> elements");
      }
      ConnectionConfigBuilder connectionConfig = new ConnectionConfigBuilder()
          .withName(attributes.getValue(JdbcConnectorServiceXmlParser.NAME))
          .withUrl(attributes.getValue(JdbcConnectorServiceXmlParser.URL))
          .withUser(attributes.getValue(JdbcConnectorServiceXmlParser.USER))
          .withPassword(attributes.getValue(JdbcConnectorServiceXmlParser.PASSWORD));
      addParameters(connectionConfig,
          attributes.getValue(JdbcConnectorServiceXmlParser.PARAMETERS));
      stack.push(connectionConfig);
    }

    private void addParameters(ConnectionConfigBuilder connectionConfig, String value) {
      if (value == null) {
        return;
      }
      connectionConfig.withParameters(value.split(","));
    }

    @Override
    void endElement(Stack<Object> stack) {
      ConnectionConfiguration config = ((ConnectionConfigBuilder) stack.pop()).build();
      JdbcServiceConfiguration connectorService = (JdbcServiceConfiguration) stack.peek();
      connectorService.addConnectionConfig(config);
    }
  },
  REGION_MAPPING("region-mapping") {
    @Override
    void startElement(Stack<Object> stack, Attributes attributes) {
      if (!(stack.peek() instanceof JdbcServiceConfiguration)) {
        throw new CacheXmlException(
            "jdbc <region-mapping> elements must occur within <connector-service> elements");
      }
      RegionMappingBuilder mapping = new RegionMappingBuilder()
          .withRegionName(attributes.getValue(JdbcConnectorServiceXmlParser.REGION))
          .withConnectionConfigName(
              attributes.getValue(JdbcConnectorServiceXmlParser.CONNECTION_NAME))
          .withTableName(attributes.getValue(JdbcConnectorServiceXmlParser.TABLE))
          .withPdxClassName(attributes.getValue(JdbcConnectorServiceXmlParser.PDX_CLASS))
          .withPrimaryKeyInValue(
              attributes.getValue(JdbcConnectorServiceXmlParser.PRIMARY_KEY_IN_VALUE));
      stack.push(mapping);
    }

    @Override
    void endElement(Stack<Object> stack) {
      RegionMapping mapping = ((RegionMappingBuilder) stack.pop()).build();
      JdbcServiceConfiguration connectorService = (JdbcServiceConfiguration) stack.peek();
      connectorService.addRegionMapping(mapping);
    }
  },
  FIELD_MAPPING("field-mapping") {
    @Override
    void startElement(Stack<Object> stack, Attributes attributes) {
      if (!(stack.peek() instanceof RegionMappingBuilder)) {
        throw new CacheXmlException(
            "jdbc <field-mapping> elements must occur within <region-mapping> elements");
      }
      RegionMappingBuilder mapping = (RegionMappingBuilder) stack.peek();
      String fieldName = attributes.getValue(JdbcConnectorServiceXmlParser.FIELD_NAME);
      String columnName = attributes.getValue(JdbcConnectorServiceXmlParser.COLUMN_NAME);
      mapping.withFieldToColumnMapping(fieldName, columnName);
    }

    @Override
    void endElement(Stack<Object> stack) {}
  };

  private String typeName;

  ElementType(String typeName) {
    this.typeName = typeName;
  }

  static ElementType getTypeFromName(String typeName) {
    for (ElementType type : ElementType.values()) {
      if (type.typeName.equals(typeName))
        return type;
    }
    throw new IllegalArgumentException("Invalid type '" + typeName + "'");
  }

  public String getTypeName() {
    return typeName;
  }

  abstract void startElement(Stack<Object> stack, Attributes attributes);

  abstract void endElement(Stack<Object> stack);

}
