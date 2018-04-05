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
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
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
      ConnectorService.Connection connection = new ConnectorService.Connection();
      connection.setName(attributes.getValue(JdbcConnectorServiceXmlParser.NAME));
      connection.setUrl(attributes.getValue(JdbcConnectorServiceXmlParser.URL));
      connection.setUser(attributes.getValue(JdbcConnectorServiceXmlParser.USER));
      connection.setPassword(attributes.getValue(JdbcConnectorServiceXmlParser.PASSWORD));
      connection.setParameters(parseParameters(attributes));
      stack.push(connection);
    }

    private String[] parseParameters(Attributes attributes) {
      String[] result = null;
      String value = attributes.getValue(JdbcConnectorServiceXmlParser.PARAMETERS);
      if (value != null) {
        result = value.split(",");
      }
      return result;
    }

    @Override
    void endElement(Stack<Object> stack) {
      ConnectorService.Connection config = (ConnectorService.Connection) stack.pop();
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
      ConnectorService.RegionMapping mapping = new ConnectorService.RegionMapping();
      mapping.setRegionName(attributes.getValue(JdbcConnectorServiceXmlParser.REGION));
      mapping.setConnectionConfigName(
          attributes.getValue(JdbcConnectorServiceXmlParser.CONNECTION_NAME));
      mapping.setTableName(attributes.getValue(JdbcConnectorServiceXmlParser.TABLE));
      mapping.setPdxClassName(attributes.getValue(JdbcConnectorServiceXmlParser.PDX_CLASS));
      mapping.setPrimaryKeyInValue(
          Boolean.valueOf(attributes.getValue(JdbcConnectorServiceXmlParser.PRIMARY_KEY_IN_VALUE)));
      stack.push(mapping);
    }

    @Override
    void endElement(Stack<Object> stack) {
      ConnectorService.RegionMapping mapping = (ConnectorService.RegionMapping) stack.pop();
      JdbcServiceConfiguration connectorService = (JdbcServiceConfiguration) stack.peek();
      connectorService.addRegionMapping(mapping);
    }
  },
  FIELD_MAPPING("field-mapping") {
    @Override
    void startElement(Stack<Object> stack, Attributes attributes) {
      if (!(stack.peek() instanceof ConnectorService.RegionMapping)) {
        throw new CacheXmlException(
            "jdbc <field-mapping> elements must occur within <region-mapping> elements");
      }
      ConnectorService.RegionMapping mapping = (ConnectorService.RegionMapping) stack.peek();
      mapping.getFieldMapping()
          .add(new ConnectorService.RegionMapping.FieldMapping(
              attributes.getValue(JdbcConnectorServiceXmlParser.FIELD_NAME),
              attributes.getValue(JdbcConnectorServiceXmlParser.COLUMN_NAME)));
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
