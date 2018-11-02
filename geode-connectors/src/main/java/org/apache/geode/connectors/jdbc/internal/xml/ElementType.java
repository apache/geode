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
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.xmlcache.RegionCreation;

public enum ElementType {
  JDBC_MAPPING("mapping") {
    @Override
    void startElement(Stack<Object> stack, Attributes attributes) {
      if (!(stack.peek() instanceof RegionCreation)) {
        throw new CacheXmlException(
            "<jdbc:mapping> elements must occur within <region> elements");
      }
      RegionCreation regionCreation = (RegionCreation) stack.peek();
      RegionMapping mapping = new RegionMapping();
      mapping.setRegionName(regionCreation.getFullPath().substring(1));
      mapping.setConnectionConfigName(
          attributes.getValue(JdbcConnectorServiceXmlParser.CONNECTION_NAME));
      mapping.setTableName(attributes.getValue(JdbcConnectorServiceXmlParser.TABLE));
      mapping.setPdxName(attributes.getValue(JdbcConnectorServiceXmlParser.PDX_NAME));
      stack.push(mapping);
    }

    @Override
    void endElement(Stack<Object> stack) {
      RegionMapping mapping = (RegionMapping) stack.pop();
      RegionCreation regionCreation = (RegionCreation) stack.peek();
      regionCreation.getExtensionPoint().addExtension(new RegionMappingConfiguration(mapping));
    }
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
