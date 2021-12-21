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
package org.apache.geode.cache.query.internal.xml;

import static org.apache.geode.cache.query.internal.xml.QueryServiceXmlParser.AUTHORIZER_PARAMETER;
import static org.apache.geode.cache.query.internal.xml.QueryServiceXmlParser.CLASS_NAME;
import static org.apache.geode.cache.query.internal.xml.QueryServiceXmlParser.PARAMETER_VALUE;
import static org.apache.geode.cache.query.internal.xml.QueryServiceXmlParser.PARENT_QUERY_ELEMENT;
import static org.apache.geode.cache.query.internal.xml.QueryServiceXmlParser.PREFIX;
import static org.apache.geode.cache.query.internal.xml.QueryServiceXmlParser.QUERY_METHOD_AUTHORIZER;

import java.util.Stack;

import org.xml.sax.Attributes;

import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;

public enum ElementType {
  QUERY_CONFIG_SERVICE(PARENT_QUERY_ELEMENT) {
    @Override
    void startElement(Stack<Object> stack, Attributes attributes) {
      if (!(stack.peek() instanceof CacheCreation)) {
        throw new CacheXmlException(
            "<" + PREFIX + ":" + PARENT_QUERY_ELEMENT
                + "> elements must occur within <cache> elements");
      }
      QueryConfigurationServiceCreation queryConfigurationServiceCreation =
          new QueryConfigurationServiceCreation();
      stack.push(queryConfigurationServiceCreation);
    }

    @Override
    void endElement(Stack<Object> stack) {
      QueryConfigurationServiceCreation queryConfigurationServiceCreation =
          (QueryConfigurationServiceCreation) stack.pop();
      CacheCreation cacheCreation = (CacheCreation) stack.peek();
      cacheCreation.setQueryConfigurationServiceCreation(queryConfigurationServiceCreation);
    }
  },

  METHOD_AUTHORIZER(QUERY_METHOD_AUTHORIZER) {
    @Override
    void startElement(Stack<Object> stack, Attributes attributes) {
      if (!(stack.peek() instanceof QueryConfigurationServiceCreation)) {
        throw new CacheXmlException(
            "<" + PREFIX + ":" + QUERY_METHOD_AUTHORIZER + "> elements must occur within <" + PREFIX
                + ":" + PARENT_QUERY_ELEMENT + "> elements");
      }
      String className = attributes.getValue(CLASS_NAME);
      QueryMethodAuthorizerCreation methodAuthorizer = new QueryMethodAuthorizerCreation();
      if (className != null) {
        methodAuthorizer.setClassName(className);
      }
      stack.push(methodAuthorizer);
    }

    @Override
    void endElement(Stack<Object> stack) {
      QueryMethodAuthorizerCreation methodAuthorizer = (QueryMethodAuthorizerCreation) stack.pop();
      QueryConfigurationServiceCreation queryConfigurationServiceCreation =
          (QueryConfigurationServiceCreation) stack.peek();
      queryConfigurationServiceCreation.setMethodAuthorizerCreation(methodAuthorizer);
    }
  },

  PARAMETER(AUTHORIZER_PARAMETER) {
    @Override
    void startElement(Stack<Object> stack, Attributes attributes) {
      if (!(stack.peek() instanceof QueryMethodAuthorizerCreation)) {
        throw new CacheXmlException(
            "<" + PREFIX + ":" + AUTHORIZER_PARAMETER + "> elements must occur within <" + PREFIX
                + ":" + QUERY_METHOD_AUTHORIZER + "> elements");
      }
      QueryMethodAuthorizerCreation methodAuthorizer = (QueryMethodAuthorizerCreation) stack.peek();
      String parameter = attributes.getValue(PARAMETER_VALUE);
      if (parameter != null) {
        methodAuthorizer.addSingleParameter(parameter);
      }
    }

    @Override
    void endElement(Stack<Object> stack) {}
  };

  private final String typeName;

  ElementType(String typeName) {
    this.typeName = typeName;
  }

  static ElementType getTypeFromName(String typeName) {
    for (ElementType type : ElementType.values()) {
      if (type.typeName.equals(typeName)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Invalid type '" + typeName + "'");
  }

  public String getTypeName() {
    return typeName;
  }

  abstract void startElement(Stack<Object> stack, Attributes attributes);

  abstract void endElement(Stack<Object> stack);
}
