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

import org.xml.sax.Attributes;

import org.apache.geode.internal.cache.xmlcache.AbstractXmlParser;

public class QueryServiceXmlParser extends AbstractXmlParser {
  public static final String NAMESPACE = "http://geode.apache.org/schema/query-service";
  static final String PREFIX = "query-service";

  static final String PARENT_QUERY_ELEMENT = "query-service";
  static final String QUERY_METHOD_AUTHORIZER = "method-authorizer";
  static final String AUTHORIZER_PARAMETER = "parameter";

  static final String CLASS_NAME = "class-name";
  static final String PARAMETER_VALUE = "parameter-value";

  @Override
  public String getNamespaceUri() {
    return NAMESPACE;
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) {
    if (!NAMESPACE.equals(uri)) {
      return;
    }
    ElementType.getTypeFromName(localName).startElement(stack, attributes);
  }

  @Override
  public void endElement(String uri, String localName, String qName) {
    if (!NAMESPACE.equals(uri)) {
      return;
    }
    ElementType.getTypeFromName(localName).endElement(stack);
  }
}
