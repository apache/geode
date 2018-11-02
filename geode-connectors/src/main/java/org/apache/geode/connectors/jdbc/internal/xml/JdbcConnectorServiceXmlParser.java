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

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import org.apache.geode.internal.cache.xmlcache.AbstractXmlParser;

public class JdbcConnectorServiceXmlParser extends AbstractXmlParser {
  public static final String NAMESPACE = "http://geode.apache.org/schema/jdbc";
  public static final String PARAMS_DELIMITER = ":";
  public static final String NAME = "name";
  static final String URL = "url";
  static final String USER = "user";
  static final String PASSWORD = "password";
  static final String PARAMETERS = "parameters";
  static final String CONNECTION_NAME = "connection-name";
  static final String TABLE = "table";
  static final String PDX_NAME = "pdx-name";
  static final String FIELD_NAME = "field-name";
  static final String COLUMN_NAME = "column-name";

  @Override
  public String getNamespaceUri() {
    return NAMESPACE;
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes)
      throws SAXException {
    if (!NAMESPACE.equals(uri)) {
      return;
    }
    ElementType.getTypeFromName(localName).startElement(stack, attributes);
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    if (!NAMESPACE.equals(uri)) {
      return;
    }
    ElementType.getTypeFromName(localName).endElement(stack);
  }
}
