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

import static org.apache.geode.connectors.jdbc.internal.xml.ElementType.JDBC_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.NAMESPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Stack;

import org.junit.Before;
import org.junit.Test;
import org.xml.sax.Attributes;

import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.xmlcache.RegionCreation;

public class JdbcConnectorServiceXmlParserTest {

  private Attributes attributes;
  private RegionCreation regionCreation;
  private Stack<Object> stack;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
    attributes = mock(Attributes.class);
    regionCreation = mock(RegionCreation.class);
    when(regionCreation.getFullPath()).thenReturn("/region");
    ExtensionPoint<Region<?, ?>> extensionPoint = mock(ExtensionPoint.class);
    when(regionCreation.getExtensionPoint()).thenReturn(extensionPoint);
    stack = new Stack<>();
  }

  @Test
  public void getNamespaceUriReturnsNamespace() {
    assertThat(new JdbcConnectorServiceXmlParser().getNamespaceUri()).isEqualTo(NAMESPACE);
  }

  @Test
  public void startElementCreatesJdbcServiceConfiguration() throws Exception {
    JdbcConnectorServiceXmlParser parser = new JdbcConnectorServiceXmlParser();
    stack.push(regionCreation);
    parser.setStack(stack);

    parser.startElement(NAMESPACE, JDBC_MAPPING.getTypeName(), null, attributes);

    assertThat(stack.pop()).isInstanceOf(RegionMapping.class);
  }

  @Test
  public void startElementWithWrongUriDoesNothing() throws Exception {
    JdbcConnectorServiceXmlParser parser = new JdbcConnectorServiceXmlParser();
    stack.push(regionCreation);
    parser.setStack(stack);

    parser.startElement("wrongNamespace", JDBC_MAPPING.getTypeName(), null, attributes);

    assertThat(stack.pop()).isEqualTo(regionCreation);
  }

  @Test
  public void endElementRemovesJdbcServiceConfiguration() throws Exception {
    JdbcConnectorServiceXmlParser parser = new JdbcConnectorServiceXmlParser();
    stack.push(regionCreation);
    RegionMapping regionMapping = mock(RegionMapping.class);
    stack.push(regionMapping);
    parser.setStack(stack);

    parser.endElement(NAMESPACE, JDBC_MAPPING.getTypeName(), null);

    assertThat(stack.pop()).isEqualTo(regionCreation);
    verifyZeroInteractions(regionMapping);
  }

  @Test
  public void endElementRemovesWithWrongUriDoesNothing() throws Exception {
    JdbcConnectorServiceXmlParser parser = new JdbcConnectorServiceXmlParser();
    stack.push(regionCreation);
    RegionMapping regionMapping = mock(RegionMapping.class);
    stack.push(regionMapping);
    parser.setStack(stack);

    parser.endElement("wrongNamespace", JDBC_MAPPING.getTypeName(), null);

    assertThat(stack.pop()).isEqualTo(regionMapping);
    verifyZeroInteractions(regionMapping);
  }
}
