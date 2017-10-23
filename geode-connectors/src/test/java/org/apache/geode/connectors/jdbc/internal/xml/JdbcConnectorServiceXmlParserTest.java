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

import static org.apache.geode.connectors.jdbc.internal.xml.ElementType.CONNECTION_SERVICE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.NAMESPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Stack;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.xml.sax.Attributes;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class JdbcConnectorServiceXmlParserTest {

  private Stack<Object> stack = new Stack<>();
  private Attributes attributes;
  private CacheCreation cacheCreation;
  private ExtensionPoint<Cache> extensionPoint;

  @Before
  public void setup() {
    attributes = mock(Attributes.class);
    cacheCreation = mock(CacheCreation.class);
    extensionPoint = mock(ExtensionPoint.class);
    when(cacheCreation.getExtensionPoint()).thenReturn(extensionPoint);
  }

  @Test
  public void getNamespaceUriReturnsNamespace() {
    assertThat(new JdbcConnectorServiceXmlParser().getNamespaceUri()).isEqualTo(NAMESPACE);
  }

  @Test
  public void startElementCreatesJdbcServiceConfiguration() throws Exception {
    JdbcConnectorServiceXmlParser parser = new JdbcConnectorServiceXmlParser();
    stack.push(cacheCreation);
    parser.setStack(stack);

    parser.startElement(NAMESPACE, CONNECTION_SERVICE.getTypeName(), null, attributes);

    assertThat(stack.pop()).isInstanceOf(JdbcServiceConfiguration.class);
  }

  @Test
  public void startElementWithWrongUriDoesNothing() throws Exception {
    JdbcConnectorServiceXmlParser parser = new JdbcConnectorServiceXmlParser();
    stack.push(cacheCreation);
    parser.setStack(stack);

    parser.startElement("wrongNamespace", CONNECTION_SERVICE.getTypeName(), null, attributes);

    assertThat(stack.pop()).isEqualTo(cacheCreation);
  }

  @Test
  public void endElementRemovesJdbcServiceConfiguration() throws Exception {
    JdbcConnectorServiceXmlParser parser = new JdbcConnectorServiceXmlParser();
    stack.push(cacheCreation);
    JdbcServiceConfiguration serviceConfiguration = mock(JdbcServiceConfiguration.class);
    stack.push(serviceConfiguration);
    parser.setStack(stack);

    parser.endElement(NAMESPACE, CONNECTION_SERVICE.getTypeName(), null);

    assertThat(stack.pop()).isEqualTo(cacheCreation);
    verifyZeroInteractions(serviceConfiguration);
  }

  @Test
  public void endElementRemovesWithWrongUriDoesNothing() throws Exception {
    JdbcConnectorServiceXmlParser parser = new JdbcConnectorServiceXmlParser();
    stack.push(cacheCreation);
    JdbcServiceConfiguration serviceConfiguration = mock(JdbcServiceConfiguration.class);
    stack.push(serviceConfiguration);
    parser.setStack(stack);

    parser.endElement("wrongNamespace", CONNECTION_SERVICE.getTypeName(), null);

    assertThat(stack.pop()).isEqualTo(serviceConfiguration);
    verifyZeroInteractions(serviceConfiguration);
  }
}
