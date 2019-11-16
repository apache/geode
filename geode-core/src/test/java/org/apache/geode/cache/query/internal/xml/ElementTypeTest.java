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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Stack;

import org.junit.Before;
import org.junit.Test;
import org.xml.sax.Attributes;

import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;

public class ElementTypeTest {
  private CacheCreation cacheCreation;
  private QueryConfigurationServiceCreation queryConfigurationServiceCreation;
  private Stack<Object> stack;
  private Attributes attributes;

  @Before
  public void setup() {
    cacheCreation = mock(CacheCreation.class);
    queryConfigurationServiceCreation = mock(QueryConfigurationServiceCreation.class);
    attributes = mock(Attributes.class);

    stack = new Stack<>();
  }

  @Test
  public void gettingElementTypeThatDoesNotExistThrowsException() {
    assertThatThrownBy(() -> ElementType.getTypeFromName("non-existent element"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void startElementForQueryServiceThrowsCacheXmlExceptionWhenParentElementIsIncorrect() {
    stack.push("wrongClass");
    assertThatThrownBy(() -> ElementType.QUERY_CONFIG_SERVICE.startElement(stack, attributes))
        .isInstanceOf(
            CacheXmlException.class);
  }

  @Test
  public void endElementForQueryServiceAddsQueryServiceCreationToCacheCreation() {
    stack.push(cacheCreation);
    QueryConfigurationServiceCreation queryConfigurationServiceCreation =
        new QueryConfigurationServiceCreation();
    stack.push(queryConfigurationServiceCreation);

    ElementType.QUERY_CONFIG_SERVICE.endElement(stack);

    verify(cacheCreation).setQueryConfigurationServiceCreation(queryConfigurationServiceCreation);
  }

  @Test
  public void startElementForMethodAuthorizerThrowsCacheXmlExceptionWhenParentElementIsIncorrect() {
    stack.push("wrongClass");
    assertThatThrownBy(() -> ElementType.METHOD_AUTHORIZER.startElement(stack, attributes))
        .isInstanceOf(
            CacheXmlException.class);
  }

  @Test
  public void startElementForMethodAuthorizerAddsClassNameToMethodAuthorizerCreationField() {
    String className = "testClass";
    when(attributes.getValue(QueryServiceXmlParser.CLASS_NAME)).thenReturn(className);

    stack.push(queryConfigurationServiceCreation);

    ElementType.METHOD_AUTHORIZER.startElement(stack, attributes);

    QueryMethodAuthorizerCreation authorizerCreation = (QueryMethodAuthorizerCreation) stack.pop();
    assertThat(authorizerCreation.getClassName()).isEqualTo(className);
  }

  @Test
  public void endElementForMethodAuthorizerAddsMethodAuthorizerCreationToQueryServiceCreation() {
    stack.push(queryConfigurationServiceCreation);
    QueryMethodAuthorizerCreation authorizerCreation = new QueryMethodAuthorizerCreation();
    stack.push(authorizerCreation);

    ElementType.METHOD_AUTHORIZER.endElement(stack);

    verify(queryConfigurationServiceCreation).setMethodAuthorizerCreation(authorizerCreation);
  }

  @Test
  public void startElementForParameterThrowsCacheXmlExceptionWhenParentElementIsIncorrect() {
    stack.push("wrongClass");
    assertThatThrownBy(() -> ElementType.PARAMETER.startElement(stack, attributes)).isInstanceOf(
        CacheXmlException.class);
  }

  @Test
  public void startElementForParameterAddsParameterToMethodAuthorizerCreationField() {
    String parameterValue = "testValue";
    when(attributes.getValue(QueryServiceXmlParser.PARAMETER_VALUE)).thenReturn(parameterValue);

    stack.push(new QueryMethodAuthorizerCreation());

    ElementType.PARAMETER.startElement(stack, attributes);

    QueryMethodAuthorizerCreation authorizerCreation = (QueryMethodAuthorizerCreation) stack.pop();
    assertThat(authorizerCreation.getParameters().size()).isEqualTo(1);
    assertThat(authorizerCreation.getParameters().contains(parameterValue)).isTrue();
  }
}
