/*
 *
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
 *
 */

package org.apache.geode.tools.pulse.internal;

import static java.util.Collections.emptyEnumeration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Enumeration;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.function.BiFunction;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.tools.pulse.internal.data.Repository;

public class PulseAppListenerUnitTest {

  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  private PulseAppListener subject;

  @Mock
  private ServletContextEvent contextEvent;

  @Mock
  private ServletContext servletContext;

  @Mock
  private Repository repository;

  @Mock
  private BiFunction<String, ResourceBundle, Properties> loadProperties;

  private ResourceBundle resourceBundle;

  @Before
  public void setUp() {
    when(loadProperties.apply(eq("GemFireVersion.properties"), any())).thenReturn(new Properties());
  }

  @Test
  public void contextInitialized_isEmbeddedModeWithoutSslProperties_doesNotSetSslProperties() {
    resourceBundle = new StubResourceBundle();

    when(contextEvent.getServletContext()).thenReturn(servletContext);
    when(repository.getResourceBundle()).thenReturn(resourceBundle);

    subject = new PulseAppListener(true, repository, loadProperties);

    subject.contextInitialized(contextEvent);

    verify(repository, never()).setJavaSslProperties(any());
  }

  @Test
  public void contextInitialized_isEmbeddedModeWithSslProperties_setsProvidedSslProperties() {
    resourceBundle = new StubResourceBundle();

    Properties sslProperties = new Properties();

    when(contextEvent.getServletContext()).thenReturn(servletContext);
    when(repository.getResourceBundle()).thenReturn(resourceBundle);
    when(servletContext.getAttribute("org.apache.geode.sslConfig")).thenReturn(sslProperties);

    subject = new PulseAppListener(true, repository, loadProperties);

    subject.contextInitialized(contextEvent);

    verify(repository).setJavaSslProperties(sslProperties);
  }

  @Test
  public void contextInitialized_isNonEmbeddedModeWithoutSslProperties_doesNotSetSslProperties() {
    resourceBundle = new StubResourceBundle();

    when(repository.getResourceBundle()).thenReturn(resourceBundle);
    when(loadProperties.apply(anyString(), any())).thenReturn(new Properties());

    subject = new PulseAppListener(false, repository, loadProperties);

    subject.contextInitialized(contextEvent);

    verify(repository, never()).setJavaSslProperties(any());
  }

  @Test
  public void contextInitialized_isNonEmbeddedModeWithSslProperties_setsProvidedSslProperties() {
    resourceBundle = new StubResourceBundle();

    Properties sslProperties = new Properties();
    sslProperties.put("foo", "bar");

    when(repository.getResourceBundle()).thenReturn(resourceBundle);
    when(loadProperties.apply(eq("pulse.properties"), any())).thenReturn(new Properties());
    when(loadProperties.apply(eq("pulsesecurity.properties"), any())).thenReturn(sslProperties);

    subject = new PulseAppListener(false, repository, loadProperties);

    subject.contextInitialized(contextEvent);

    verify(repository).setJavaSslProperties(sslProperties);
  }

  class StubResourceBundle extends ResourceBundle {

    @Override
    protected Object handleGetObject(String key) {
      return "the same string";
    }

    @Override
    public Enumeration<String> getKeys() {
      return emptyEnumeration();
    }
  }
}
