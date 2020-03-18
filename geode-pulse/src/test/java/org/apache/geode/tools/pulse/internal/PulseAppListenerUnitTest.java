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

import javax.servlet.ServletContext;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.tools.pulse.internal.controllers.PulseController;
import org.apache.geode.tools.pulse.internal.data.PulseVersion;
import org.apache.geode.tools.pulse.internal.data.Repository;

public class PulseAppListenerUnitTest {
  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  @Mock
  private ContextRefreshedEvent contextEvent;

  @Mock
  private ServletContext servletContext;

  @Mock
  private Repository repository;

  @Mock
  private PulseController pulseController;

  @Mock
  private PropertiesFileLoader loadProperties;

  @Mock
  private WebApplicationContext applicationContext;

  private ResourceBundle resourceBundle;
  private PulseVersion pulseVersion;
  private PulseAppListener subject;


  @Before
  public void setUp() {
    pulseVersion = new PulseVersion(repository);
  }

  @Test
  public void contextInitialized_isEmbeddedModeWithoutSslProperties_doesNotSetSslProperties() {
    resourceBundle = new StubResourceBundle();

    when(loadProperties.loadProperties(eq("GemFireVersion.properties"), any()))
        .thenReturn(new Properties());
    when(contextEvent.getApplicationContext()).thenReturn(applicationContext);
    when(applicationContext.getServletContext()).thenReturn(servletContext);
    when(repository.getResourceBundle()).thenReturn(resourceBundle);
    when(pulseController.getPulseVersion()).thenReturn(pulseVersion);

    subject = new PulseAppListener(true, loadProperties, pulseController, repository);

    subject.contextInitialized(contextEvent);

    verify(repository, never()).setJavaSslProperties(any());
  }

  @Test
  public void contextInitialized_isEmbeddedModeWithSslProperties_setsProvidedSslProperties() {
    resourceBundle = new StubResourceBundle();

    Properties sslProperties = new Properties();

    when(contextEvent.getApplicationContext()).thenReturn(applicationContext);
    when(applicationContext.getServletContext()).thenReturn(servletContext);
    when(loadProperties.loadProperties(eq("GemFireVersion.properties"), any()))
        .thenReturn(new Properties());
    when(repository.getResourceBundle()).thenReturn(resourceBundle);
    when(servletContext.getAttribute("org.apache.geode.sslConfig")).thenReturn(sslProperties);
    when(pulseController.getPulseVersion()).thenReturn(pulseVersion);

    subject = new PulseAppListener(true, loadProperties, pulseController, repository);

    subject.contextInitialized(contextEvent);

    verify(repository).setJavaSslProperties(sslProperties);
  }

  @Test
  public void contextInitialized_isNonEmbeddedModeWithoutSslProperties_doesNotSetSslProperties() {
    resourceBundle = new StubResourceBundle();

    when(repository.getResourceBundle()).thenReturn(resourceBundle);
    when(loadProperties.loadProperties(anyString(), any())).thenReturn(new Properties());
    when(pulseController.getPulseVersion()).thenReturn(pulseVersion);

    subject = new PulseAppListener(false, loadProperties, pulseController, repository);

    subject.contextInitialized(contextEvent);

    verify(repository, never()).setJavaSslProperties(any());
  }

  @Test
  public void contextInitialized_isNonEmbeddedModeWithSslProperties_setsProvidedSslProperties() {
    resourceBundle = new StubResourceBundle();

    Properties sslProperties = new Properties();
    sslProperties.put("foo", "bar");

    when(repository.getResourceBundle()).thenReturn(resourceBundle);
    when(loadProperties.loadProperties(eq("pulse.properties"), any())).thenReturn(new Properties());
    when(loadProperties.loadProperties(eq("pulsesecurity.properties"), any()))
        .thenReturn(sslProperties);
    when(loadProperties.loadProperties(eq("GemFireVersion.properties"), any()))
        .thenReturn(new Properties());
    when(pulseController.getPulseVersion()).thenReturn(pulseVersion);

    subject = new PulseAppListener(false, loadProperties, pulseController, repository);

    subject.contextInitialized(contextEvent);

    verify(repository).setJavaSslProperties(sslProperties);
  }

  static class StubResourceBundle extends ResourceBundle {
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
