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
 *
 */

package org.apache.geode.tools.pulse.internal;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.servlet.ServletContext;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestRule;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.tools.pulse.internal.controllers.PulseController;
import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.tools.pulse.internal.data.PulseVersion;
import org.apache.geode.tools.pulse.internal.data.Repository;

public class PulseAppListenerTest {
  private Repository repository;
  private PulseAppListener appListener;

  @Rule
  public final TestRule restoreSystemProperties = new RestoreSystemProperties();

  ContextRefreshedEvent contextEvent;

  @Before
  public void setUp() {
    System.setProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_EMBEDDED, "true");

    repository = new Repository();

    PulseController pulseController = mock(PulseController.class);
    appListener =
        new PulseAppListener(pulseController, repository, new ClassPathPropertiesFileLoader());
    PulseVersion pulseVersion = new PulseVersion(repository);
    when(pulseController.getPulseVersion()).thenReturn(pulseVersion);

    contextEvent = mock(ContextRefreshedEvent.class);
    WebApplicationContext applicationContext = mock(WebApplicationContext.class);
    when(contextEvent.getApplicationContext()).thenReturn(applicationContext);

    ServletContext servletContext = mock(ServletContext.class);
    when(servletContext.getAttribute(anyString())).thenReturn(null);
    when(applicationContext.getServletContext()).thenReturn(servletContext);
  }

  @Test
  public void embeddedModeDefaultPropertiesRepositoryInitializationTest() {
    appListener.contextInitialized(contextEvent);

    Assert.assertEquals(false, repository.getJmxUseLocator());
    Assert.assertEquals(false, repository.isUseSSLManager());
    Assert.assertEquals(false, repository.isUseSSLLocator());
    Assert.assertEquals(PulseConstants.GEMFIRE_DEFAULT_PORT, repository.getPort());
    Assert.assertEquals(PulseConstants.GEMFIRE_DEFAULT_HOST, repository.getHost());

  }

  @Test
  public void embeddedModeNonDefaultPropertiesRepositoryInitializationTest() {
    System.setProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_PORT, "9999");
    System.setProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_HOST, "nonDefaultBindAddress");
    System.setProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_USESSL_MANAGER,
        Boolean.TRUE.toString());
    System.setProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_USESSL_LOCATOR,
        Boolean.TRUE.toString());

    appListener.contextInitialized(contextEvent);

    Assert.assertEquals(false, repository.getJmxUseLocator());
    Assert.assertEquals(true, repository.isUseSSLManager());
    Assert.assertEquals(true, repository.isUseSSLLocator());
    Assert.assertEquals("9999", repository.getPort());
    Assert.assertEquals("nonDefaultBindAddress", repository.getHost());
  }

  @After
  public void tearDown() {
    if (repository != null) {
      repository.setPort(null);
      repository.setHost(null);
      repository.setJmxUseLocator(false);
      repository.setUseSSLManager(false);
      repository.removeAllClusters();
    }
  }
}
