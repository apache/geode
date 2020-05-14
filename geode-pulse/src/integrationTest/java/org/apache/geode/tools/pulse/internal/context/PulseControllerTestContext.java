/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geode.tools.pulse.internal.context;

import static java.util.Collections.enumeration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import org.apache.geode.tools.pulse.internal.PropertiesFileLoader;
import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.tools.pulse.internal.data.Repository;

@Configuration
@Profile("pulse.controller.test")
public class PulseControllerTestContext {

  @Bean
  public Repository repository() {
    Repository repository = mock(Repository.class);
    ResourceBundle resourceBundle = getResourceBundle();
    when(repository.getResourceBundle()).thenReturn(resourceBundle);
    return repository;
  }

  @Bean(name = "logoutTargetURL")
  public String defaultLogoutTargetURL() {
    return "/login.html";
  }

  @Bean
  public PropertiesFileLoader propertiesLoader() {
    PropertiesFileLoader propertiesFileLoader = mock(PropertiesFileLoader.class);
    Properties properties = new Properties();
    properties.setProperty(PulseConstants.PROPERTY_BUILD_DATE, "not empty");
    properties.setProperty(PulseConstants.PROPERTY_BUILD_ID, "not empty");
    properties.setProperty(PulseConstants.PROPERTY_PULSE_VERSION, "not empty");
    properties.setProperty(PulseConstants.PROPERTY_SOURCE_DATE, "not empty");
    properties.setProperty(PulseConstants.PROPERTY_SOURCE_REPOSITORY, "not empty");
    properties.setProperty(PulseConstants.PROPERTY_SOURCE_REVISION, "not empty");

    when(propertiesFileLoader.loadProperties(any(), any())).thenReturn(properties);

    return propertiesFileLoader;
  }

  private ResourceBundle getResourceBundle() {
    return new ResourceBundle() {
      Map<String, Object> objects = new HashMap<>();

      @Override
      protected Object handleGetObject(String key) {
        objects.put(key, key);
        return key;
      }

      @Override
      public Enumeration<String> getKeys() {
        return enumeration(objects.keySet());
      }
    };
  }
}
