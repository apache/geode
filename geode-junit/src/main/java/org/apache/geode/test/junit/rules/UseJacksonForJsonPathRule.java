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
package org.apache.geode.test.junit.rules;

import java.util.EnumSet;
import java.util.Set;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Configuration.Defaults;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

/**
 * JUnit Rule that configures json-path to use the {@code JacksonJsonProvider}
 *
 * <p>
 * UseJacksonForJsonPathRule can be used in tests that need to use json-path-assert:
 * 
 * <pre>
 * {@literal @}ClassRule
 * public static UseJacksonForJsonPathRule useJacksonForJsonPathRule = new UseJacksonForJsonPathRule();
 *
 * {@literal @}Test
 * public void hasAssertionsUsingJsonPathMatchers() {
 *   ...
 *   assertThat(json, isJson());
 *   assertThat(json, hasJsonPath("$.result"));
 * }
 * </pre>
 */
@SuppressWarnings({"serial", "unused"})
public class UseJacksonForJsonPathRule extends SerializableExternalResource {

  private boolean hadDefaults;
  private JsonProvider jsonProvider;
  private MappingProvider mappingProvider;
  private Set<Option> options;

  /**
   * Override to set up your specific external resource.
   */
  @Override
  public void before() {
    saveDefaults();
    Configuration.setDefaults(new Defaults() {

      private final JsonProvider jsonProvider = new JacksonJsonProvider();
      private final MappingProvider mappingProvider = new JacksonMappingProvider();

      @Override
      public JsonProvider jsonProvider() {
        return jsonProvider;
      }

      @Override
      public MappingProvider mappingProvider() {
        return mappingProvider;
      }

      @Override
      public Set<Option> options() {
        return EnumSet.noneOf(Option.class);
      }

    });
  }

  /**
   * Override to tear down your specific external resource.
   */
  @Override
  public void after() {
    restoreDefaults();
  }

  private void saveDefaults() {
    try {
      Configuration defaultConfiguration = Configuration.defaultConfiguration();
      this.jsonProvider = defaultConfiguration.jsonProvider();
      this.mappingProvider = defaultConfiguration.mappingProvider();
      this.options = defaultConfiguration.getOptions();
      this.hadDefaults = true;
    } catch (NoClassDefFoundError ignore) {
      this.hadDefaults = false;
    }
  }

  private void restoreDefaults() {
    if (!this.hadDefaults) {
      return;
    }
    Configuration.setDefaults(new Defaults() {

      @Override
      public JsonProvider jsonProvider() {
        return jsonProvider;
      }

      @Override
      public MappingProvider mappingProvider() {
        return mappingProvider;
      }

      @Override
      public Set<Option> options() {
        return options;
      }

    });
  }
}
