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

import static org.apache.geode.test.junit.runners.TestRunner.runTestWithValidation;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicReference;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link UseJacksonForJsonPathRule}.
 */
public class UseJacksonForJsonPathRuleTest {

  private static final AtomicReference<JsonProvider> jsonProviderRef = new AtomicReference<>();
  private static final AtomicReference<MappingProvider> mappingProviderRef =
      new AtomicReference<>();

  private JsonProvider defaultJsonProvider;
  private MappingProvider defaultMappingProvider;

  @Before
  public void setUp() {
    Configuration configuration = Configuration.defaultConfiguration();
    defaultJsonProvider = configuration.jsonProvider();
    defaultMappingProvider = configuration.mappingProvider();
  }

  @After
  public void tearDown() {
    jsonProviderRef.set(null);
    mappingProviderRef.set(null);
  }

  @Test
  public void setsJsonProviderToJacksonJsonProviderBeforeTest() {
    runTestWithValidation(CaptureJsonProvider.class);

    assertThat(jsonProviderRef.get()).isInstanceOf(JacksonJsonProvider.class);
  }

  @Test
  public void setsMappingProviderToJacksonMappingProviderBeforeTest() {
    runTestWithValidation(CaptureMappingProvider.class);

    assertThat(mappingProviderRef.get()).isInstanceOf(JacksonMappingProvider.class);
  }

  @Test
  public void restoresJsonProviderToDefaultAfterTest() {
    runTestWithValidation(HasUseJacksonForJsonPathRule.class);

    Configuration configuration = Configuration.defaultConfiguration();
    assertThat(configuration.jsonProvider()).isSameAs(defaultJsonProvider);
  }

  @Test
  public void restoresMappingProviderToDefaultAfterTest() {
    runTestWithValidation(HasUseJacksonForJsonPathRule.class);


    Configuration configuration = Configuration.defaultConfiguration();
    assertThat(configuration.mappingProvider()).isSameAs(defaultMappingProvider);
  }

  public static class HasUseJacksonForJsonPathRule {

    @Rule
    public UseJacksonForJsonPathRule useJacksonForJsonPathRule = new UseJacksonForJsonPathRule();

    @Test
    public void doNothing() {
      assertThat(useJacksonForJsonPathRule).isNotNull();
    }
  }

  public static class CaptureJsonProvider {

    @Rule
    public UseJacksonForJsonPathRule useJacksonForJsonPathRule = new UseJacksonForJsonPathRule();

    @Test
    public void captureJsonProvider() {
      Configuration configuration = Configuration.defaultConfiguration();
      jsonProviderRef.set(configuration.jsonProvider());
      assertThat(jsonProviderRef.get()).isNotNull();
    }
  }

  public static class CaptureMappingProvider {

    @Rule
    public UseJacksonForJsonPathRule useJacksonForJsonPathRule = new UseJacksonForJsonPathRule();

    @Test
    public void captureMappingProvider() {
      Configuration configuration = Configuration.defaultConfiguration();
      mappingProviderRef.set(configuration.mappingProvider());
      assertThat(mappingProviderRef.get()).isNotNull();
    }
  }
}
