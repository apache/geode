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
package org.apache.geode.connectors.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.Test;

public class JdbcPooledDataSourceFactoryTest {

  private JdbcPooledDataSourceFactory instance = new JdbcPooledDataSourceFactory();

  @Test
  public void validateThatConnectionUrlConvertedToJdbcUrl() throws Exception {
    Properties poolProperties = new Properties();
    poolProperties.setProperty("connection-url", "foo");
    Properties hikariProperties = instance.convertToHikari(poolProperties);

    assertThat(hikariProperties.stringPropertyNames()).contains("jdbcUrl");
  }

  @Test
  public void validateThatUserNameConvertedToUsername() throws Exception {
    Properties poolProperties = new Properties();
    poolProperties.setProperty("user-name", "foo");
    Properties hikariProperties = instance.convertToHikari(poolProperties);

    assertThat(hikariProperties.stringPropertyNames()).contains("username");
  }

  @Test
  public void validateThatJdbcDriverClassConvertedToDriverClassName() throws Exception {
    Properties poolProperties = new Properties();
    poolProperties.setProperty("jdbc-driver-class", "foo");
    Properties hikariProperties = instance.convertToHikari(poolProperties);

    assertThat(hikariProperties.stringPropertyNames()).contains("driverClassName");
  }

  @Test
  public void validateThatMaxPoolSizeConvertedToMaximumPoolSize() throws Exception {
    Properties poolProperties = new Properties();
    poolProperties.setProperty("max-pool-size", "foo");
    Properties hikariProperties = instance.convertToHikari(poolProperties);

    assertThat(hikariProperties.stringPropertyNames()).contains("maximumPoolSize");
  }

  @Test
  public void validateThatIdleTimeoutSecondsConvertedToIdleTimeout() throws Exception {
    Properties poolProperties = new Properties();
    poolProperties.setProperty("idle-timeout-seconds", "20");
    Properties hikariProperties = instance.convertToHikari(poolProperties);

    assertThat(hikariProperties.stringPropertyNames()).contains("idleTimeout");
  }

  @Test
  public void validateThatIdleTimeoutSecondsConvertedToMilliseconds() throws Exception {
    Properties poolProperties = new Properties();
    poolProperties.setProperty("idle-timeout-seconds", "20");
    Properties hikariProperties = instance.convertToHikari(poolProperties);

    assertThat(hikariProperties.getProperty("idleTimeout")).isEqualTo("20000");
  }

  @Test
  public void validateThatHyphensConvertedToCamelCase() throws Exception {
    Properties poolProperties = new Properties();
    poolProperties.setProperty("foo-bar-zoo", "value");
    poolProperties.setProperty("foo", "value");
    poolProperties.setProperty("-bar", "value");
    poolProperties.setProperty("zoo-", "value");
    Properties hikariProperties = instance.convertToHikari(poolProperties);

    assertThat(hikariProperties.stringPropertyNames()).containsExactlyInAnyOrder("foo", "Bar",
        "zoo", "fooBarZoo");
  }

}
