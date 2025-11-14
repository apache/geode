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

  private final JdbcPooledDataSourceFactory instance = new JdbcPooledDataSourceFactory();

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

  /**
   * Verifies that username embedded in JDBC URL query parameters is extracted and set as a
   * separate property.
   * <p>
   * This test ensures that JDBC URLs like "jdbc:postgresql://...?user=postgres" are properly
   * handled by extracting the username from the URL and setting it as the "username" property
   * that HikariCP expects.
   * <p>
   * Context: PostgreSQL (and other databases) support embedding credentials in the JDBC URL
   * query string. HikariCP, however, expects credentials as separate properties. This extraction
   * logic ensures compatibility with both URL formats.
   */
  @Test
  public void validateThatUsernameIsExtractedFromUrl() throws Exception {
    Properties poolProperties = new Properties();
    poolProperties.setProperty("connection-url",
        "jdbc:postgresql://localhost:5432/test?user=postgres");
    Properties hikariProperties = instance.convertToHikari(poolProperties);

    assertThat(hikariProperties.getProperty("username")).isEqualTo("postgres");
  }

  /**
   * Verifies that both username and password embedded in JDBC URL query parameters are
   * extracted and set as separate properties.
   * <p>
   * This test ensures that JDBC URLs with multiple parameters like
   * "jdbc:postgresql://...?user=postgres&password=secret" are properly parsed, with both
   * the username and password extracted and set as separate properties for HikariCP.
   * <p>
   * Context: This handles the common case where both credentials are embedded in the URL,
   * which is typical in test environments and some production configurations.
   */
  @Test
  public void validateThatPasswordIsExtractedFromUrl() throws Exception {
    Properties poolProperties = new Properties();
    poolProperties.setProperty("connection-url",
        "jdbc:postgresql://localhost:5432/test?user=postgres&password=secret");
    Properties hikariProperties = instance.convertToHikari(poolProperties);

    assertThat(hikariProperties.getProperty("username")).isEqualTo("postgres");
    assertThat(hikariProperties.getProperty("password")).isEqualTo("secret");
  }

  /**
   * Verifies that only user and password parameters are stripped from the JDBC URL after
   * extraction.
   * <p>
   * This test ensures that after extracting username/password from the URL query string,
   * the resulting jdbcUrl property has only those credentials removed while other parameters
   * are preserved. For example, "jdbc:postgresql://localhost:5432/test?user=postgres" becomes
   * "jdbc:postgresql://localhost:5432/test", with credentials removed.
   * <p>
   * Context: This is critical because HikariCP will set username/password as separate
   * properties on the connection. If we leave them in the URL as well, it could cause
   * conflicts or the JDBC driver might reject duplicate credentials. Only credential
   * parameters must be removed after extraction.
   */
  @Test
  public void validateThatUrlParametersAreStrippedFromJdbcUrl() throws Exception {
    Properties poolProperties = new Properties();
    poolProperties.setProperty("connection-url",
        "jdbc:postgresql://localhost:5432/test?user=postgres");
    Properties hikariProperties = instance.convertToHikari(poolProperties);

    assertThat(hikariProperties.getProperty("jdbcUrl"))
        .isEqualTo("jdbc:postgresql://localhost:5432/test");
  }

  /**
   * Verifies that explicitly provided username takes precedence over URL parameters.
   * <p>
   * This test ensures that when both a URL parameter (user=postgres) and an explicit
   * user-name property (admin) are provided, the explicit property wins. This gives
   * administrators the ability to override URL-embedded credentials without modifying
   * the URL itself.
   * <p>
   * Context: This precedence rule is important for security and flexibility. It allows
   * users to:
   * 1. Override credentials in production without changing connection strings
   * 2. Use different credentials for the same URL in different environments
   * 3. Maintain backward compatibility with existing configurations
   * <p>
   * The implementation checks for existing "username" property before extracting from URL.
   */
  @Test
  public void validateThatExplicitUsernameOverridesUrlParameter() throws Exception {
    Properties poolProperties = new Properties();
    poolProperties.setProperty("connection-url",
        "jdbc:postgresql://localhost:5432/test?user=postgres");
    poolProperties.setProperty("user-name", "admin");
    Properties hikariProperties = instance.convertToHikari(poolProperties);

    assertThat(hikariProperties.getProperty("username")).isEqualTo("admin");
  }

  /**
   * Verifies that non-credential parameters (like useSSL, serverTimezone, etc.) are preserved
   * in the JDBC URL after credential extraction.
   * <p>
   * This test ensures that when extracting and stripping user/password from a JDBC URL,
   * other important parameters like "useSSL=false" are NOT removed. For example:
   * "jdbc:mysql://localhost:3306/test?user=root&password=secret&useSSL=false" should become
   * "jdbc:mysql://localhost:3306/test?useSSL=false" with credentials removed but useSSL preserved.
   * <p>
   * Context: This is critical for databases like MySQL where parameters like useSSL,
   * serverTimezone,
   * and characterEncoding control important connection behavior. Removing these parameters would
   * break functionality (e.g., MySQL might attempt SSL handshake with deprecated protocols if
   * useSSL=false is lost). Only credential parameters (user, password) should be stripped.
   */
  @Test
  public void validateThatNonCredentialParametersArePreserved() throws Exception {
    Properties poolProperties = new Properties();
    poolProperties.setProperty("connection-url",
        "jdbc:mysql://localhost:3306/test?user=root&password=secret&useSSL=false&serverTimezone=UTC");
    Properties hikariProperties = instance.convertToHikari(poolProperties);

    assertThat(hikariProperties.getProperty("jdbcUrl"))
        .isEqualTo("jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC");
    assertThat(hikariProperties.getProperty("username")).isEqualTo("root");
    assertThat(hikariProperties.getProperty("password")).isEqualTo("secret");
  }

}
