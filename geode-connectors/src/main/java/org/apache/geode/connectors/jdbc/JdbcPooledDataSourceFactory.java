/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.connectors.jdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.geode.datasource.PooledDataSourceFactory;

/**
 * This class implements PooledDataSourceFactory for the JDBC Connector extension.
 * It will be used by default for a data-source with type "POOLED" specified when using
 * the "gfsh create data-source" command.
 * <p>
 * The connection pooling is provided by Hikari, a high-performance JDBC connection pool.
 * <br>
 * The following data-source parameters will be will be passed to Hikari as the following:
 * <br>
 * connection-url --&gt; jdbcUrl<br>
 * jdbc-driver-class --&gt; driverClassName<br>
 * user-name --&gt; username<br>
 * password --&gt; password<br>
 * max-pool-size --&gt; maximumPoolSize<br>
 * idle-timeout-seconds --&gt; idleTimeout<br>
 * <p>
 * Additional Hikari configuration parameters may be passed to configure the Hikari pool
 * by specifying them using the --pool-properties of the "gfsh create data-source" command.
 * <p>
 * For more information see the "gfsh create data-source" command.
 * <br>
 * Additional Hikari options are described at
 * https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby
 */
public class JdbcPooledDataSourceFactory implements PooledDataSourceFactory {

  public JdbcPooledDataSourceFactory() {}

  @Override
  public DataSource createDataSource(Properties poolProperties, Properties dataSourceProperties) {
    Properties hikariProperties = convertToHikari(poolProperties);
    HikariConfig config = new HikariConfig(hikariProperties);
    config.setDataSourceProperties(dataSourceProperties);
    return new HikariDataSource(config);
  }

  Properties convertToHikari(Properties poolProperties) {
    final int MILLIS_PER_SECOND = 1000;
    Properties result = new Properties();

    // Capture the JDBC URL to extract embedded parameters later
    String jdbcUrl = null;

    for (String name : poolProperties.stringPropertyNames()) {
      String hikariName = convertToCamelCase(name);
      String hikariValue = poolProperties.getProperty(name);
      if (name.equals("connection-url")) {
        hikariName = "jdbcUrl";
        // Store the URL for parameter extraction
        jdbcUrl = hikariValue;
      } else if (name.equals("jdbc-driver-class")) {
        hikariName = "driverClassName";
      } else if (name.equals("user-name")) {
        hikariName = "username";
      } else if (name.equals("max-pool-size")) {
        hikariName = "maximumPoolSize";
      } else if (name.equals("idle-timeout-seconds")) {
        hikariName = "idleTimeout";
        hikariValue = String.valueOf(Integer.parseInt(hikariValue) * MILLIS_PER_SECOND);
      }
      result.setProperty(hikariName, hikariValue);
    }

    // Extract username and password from JDBC URL query parameters if not explicitly provided.
    // This is necessary because some JDBC URLs embed credentials in the query string
    // (e.g., jdbc:postgresql://localhost:5432/db?user=postgres&password=secret).
    // HikariCP expects these as separate properties, so we extract them from the URL
    // and set them explicitly, then strip the parameters from the URL.
    if (jdbcUrl != null && jdbcUrl.contains("?")) {
      Map<String, String> urlParams = parseUrlParameters(jdbcUrl);

      // Only set username from URL if not explicitly provided via user-name property.
      // Explicit properties take precedence over URL parameters.
      if (!result.containsKey("username") && urlParams.containsKey("user")) {
        String userFromUrl = urlParams.get("user");
        result.setProperty("username", userFromUrl);
      }

      // Only set password from URL if not explicitly provided via password property.
      // Explicit properties take precedence over URL parameters.
      if (!result.containsKey("password") && urlParams.containsKey("password")) {
        String passwordFromUrl = urlParams.get("password");
        result.setProperty("password", passwordFromUrl);
      }

      // Strip parameters from the URL since HikariCP expects them as separate properties.
      // This prevents the JDBC driver from receiving duplicate or conflicting credentials.
      String cleanUrl = jdbcUrl.substring(0, jdbcUrl.indexOf('?'));
      result.setProperty("jdbcUrl", cleanUrl);
    }

    return result;
  }

  /**
   * Parses query string parameters from a JDBC URL.
   * <p>
   * Extracts key-value pairs from the query string portion of a JDBC URL.
   * For example, given "jdbc:postgresql://localhost:5432/db?user=postgres&password=secret",
   * this method returns a map containing {"user": "postgres", "password": "secret"}.
   * <p>
   * This is necessary because JDBC URLs can contain credentials and other configuration
   * parameters in their query strings, but HikariCP expects these to be provided as
   * separate properties. By extracting these parameters, we can properly configure
   * the connection pool regardless of how the URL is formatted.
   * <p>
   * Invalid parameter pairs (missing '=' or empty values) are silently skipped to avoid
   * errors during connection pool initialization.
   *
   * @param jdbcUrl the JDBC URL (e.g., "jdbc:postgresql://host:port/db?user=foo&password=bar")
   * @return a map of parameter names to values; empty map if no query string is present
   */
  Map<String, String> parseUrlParameters(String jdbcUrl) {
    Map<String, String> params = new HashMap<>();

    // Return empty map if URL has no query string
    if (jdbcUrl == null || !jdbcUrl.contains("?")) {
      return params;
    }

    // Extract the query string portion after the '?'
    String queryString = jdbcUrl.substring(jdbcUrl.indexOf('?') + 1);

    // Split by '&' to get individual parameter pairs
    String[] pairs = queryString.split("&");

    for (String pair : pairs) {
      int idx = pair.indexOf('=');

      // Only process valid key=value pairs (skip malformed parameters)
      // idx > 0 ensures non-empty key, idx < length-1 ensures non-empty value
      if (idx > 0 && idx < pair.length() - 1) {
        String key = pair.substring(0, idx);
        String value = pair.substring(idx + 1);
        params.put(key, value);
      }
    }

    return params;
  }

  private String convertToCamelCase(String name) {
    StringBuilder nameBuilder = new StringBuilder(name.length());
    boolean capitalizeNextChar = false;
    for (char c : name.toCharArray()) {
      if (c == '-') {
        capitalizeNextChar = true;
        continue;
      }
      if (capitalizeNextChar) {
        nameBuilder.append(Character.toUpperCase(c));
      } else {
        nameBuilder.append(c);
      }
      capitalizeNextChar = false;
    }
    return nameBuilder.toString();
  }

}
