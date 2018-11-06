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

import java.util.Properties;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.geode.datasource.PooledDataSourceFactory;

/**
 * This class implements PooledDataSourceFactory for the JDBC Connector extension.
 * It will be used by default for a jndi-binding of type "POOLED".
 * For more information see "gfsh create jndi-binding".
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
    for (String name : poolProperties.stringPropertyNames()) {
      String hikariName = convertToCamelCase(name);
      String hikariValue = poolProperties.getProperty(name);
      if (name.equals("connection-url")) {
        hikariName = "jdbcUrl";
      } else if (name.equals("jdbc-driver-class")) {
        hikariName = "driverClassName";
      } else if (name.equals("user-name")) {
        hikariName = "username";
      } else if (name.equals("max-pool-size")) {
        hikariName = "maximumPoolSize";
      } else if (name.equals("idle-timeout-seconds")) {
        hikariName = "idleTimeout";
        hikariValue = String.valueOf(Integer.valueOf(hikariValue) * MILLIS_PER_SECOND);
      }
      result.setProperty(hikariName, hikariValue);
    }
    return result;
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
