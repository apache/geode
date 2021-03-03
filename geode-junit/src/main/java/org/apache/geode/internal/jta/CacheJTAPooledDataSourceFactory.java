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
package org.apache.geode.internal.jta;

import java.lang.reflect.Method;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.geode.datasource.PooledDataSourceFactory;
import org.apache.geode.internal.ClassPathLoader;

public class CacheJTAPooledDataSourceFactory implements PooledDataSourceFactory {
  @Override
  public DataSource createDataSource(Properties poolProperties, Properties dataSourceProperties) {
    try {
      Class<?> cl =
          ClassPathLoader.getLatest().forName("org.apache.derby.jdbc.EmbeddedDataSource");
      DataSource dataSource = (DataSource) cl.newInstance();
      String url = poolProperties.getProperty("connection-url");
      int startIdx = url.lastIndexOf(':');
      int endIdx = url.indexOf(';');
      if (endIdx == -1) {
        endIdx = url.length();
      }
      String dbName = url.substring(startIdx + 1, endIdx);
      Method setName = cl.getMethod("setDatabaseName", String.class);
      Object[] arg = new Object[1];
      arg[0] = dbName;
      setName.invoke(dataSource, arg);
      if (url.contains("create=true")) {
        Method setCreateDatabase = cl.getMethod("setCreateDatabase", String.class);
        arg[0] = "create";
        setCreateDatabase.invoke(dataSource, arg);
      }
      String username = poolProperties.getProperty("user-name");
      if (username != null) {
        Method setUser = cl.getMethod("setUser", String.class);
        arg[0] = username;
        setUser.invoke(dataSource, arg);
      }
      String password = poolProperties.getProperty("password");
      if (password != null) {
        Method setPassword = cl.getMethod("setPassword", String.class);
        arg[0] = password;
        setPassword.invoke(dataSource, arg);
      }
      return dataSource;
    } catch (Exception e) {
      System.out.println("Failed to create dataSource " + e);
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
