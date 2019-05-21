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

package org.apache.geode.internal.util;

import org.apache.geode.internal.ClassPathLoader;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;


public class DriverJarUtil {

  public void registerDriver(String driverClassName)
      throws ClassNotFoundException, IllegalAccessException,
      InstantiationException, SQLException {
    Driver driver = getDriverInstanceByClassName(driverClassName);
    Driver d = new DriverWrapper(driver);
    registerDriverWithDriverManager(d);
  }

  // The methods below are included to facilitate testing and to make the helper methods in this
  // class cleaner
  Driver getDriverInstanceByClassName(String driverClassName)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    return (Driver) ClassPathLoader.getLatest().forName(driverClassName).newInstance();
  }

  void registerDriverWithDriverManager(Driver driver) throws SQLException {
    DriverManager.registerDriver(driver);
  }

  // DriverManager only uses a driver loaded by system ClassLoader
  class DriverWrapper implements Driver {

    private Driver jdbcDriver;

    DriverWrapper(Driver jdbcDriver) {
      this.jdbcDriver = jdbcDriver;
    }

    public Connection connect(String url, java.util.Properties info)
        throws SQLException {
      return this.jdbcDriver.connect(url, info);
    }

    public boolean acceptsURL(String url) throws SQLException {
      return this.jdbcDriver.acceptsURL(url);
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info)
        throws SQLException {
      return this.jdbcDriver.getPropertyInfo(url, info);
    }

    public int getMajorVersion() {
      return this.jdbcDriver.getMajorVersion();
    }

    public int getMinorVersion() {
      return this.jdbcDriver.getMinorVersion();
    }

    public boolean jdbcCompliant() {
      return this.jdbcDriver.jdbcCompliant();
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return this.jdbcDriver.getParentLogger();
    }
  }
}
