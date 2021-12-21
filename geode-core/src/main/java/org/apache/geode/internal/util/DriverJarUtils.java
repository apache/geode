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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.apache.geode.internal.classloader.ClassPathLoader;


public class DriverJarUtils {

  public void registerDriver(String driverClassName)
      throws ClassNotFoundException, IllegalAccessException,
      InstantiationException, SQLException {
    Driver driver = getDriverInstanceByClassName(driverClassName);
    Driver d = new DriverWrapper(driver);
    registerDriverWithDriverManager(d);
  }

  public void deregisterDriver(String driverClassName) throws SQLException {
    Enumeration<Driver> driverEnumeration = getDrivers();
    Driver driver;
    while (driverEnumeration.hasMoreElements()) {
      driver = driverEnumeration.nextElement();
      if (compareDriverClassName(driver, driverClassName)) {
        deregisterDriverWithDriverManager(driver);
      }
    }
  }

  public List<Driver> getRegisteredDrivers() {
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    return Collections.list(drivers);
  }

  public List<String> getRegisteredDriverNames() {
    List<String> listOfDriverNames = new ArrayList<>();
    for (Driver driver : getRegisteredDrivers()) {
      if (driver instanceof DriverWrapper) {
        listOfDriverNames.add(((DriverWrapper) driver).getWrappedDriverName());
      } else {
        listOfDriverNames.add(driver.getClass().getName());
      }
    }
    return listOfDriverNames;

  }

  // The methods below are included to facilitate testing and to make the helper methods in this
  // class cleaner
  Driver getDriverInstanceByClassName(String driverClassName)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    return (Driver) ClassPathLoader.getLatest().forName(driverClassName).newInstance();
  }

  Enumeration<Driver> getDrivers() {
    return DriverManager.getDrivers();
  }

  boolean compareDriverClassName(Driver driver, String driverClassName) {
    if (driver instanceof DriverWrapper) {
      return ((DriverWrapper) driver).getWrappedDriverName().equals(driverClassName);
    } else {
      return driver.getClass().getName().equals(driverClassName);
    }
  }

  void registerDriverWithDriverManager(Driver driver) throws SQLException {
    DriverManager.registerDriver(driver);
  }

  void deregisterDriverWithDriverManager(Driver driver) throws SQLException {
    DriverManager.deregisterDriver(driver);
  }

  /**
   * <p>
   * Driver Wrapper encapsulates a driver class with a wrapper which is loaded by the system class
   * loader. Since driver classes may be added to the clusters class path after startup and may not
   * be available in the system class loader
   * we use this wrapper to allow the DriverManager to load these drivers.
   * </p>
   */
  class DriverWrapper implements Driver {

    private final Driver jdbcDriver;

    DriverWrapper(Driver jdbcDriver) {
      this.jdbcDriver = jdbcDriver;
    }

    public String getWrappedDriverName() {
      return jdbcDriver.getClass().getName();
    }

    @Override
    public Connection connect(String url, java.util.Properties info)
        throws SQLException {
      return jdbcDriver.connect(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
      return jdbcDriver.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info)
        throws SQLException {
      return jdbcDriver.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
      return jdbcDriver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
      return jdbcDriver.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
      return jdbcDriver.jdbcCompliant();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return jdbcDriver.getParentLogger();
    }
  }
}
