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
package org.apache.geode.internal.datasource;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.logging.LogService;

/**
 * GemFireBasicDataSource extends AbstractDataSource. This is a datasource class which provides
 * connections. fromthe databse without any pooling.
 *
 */
public class GemFireBasicDataSource extends AbstractDataSource {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = -4010116024816908360L;

  /** Creates a new instance of BaseDataSource */
  protected transient Driver driverObject = null;

  /**
   * Place holder for abstract method isWrapperFor(java.lang.Class) in java.sql.Wrapper required by
   * jdk 1.6
   *
   * @param iface - a Class defining an interface.
   */
  public boolean isWrapperFor(Class iface) throws SQLException {
    return true;
  }

  /**
   * Place holder for abstract method java.lang Object unwrap(java.lang.Class) in java.sql.Wrapper
   * required by jdk 1.6
   *
   * @param iface - a Class defining an interface.
   * @return java.lang.Object
   */
  public Object unwrap(Class iface) throws SQLException {
    return iface;
  }

  /**
   * Creates a new instance of GemFireBasicDataSource
   *
   * @param configs The ConfiguredDataSourceProperties containing the datasource properties.
   */
  public GemFireBasicDataSource(ConfiguredDataSourceProperties configs) throws SQLException {
    super(configs);
    loadDriver();
  }

  /**
   * Implementation of datasource interface function. This method is used to get the connection from
   * the database. Default user name and password will be used.
   *
   * @return ???
   */
  @Override
  public Connection getConnection() throws SQLException {
    // Asif : In case the user is requesting the
    // connection without username & password
    // we should just return the desired connection
    Connection connection = null;
    if (driverObject == null) {
      synchronized (this) {
        if (driverObject == null)
          loadDriver();
      }
    }

    if (url != null) {
      Properties props = new Properties();

      // If no default username or password is specified don't add these properties - the user may
      // be connecting to a system which does not require authentication
      if (user != null) {
        props.put("user", user);
      }

      // check for password separately from username - some drivers may throw different error
      // messages we want to capture
      if (password != null) {
        props.put("password", password);
      }
      connection = driverObject.connect(url, props);
    } else {
      String exception =
          "GemFireBasicDataSource::getConnection:Url for the DataSource not available";
      logger.info(exception);
      throw new SQLException(exception);
    }
    return connection;
  }

  /**
   * Implementation of datasource function. This method is used to get the connection. The specified
   * user name and passowrd will be used.
   *
   * @param clUsername The username for the database connection.
   * @param clPassword The password for the database connection.
   */
  @Override
  public Connection getConnection(String clUsername, String clPassword) throws SQLException {
    // First Autheticate the user
    checkCredentials(clUsername, clPassword);
    return getConnection();
  }

  private void loadDriver() throws SQLException {
    try {
      if (jdbcDriver != null && jdbcDriver.length() > 0) {
        loadDriverUsingClassName();
      } else {
        loadDriverUsingURL();
      }
    } catch (Exception ex) {
      String msg =
          "An Exception was caught while trying to load the driver. %s";
      String msgArg = ex.getLocalizedMessage();
      logger.error(String.format(msg, msgArg), ex);
      throw new SQLException(String.format(msg, msgArg));
    }
  }

  private void loadDriverUsingURL() throws SQLException {
    driverObject = DriverManager.getDriver(this.url);
  }

  private void loadDriverUsingClassName()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class<?> driverClass = ClassPathLoader.getLatest().forName(jdbcDriver);
    driverObject = (Driver) driverClass.newInstance();
  }
}
