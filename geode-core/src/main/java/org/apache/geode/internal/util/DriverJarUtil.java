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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DeployedJar;

public class DriverJarUtil {

  // public void registerDriver(String deployedJarName)
  // {
  // try {
  // DeployedJar jar =
  // ClassPathLoader.getLatest().getJarDeployer().findLatestValidDeployedJarFromDisk(deployedJarName);
  // String driverClassName = getJdbcDriverName(jar);
  // File jarFile = jar.getFile();
  // URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{jarFile.toURI().toURL()});
  // Driver driver = (Driver) Class.forName(driverClassName, true,
  // urlClassLoader).newInstance();
  // Driver d = new DriverWrapper(driver);
  // DriverManager.registerDriver(d);
  // } catch (IllegalAccessException | ClassNotFoundException | InstantiationException
  // | SQLException | IOException e) {
  // e.printStackTrace();
  // }
  // }

  public void registerDriver(String driverClassName)
      throws SQLException, ClassNotFoundException, IllegalAccessException,
      InstantiationException {
    URLClassLoader urlClassLoader =
        new URLClassLoader(ClassPathLoader.getLatest().getJarDeployer().getDeployedJarURLs());
    Driver driver = (Driver) Class.forName(driverClassName, true,
        urlClassLoader).newInstance();
    Driver d = new DriverWrapper(driver);
    DriverManager.registerDriver(d);
  }


  public String getJdbcDriverName(String driverJarName) throws IOException {
    return getJdbcDriverName(createDeployedJar(driverJarName));
  }

  public String getJdbcDriverName(DeployedJar jar) throws IOException {
    File jarFile = jar.getFile();
      FileInputStream fileInputStream = createFileInputStream(jarFile.getAbsolutePath());
      BufferedInputStream bufferedInputStream = createBufferedInputStream(fileInputStream);
      ZipInputStream zipInputStream = createZipInputStream(bufferedInputStream);
      ZipEntry zipEntry;
      while ((zipEntry = zipInputStream.getNextEntry()) != null) {
        // JDBC 4.0 Drivers must include the file META-INF/services/java.sql.Driver. This file
        // contains the name of the JDBC drivers implementation of java.sql.Driver
        // See https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
        if (!zipEntry.getName().equals("META-INF/services/java.sql.Driver")) {
          continue;
        }
        int size = (int) zipEntry.getSize();
        if (size == -1) {
          throw new IOException("Invalid zip entry found for META-INF/services/java.sql.Driver " +
              "within jar. Ensure that the jar containing the driver has been deployed and that " +
              "the driver is at least JDBC 4.0");
        }
        byte[] bytes = new byte[size];
        int offset = 0;
        int chunk;
        while ((size - offset) > 0) {
          chunk = zipInputStream.read(bytes, offset, size - offset);
          if (chunk == -1) {
            break;
          }
          offset += chunk;
        }
        return new String(bytes);
      }
      throw new IOException("Could not find JDBC Driver class name in jar file '"
          + jar.getJarName() + "'");
  }

  FileInputStream createFileInputStream(String jarFilePath) throws FileNotFoundException {
    return new FileInputStream(jarFilePath);
  }

  BufferedInputStream createBufferedInputStream(FileInputStream fileInputStream) {
    return new BufferedInputStream(fileInputStream);
  }

  ZipInputStream createZipInputStream(BufferedInputStream bufferedInputStream) {
    return new ZipInputStream(bufferedInputStream);
  }

  DeployedJar createDeployedJar(String driverJarName) throws IOException {
    return ClassPathLoader.getLatest().getJarDeployer().findLatestValidDeployedJarFromDisk(driverJarName);
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
