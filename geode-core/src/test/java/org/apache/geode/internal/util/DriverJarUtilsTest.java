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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Enumeration;

import org.junit.Before;
import org.junit.Test;

public class DriverJarUtilsTest {

  private DriverJarUtils util;

  @Before
  public void setup() {
    util = spy(new DriverJarUtils());
  }

  @Test
  public void registerDriverSucceedsWithClassName()
      throws IllegalAccessException, InstantiationException, ClassNotFoundException, SQLException {
    String driverName = "driver-name";

    Driver driver = mock(Driver.class);
    doReturn(driver).when(util).getDriverInstanceByClassName(driverName);

    util.registerDriver(driverName);
    verify(util).registerDriverWithDriverManager(any());
  }

  @Test
  public void deregisterDriverSucceedsWithClassName() throws SQLException {
    String driverName = "driver-name";
    Enumeration<Driver> drivers = mock(Enumeration.class);
    doReturn(drivers).when(util).getDrivers();
    Driver driver = mock(Driver.class);
    when(drivers.hasMoreElements()).thenReturn(true).thenReturn(false);
    when(drivers.nextElement()).thenReturn(driver);
    doReturn(true).when(util).compareDriverClassName(driver, driverName);

    util.deregisterDriver(driverName);
    verify(util).deregisterDriverWithDriverManager(any());
  }

}
