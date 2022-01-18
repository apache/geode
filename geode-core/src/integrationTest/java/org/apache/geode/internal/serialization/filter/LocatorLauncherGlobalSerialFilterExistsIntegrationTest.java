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
package org.apache.geode.internal.serialization.filter;

import static java.lang.String.valueOf;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.internal.serialization.filter.SerialFilterAssertions.assertThatSerialFilterIsSameAs;

import java.lang.reflect.InvocationTargetException;

import org.junit.Test;

import org.apache.geode.distributed.LocatorLauncher;

public class LocatorLauncherGlobalSerialFilterExistsIntegrationTest
    extends LocatorLauncherWithJmxManager {

  @Test
  public void startDoesNotConfigureGlobalSerialFilter_whenFilterExists()
      throws InvocationTargetException, IllegalAccessException {
    Object existingGlobalSerialFilter = OBJECT_INPUT_FILTER_API.createFilter("!*");
    OBJECT_INPUT_FILTER_API.setSerialFilter(existingGlobalSerialFilter);

    locator.set(new LocatorLauncher.Builder()
        .setMemberName(NAME)
        .setPort(locatorPort)
        .setWorkingDirectory(valueOf(workingDirectory))
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOG_FILE, valueOf(logFile))
        .build())
        .get()
        .start();

    assertThatSerialFilterIsSameAs(existingGlobalSerialFilter);
  }
}
