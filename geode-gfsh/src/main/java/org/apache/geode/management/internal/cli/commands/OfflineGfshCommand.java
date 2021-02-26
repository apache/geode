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
package org.apache.geode.management.internal.cli.commands;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import javax.management.remote.JMXServiceURL;

import org.springframework.shell.core.CommandMarker;

import org.apache.geode.management.internal.cli.CliUtils;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.JmxOperationInvoker;
import org.apache.geode.management.internal.cli.shell.OperationInvoker;
import org.apache.geode.management.internal.i18n.CliStrings;

public abstract class OfflineGfshCommand implements CommandMarker {

  public boolean isDebugging() {
    return getGfsh() != null && getGfsh().getDebug();
  }

  public boolean isLogging() {
    return getGfsh() != null;
  }

  public Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  public JMXServiceURL getJmxServiceUrl() {
    OperationInvoker operationInvoker = getGfsh().getOperationInvoker();
    if (operationInvoker instanceof JmxOperationInvoker) {
      return ((JmxOperationInvoker) operationInvoker).getJmxServiceUrl();
    }

    return null;
  }

  public boolean isConnectedAndReady() {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    return gfsh != null && gfsh.isConnectedAndReady();
  }

  protected Properties loadProperties(File... files) {
    Properties properties = new Properties();
    if (files == null) {
      return properties;
    }
    for (File file : files) {
      if (file != null) {
        properties.putAll(loadPropertiesFromFile(file));
      }
    }
    return properties;
  }

  private Properties loadPropertiesFromFile(File propertyFile) {
    try {
      return loadPropertiesFromUrl(propertyFile.toURI().toURL());
    } catch (MalformedURLException e) {
      throw new RuntimeException(
          CliStrings.format("Failed to load configuration properties from pathname (%1$s)!",
              propertyFile.getAbsolutePath()),
          e);
    }
  }

  private static Properties loadPropertiesFromUrl(URL url) {
    Properties properties = new Properties();

    if (url == null) {
      return properties;
    }

    try (InputStream inputStream = url.openStream()) {
      properties.load(inputStream);
    } catch (IOException io) {
      throw new RuntimeException(
          CliStrings.format(CliStrings.CONNECT__MSG__COULD_NOT_READ_CONFIG_FROM_0,
              CliUtils.decodeWithDefaultCharSet(url.getPath())),
          io);
    }

    return properties;
  }

}
