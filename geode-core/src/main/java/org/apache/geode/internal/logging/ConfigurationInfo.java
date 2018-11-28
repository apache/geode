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
package org.apache.geode.internal.logging;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.status.StatusLogger;

import org.apache.geode.internal.ClassPathLoader;

/**
 * Fetches the configuration info from {@code Log4jAgent} without direct class dependency.
 *
 * <p>
 * This could potentially be modified to support any logging backend but currently the only caller
 * is the log Banner which is static.
 */
public class ConfigurationInfo {

  private static final Logger LOGGER = StatusLogger.getLogger();

  /**
   * Fetches the configuration info from Log4jAgent without direct class dependency.
   *
   * <p>
   * If the Log4J2 Core classes are not in the classpath, the return value is simply
   * "No configuration info found."
   */
  public static String getConfigurationInfo() {
    try {
      Class<? extends ProviderAgent> agentClass =
          ClassPathLoader.getLatest().forName(Configuration.DEFAULT_PROVIDER_AGENT_NAME)
              .asSubclass(ProviderAgent.class);
      Method method = agentClass.getMethod("getConfigurationInfo", null);
      return (String) method.invoke(null, null);
    } catch (ClassNotFoundException | ClassCastException | NoSuchMethodException
        | IllegalAccessException | InvocationTargetException e) {
      LOGGER.debug("Unable to invoke Log4jAgent.getConfigurationInfo()", e);
      return "No configuration info found";
    }
  }
}
