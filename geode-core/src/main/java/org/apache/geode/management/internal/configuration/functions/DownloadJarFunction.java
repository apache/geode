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
 *
 */
package org.apache.geode.management.internal.configuration.functions;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.healthmarketscience.rmiio.exporter.RemoteStreamExporter;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.LogService;
import org.apache.geode.management.internal.ManagementAgent;
import org.apache.geode.management.internal.SystemManagementService;

public class DownloadJarFunction implements InternalFunction<Object[]> {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext<Object[]> context) {
    InternalLocator locator = getLocator();
    Object[] args = context.getArguments();
    String group = (String) args[0];
    String jarName = (String) args[1];

    RemoteInputStream result = null;
    if (locator != null && group != null && jarName != null) {
      InternalConfigurationPersistenceService sharedConfig =
          locator.getConfigurationPersistenceService();
      if (sharedConfig != null) {
        SystemManagementService managementService = getExistingManagementService(context);
        ManagementAgent managementAgent = managementService.getManagementAgent();
        if (managementAgent == null) {
          throw new IllegalStateException(
              "Failed to download jar because JMX Management agent is not available. Please ensure geode property jmx-manager is set to true.");
        }

        RemoteInputStreamServer istream = null;
        try {
          File jarFile = sharedConfig.getPathToJarOnThisLocator(group, jarName).toFile();

          RemoteStreamExporter exporter = managementAgent.getRemoteStreamExporter();
          istream =
              new SimpleRemoteInputStream(new BufferedInputStream(new FileInputStream(jarFile)));
          result = exporter.export(istream);
          istream = null;

        } catch (Exception e) {
          logger.error(e);
          throw new IllegalStateException(e.getMessage());
        } finally {
          // we will only close the stream here if the server fails before
          // returning an exported stream
          if (istream != null) {
            istream.close();
          }
        }
      }
    }

    context.getResultSender().lastResult(result);
  }

  InternalLocator getLocator() {
    return (InternalLocator) Locator.getLocator();
  }

  SystemManagementService getExistingManagementService(FunctionContext<Object[]> context) {
    return (SystemManagementService) SystemManagementService
        .getExistingManagementService(context.getCache());
  }

  @Override
  public String getId() {
    return DownloadJarFunction.class.getName();
  }

}
