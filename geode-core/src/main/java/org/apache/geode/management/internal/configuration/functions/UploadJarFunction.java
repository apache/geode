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

import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;

public class UploadJarFunction implements Function<Object[]>, InternalEntity {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext<Object[]> context) {
    InternalLocator locator = (InternalLocator) Locator.getLocator();
    Object[] args = context.getArguments();
    String group = (String) args[0];
    String jarName = (String) args[1];

    byte[] jarBytes = null;
    if (locator != null && group != null && jarName != null) {
      ClusterConfigurationService sharedConfig = locator.getSharedConfiguration();
      if (sharedConfig != null) {
        try {
          jarBytes = sharedConfig.getJarBytesFromThisLocator(group, jarName);
          context.getResultSender().lastResult(jarBytes);
        } catch (Exception e) {
          logger.error(e);
          throw new IllegalStateException(e.getMessage());
        }
      }
    }
  }

  @Override
  public String getId() {
    return UploadJarFunction.class.getName();
  }

}
