/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.configuration.functions;

import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.SharedConfiguration;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.configuration.domain.ConfigurationChangeResult;

public class DeleteJarFunction extends FunctionAdapter implements
InternalEntity {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public DeleteJarFunction() {
  }

  @Override
  public void execute(FunctionContext context) {

    InternalLocator locator = InternalLocator.getLocator();
    ConfigurationChangeResult configChangeResult = new ConfigurationChangeResult();

    try {
      if (locator.isSharedConfigurationRunning()) {
        final Object[] args = (Object[]) context.getArguments();
        final String[] jarFilenames = (String[]) args[0];
        final String[] groups = (String[])args[1];

        SharedConfiguration sharedConfiguration = locator.getSharedConfiguration();
        sharedConfiguration.removeJars(jarFilenames, groups);
      } else {
        configChangeResult.setErrorMessage("Shared Configuration has not been started in locator : " + locator);
      }
    } catch (Exception e) {
      configChangeResult.setException(e);
      configChangeResult.setErrorMessage(CliUtil.stackTraceAsString(e));
    } finally {
      context.getResultSender().lastResult(configChangeResult);
    }
  }

  @Override
  public String getId() {
    return DeleteJarFunction.class.getName();
  }

}
