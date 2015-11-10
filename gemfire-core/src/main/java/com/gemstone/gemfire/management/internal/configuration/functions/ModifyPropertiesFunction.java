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
package com.gemstone.gemfire.management.internal.configuration.functions;

import java.util.Properties;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.configuration.domain.ConfigurationChangeResult;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

public class ModifyPropertiesFunction extends FunctionAdapter implements
    InternalEntity {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    InternalLocator locator = InternalLocator.getLocator();
    ConfigurationChangeResult configChangeResult = new ConfigurationChangeResult();
    try {
      if (locator.isSharedConfigurationRunning()) {
        Object[] args = (Object[]) context.getArguments();
        Properties properties = (Properties) args[0];
        XmlEntity xmlEntity = (XmlEntity) args[1];
        String[] groups = (String[])args[2];
        
        SharedConfiguration sharedConfig = locator.getSharedConfiguration();
        sharedConfig.modifyProperties(properties, groups);
        sharedConfig.modifyCacheAttributes(xmlEntity, groups);
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
    return ModifyPropertiesFunction.class.getName();
  }

}
