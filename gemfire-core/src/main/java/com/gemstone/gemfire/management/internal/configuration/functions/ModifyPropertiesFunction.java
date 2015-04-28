/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
