/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

/***
 * Function to load the shared configuration (already imported) from the disk. 
 * @author bansods
 *
 */
public class LoadSharedConfigurationFunction extends FunctionAdapter implements
InternalEntity {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    InternalLocator locator = InternalLocator.getLocator();
    String memberName = locator.getDistributedSystem().getName();

    try {
      if (locator.isSharedConfigurationRunning()) {
        SharedConfiguration sc = locator.getSharedConfiguration();
        sc.loadSharedConfigurationFromDisk();
        CliFunctionResult cliFunctionResult = new CliFunctionResult(memberName, true, CliStrings.IMPORT_SHARED_CONFIG__SUCCESS__MSG);
        context.getResultSender().lastResult(cliFunctionResult);
      } else {
        CliFunctionResult cliFunctionResult = new CliFunctionResult(memberName, false, CliStrings.SHARED_CONFIGURATION_NOT_STARTED);
        context.getResultSender().lastResult(cliFunctionResult);
      }
    } catch (Exception e) {
      context.getResultSender().lastResult(new CliFunctionResult(memberName, e, CliUtil.stackTraceAsString(e)));
    }
  }

  @Override
  public String getId() {
    return LoadSharedConfigurationFunction.class.getName();
  }

}
