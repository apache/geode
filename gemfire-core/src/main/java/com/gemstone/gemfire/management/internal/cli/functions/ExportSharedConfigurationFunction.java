/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.configuration.utils.ZipUtils;

public class ExportSharedConfigurationFunction extends FunctionAdapter
    implements InternalEntity {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    InternalLocator locator = InternalLocator.getLocator();
    String memberName = locator.getDistributedSystem().getName();
    
    if (locator.isSharedConfigurationRunning()) {
      SharedConfiguration sc = locator.getSharedConfiguration();
      Date date= new Date();
      new SimpleDateFormat("yyyyMMddhhmm").format(new Timestamp(date.getTime()));
      String zipFileName =  CliStrings.format(CliStrings.EXPORT_SHARED_CONFIG__FILE__NAME, new Timestamp(date.getTime()).toString())  ;
      
      String targetFilePath = FilenameUtils.concat(sc.getSharedConfigurationDirPath(), zipFileName);
      try {
        ZipUtils.zip(sc.getSharedConfigurationDirPath(), targetFilePath);
        File zippedSharedConfig = new File(targetFilePath);
        byte[] zippedConfigData = FileUtils.readFileToByteArray(zippedSharedConfig);
        FileUtils.forceDelete(zippedSharedConfig);
        CliFunctionResult result = new CliFunctionResult(locator.getDistributedSystem().getName(), zippedConfigData, new String[] {zipFileName});
        context.getResultSender().lastResult(result);
      } catch (Exception e) {
        context.getResultSender().lastResult(new CliFunctionResult(memberName, e, e.getMessage()));
      }
    } else {
      CliFunctionResult result = new CliFunctionResult(memberName, false, CliStrings.SHARED_CONFIGURATION_NOT_STARTED);
      context.getResultSender().lastResult(result);
    }
  }

  @Override
  public String getId() {
    return ExportSharedConfigurationFunction.class.getName();
  }
}
