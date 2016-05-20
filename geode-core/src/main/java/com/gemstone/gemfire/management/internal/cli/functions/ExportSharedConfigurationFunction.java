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
package com.gemstone.gemfire.management.internal.cli.functions;

import java.io.File;
import java.util.UUID;

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

      String zipFileName =  CliStrings.format(CliStrings.EXPORT_SHARED_CONFIG__FILE__NAME, UUID.randomUUID());
      
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
