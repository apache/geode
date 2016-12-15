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
package org.apache.geode.management.internal.cli.functions;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.SharedConfiguration;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;

import java.io.File;
import java.util.UUID;

public class ExportSharedConfigurationFunction extends FunctionAdapter implements InternalEntity {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    InternalLocator locator = InternalLocator.getLocator();
    String memberName = locator.getDistributedSystem().getName();

    if (!locator.isSharedConfigurationRunning()) {
      CliFunctionResult result =
          new CliFunctionResult(memberName, false, CliStrings.SHARED_CONFIGURATION_NOT_STARTED);
      context.getResultSender().lastResult(result);
      return;
    }

    SharedConfiguration sc = locator.getSharedConfiguration();
    String zipFileName =
        CliStrings.format(CliStrings.EXPORT_SHARED_CONFIG__FILE__NAME, UUID.randomUUID());
    File zipFile = new File(sc.getSharedConfigurationDirPath(), zipFileName);

    try {
      for (Configuration config : sc.getEntireConfiguration().values()) {
        sc.writeConfig(config);
      }

      ZipUtils.zip(sc.getSharedConfigurationDirPath(), zipFile.getCanonicalPath());

      byte[] zippedConfigData = FileUtils.readFileToByteArray(zipFile);
      FileUtils.forceDelete(zipFile);
      CliFunctionResult result = new CliFunctionResult(locator.getDistributedSystem().getName(),
          zippedConfigData, new String[]{zipFileName});
      context.getResultSender().lastResult(result);
    } catch (Exception e) {
      context.getResultSender().lastResult(new CliFunctionResult(memberName, e, e.getMessage()));
    }
  }

  @Override
  public String getId() {
    return ExportSharedConfigurationFunction.class.getName();
  }
}
