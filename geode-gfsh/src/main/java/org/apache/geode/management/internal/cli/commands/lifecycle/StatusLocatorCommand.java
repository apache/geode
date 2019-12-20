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
package org.apache.geode.management.internal.cli.commands.lifecycle;

import static org.apache.geode.management.internal.cli.shell.MXBeanProvider.getMemberMXBean;
import static org.apache.geode.management.internal.i18n.CliStrings.LOCATOR_TERM_NAME;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.AbstractLauncher;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.commands.OfflineGfshCommand;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.configuration.utils.ClusterConfigurationStatusRetriever;
import org.apache.geode.management.internal.i18n.CliStrings;

public class StatusLocatorCommand extends OfflineGfshCommand {
  private static final String SECURITY_PROPERTIES__HELP =
      "The gfsecurity.properties file for configuring SSL to connect to the Locator. The file's path can be absolute or relative to gfsh directory.";

  @CliCommand(value = CliStrings.STATUS_LOCATOR, help = CliStrings.STATUS_LOCATOR__HELP)
  @CliMetaData(shellOnly = true,
      relatedTopic = {CliStrings.TOPIC_GEODE_LOCATOR, CliStrings.TOPIC_GEODE_LIFECYCLE})
  public ResultModel statusLocator(
      @CliOption(key = CliStrings.STATUS_LOCATOR__MEMBER,
          optionContext = ConverterHint.LOCATOR_MEMBER_IDNAME,
          help = CliStrings.STATUS_LOCATOR__MEMBER__HELP) final String member,
      @CliOption(key = CliStrings.STATUS_LOCATOR__HOST,
          help = CliStrings.STATUS_LOCATOR__HOST__HELP) final String locatorHost,
      @CliOption(key = CliStrings.STATUS_LOCATOR__PORT,
          help = CliStrings.STATUS_LOCATOR__PORT__HELP) final Integer locatorPort,
      @CliOption(key = CliStrings.STATUS_LOCATOR__PID,
          help = CliStrings.STATUS_LOCATOR__PID__HELP) final Integer pid,
      @CliOption(key = CliStrings.STATUS_LOCATOR__DIR,
          help = CliStrings.STATUS_LOCATOR__DIR__HELP) final String workingDirectory,
      @CliOption(key = CliStrings.CONNECT__SECURITY_PROPERTIES, optionContext = ConverterHint.FILE,
          help = SECURITY_PROPERTIES__HELP) final File gfSecurityPropertiesFile)
      throws Exception {
    Properties properties = new Properties();
    if (gfSecurityPropertiesFile != null) {
      properties = loadProperties(gfSecurityPropertiesFile);
    }
    if (StringUtils.isNotBlank(member)) {
      if (isConnectedAndReady()) {
        final MemberMXBean locatorProxy = getMemberMXBean(member);

        if (locatorProxy != null) {
          LocatorLauncher.LocatorState state =
              LocatorLauncher.LocatorState.fromJson(locatorProxy.status());
          return createStatusLocatorResult(state, properties);
        } else {
          return ResultModel.createError(CliStrings.format(
              CliStrings.STATUS_LOCATOR__NO_LOCATOR_FOUND_FOR_MEMBER_ERROR_MESSAGE, member));
        }
      } else {
        return ResultModel.createError(CliStrings.format(
            CliStrings.STATUS_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, LOCATOR_TERM_NAME));
      }
    } else {
      final LocatorLauncher locatorLauncher =
          new LocatorLauncher.Builder().setCommand(LocatorLauncher.Command.STATUS)
              .setBindAddress(locatorHost).setDebug(isDebugging()).setPid(pid).setPort(locatorPort)
              .set(properties)
              .setWorkingDirectory(workingDirectory).build();

      final LocatorLauncher.LocatorState status = locatorLauncher.status();
      if (status.getStatus().equals(AbstractLauncher.Status.NOT_RESPONDING)
          || status.getStatus().equals(AbstractLauncher.Status.STOPPED)) {
        return ResultModel.createError(status.toString());
      }
      return createStatusLocatorResult(status, properties);
    }

  }

  protected ResultModel createStatusLocatorResult(final LocatorLauncher.LocatorState state,
      final Properties properties)
      throws NumberFormatException, IOException, ClassNotFoundException {
    ResultModel result = new ResultModel();
    InfoResultModel info = result.addInfo();
    info.addLine(state.toString());
    info.addLine(ClusterConfigurationStatusRetriever.fromLocator(state, properties));
    return result;
  }
}
