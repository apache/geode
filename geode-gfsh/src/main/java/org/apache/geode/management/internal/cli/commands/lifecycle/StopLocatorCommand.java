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

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.geode.management.internal.cli.shell.MXBeanProvider.getMemberMXBean;
import static org.apache.geode.management.internal.i18n.CliStrings.LOCATOR_TERM_NAME;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.AbstractLauncher;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.commands.OfflineGfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.i18n.CliStrings;

public class StopLocatorCommand extends OfflineGfshCommand {
  private static final long WAITING_FOR_STOP_TO_MAKE_PID_GO_AWAY_TIMEOUT_MILLIS = 30 * 1000;

  @CliCommand(value = CliStrings.STOP_LOCATOR, help = CliStrings.STOP_LOCATOR__HELP)
  @CliMetaData(shellOnly = true,
      relatedTopic = {CliStrings.TOPIC_GEODE_LOCATOR, CliStrings.TOPIC_GEODE_LIFECYCLE})
  public ResultModel stopLocator(
      @CliOption(key = CliStrings.STOP_LOCATOR__MEMBER,
          optionContext = ConverterHint.LOCATOR_MEMBER_IDNAME,
          help = CliStrings.STOP_LOCATOR__MEMBER__HELP) final String member,
      @CliOption(key = CliStrings.STOP_LOCATOR__PID,
          help = CliStrings.STOP_LOCATOR__PID__HELP) final Integer pid,
      @CliOption(key = CliStrings.STOP_LOCATOR__DIR,
          help = CliStrings.STOP_LOCATOR__DIR__HELP) final String workingDirectory)
      throws Exception {

    LocatorLauncher.LocatorState locatorState;
    if (isNotBlank(member)) {
      if (isConnectedAndReady()) {
        final MemberMXBean locatorProxy = getMemberMXBean(member);

        if (locatorProxy != null) {
          if (!locatorProxy.isLocator()) {
            throw new IllegalStateException(
                CliStrings.format(CliStrings.STOP_LOCATOR__NOT_LOCATOR_ERROR_MESSAGE, member));
          }

          if (locatorProxy.isServer()) {
            throw new IllegalStateException(CliStrings
                .format(CliStrings.STOP_LOCATOR__LOCATOR_IS_CACHE_SERVER_ERROR_MESSAGE, member));
          }

          locatorState = LocatorLauncher.LocatorState.fromJson(locatorProxy.status());
          locatorProxy.shutDownMember();
        } else {
          return ResultModel.createError(CliStrings
              .format(CliStrings.STOP_LOCATOR__NO_LOCATOR_FOUND_FOR_MEMBER_ERROR_MESSAGE, member));
        }
      } else {
        return ResultModel.createError(CliStrings
            .format(CliStrings.STOP_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, LOCATOR_TERM_NAME));
      }
    } else {
      final LocatorLauncher locatorLauncher =
          new LocatorLauncher.Builder().setCommand(LocatorLauncher.Command.STOP)
              .setDebug(isDebugging()).setPid(pid).setWorkingDirectory(workingDirectory).build();

      locatorState = locatorLauncher.status();
      locatorLauncher.stop();
    }

    if (AbstractLauncher.Status.ONLINE.equals(locatorState.getStatus())) {
      getGfsh().logInfo(
          String.format(CliStrings.STOP_LOCATOR__STOPPING_LOCATOR_MESSAGE,
              locatorState.getWorkingDirectory(), locatorState.getServiceLocation(),
              locatorState.getMemberName(), locatorState.getPid(), locatorState.getLogFile()),
          null);

      StopWatch stopWatch = new StopWatch(true);
      while (locatorState.isVmWithProcessIdRunning()) {
        Gfsh.print(".");
        if (stopWatch.elapsedTimeMillis() > WAITING_FOR_STOP_TO_MAKE_PID_GO_AWAY_TIMEOUT_MILLIS) {
          break;
        }
        synchronized (this) {
          TimeUnit.MILLISECONDS.timedWait(this, 500);
        }
      }

      return ResultModel.createInfo(StringUtils.EMPTY);
    } else {
      return ResultModel.createError(locatorState.toString());
    }
  }
}
