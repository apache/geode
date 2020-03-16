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

import static io.micrometer.core.instrument.util.StringUtils.isNotBlank;
import static org.apache.geode.management.internal.cli.shell.MXBeanProvider.getMemberMXBean;

import java.util.concurrent.TimeUnit;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.AbstractLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.commands.OfflineGfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.i18n.CliStrings;

public class StopServerCommand extends OfflineGfshCommand {
  private static final long WAITING_FOR_STOP_TO_MAKE_PID_GO_AWAY_TIMEOUT_MILLIS = 30 * 1000;

  @CliCommand(value = CliStrings.STOP_SERVER, help = CliStrings.STOP_SERVER__HELP)
  @CliMetaData(shellOnly = true,
      relatedTopic = {CliStrings.TOPIC_GEODE_SERVER, CliStrings.TOPIC_GEODE_LIFECYCLE})
  public ResultModel stopServer(
      @CliOption(key = CliStrings.STOP_SERVER__MEMBER, optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.STOP_SERVER__MEMBER__HELP) final String member,
      @CliOption(key = CliStrings.STOP_SERVER__PID,
          help = CliStrings.STOP_SERVER__PID__HELP) final Integer pid,
      @CliOption(key = CliStrings.STOP_SERVER__DIR,
          help = CliStrings.STOP_SERVER__DIR__HELP) final String workingDirectory)
      throws Exception {
    ServerLauncher.ServerState serverState;

    if (isNotBlank(member)) {
      if (!isConnectedAndReady()) {
        return ResultModel.createError(CliStrings
            .format(CliStrings.STOP_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, "Cache Server"));
      }

      final MemberMXBean serverProxy = getMemberMXBean(member);

      if (serverProxy != null) {
        if (!serverProxy.isServer()) {
          throw new IllegalStateException(CliStrings
              .format(CliStrings.STOP_SERVER__MEMBER_IS_NOT_SERVER_ERROR_MESSAGE, member));
        }

        serverState = ServerLauncher.ServerState.fromJson(serverProxy.status());
        serverProxy.shutDownMember();
      } else {
        return ResultModel.createError(CliStrings
            .format(CliStrings.STOP_SERVER__NO_SERVER_FOUND_FOR_MEMBER_ERROR_MESSAGE, member));
      }

    } else {
      final ServerLauncher serverLauncher =
          new ServerLauncher.Builder().setCommand(ServerLauncher.Command.STOP)
              .setPid(pid).setWorkingDirectory(workingDirectory).build();

      serverState = serverLauncher.status();
      serverLauncher.stop();
    }

    if (AbstractLauncher.Status.ONLINE.equals(serverState.getStatus())) {
      getGfsh().logInfo(
          String.format(CliStrings.STOP_SERVER__STOPPING_SERVER_MESSAGE,
              serverState.getWorkingDirectory(), serverState.getServiceLocation(),
              serverState.getMemberName(), serverState.getPid(), serverState.getLogFile()),
          null);

      StopWatch stopWatch = new StopWatch(true);
      while (serverState.isVmWithProcessIdRunning()) {
        Gfsh.print(".");
        if (stopWatch.elapsedTimeMillis() > WAITING_FOR_STOP_TO_MAKE_PID_GO_AWAY_TIMEOUT_MILLIS) {
          break;
        }
        synchronized (this) {
          TimeUnit.MILLISECONDS.timedWait(this, 500);
        }
      }

      return ResultModel.createInfo("");
    } else {
      return ResultModel.createError(serverState.toString());
    }
  }

}
