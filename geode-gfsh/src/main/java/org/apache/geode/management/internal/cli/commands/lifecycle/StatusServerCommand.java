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

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.AbstractLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.commands.OfflineGfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;

public class StatusServerCommand extends OfflineGfshCommand {

  @CliCommand(value = CliStrings.STATUS_SERVER, help = CliStrings.STATUS_SERVER__HELP)
  @CliMetaData(shellOnly = true,
      relatedTopic = {CliStrings.TOPIC_GEODE_SERVER, CliStrings.TOPIC_GEODE_LIFECYCLE})
  public ResultModel statusServer(
      @CliOption(key = CliStrings.STATUS_SERVER__MEMBER, optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.STATUS_SERVER__MEMBER__HELP) final String member,
      @CliOption(key = CliStrings.STATUS_SERVER__PID,
          help = CliStrings.STATUS_SERVER__PID__HELP) final Integer pid,
      @CliOption(key = CliStrings.STATUS_SERVER__DIR,
          help = CliStrings.STATUS_SERVER__DIR__HELP) final String workingDirectory)
      throws IOException {

    if (StringUtils.isNotBlank(member)) {
      if (isConnectedAndReady()) {
        final MemberMXBean serverProxy = getMemberMXBean(member);

        if (serverProxy != null) {
          return ResultModel.createInfo(
              ServerLauncher.ServerState.fromJson(serverProxy.status()).toString());
        } else {
          return ResultModel.createError((CliStrings
              .format(CliStrings.STATUS_SERVER__NO_SERVER_FOUND_FOR_MEMBER_ERROR_MESSAGE, member)));
        }
      } else {
        return ResultModel.createError(CliStrings
            .format(CliStrings.STATUS_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, "Cache Server"));
      }
    } else {
      final ServerLauncher serverLauncher = new ServerLauncher.Builder()
          .setCommand(ServerLauncher.Command.STATUS)
          // NOTE since we do not know whether the "CacheServer" was enabled or not on the GemFire
          // server when it was started,
          // set the disableDefaultServer property in the ServerLauncher.Builder to default status
          // to the MemberMBean
          // TODO fix this hack! (how, the 'start server' loop needs it)
          .setDisableDefaultServer(true).setPid(pid).setWorkingDirectory(workingDirectory).build();

      final ServerLauncher.ServerState status = serverLauncher.status();

      if (status.getStatus().equals(AbstractLauncher.Status.NOT_RESPONDING)
          || status.getStatus().equals(AbstractLauncher.Status.STOPPED)) {
        return ResultModel.createError(status.toString());
      }
      return ResultModel.createInfo(status.toString());
    }
  }
}
