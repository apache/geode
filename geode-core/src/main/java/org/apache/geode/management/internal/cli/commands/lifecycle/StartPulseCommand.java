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

import static org.apache.geode.internal.Assert.assertState;

import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;

import javax.management.ObjectName;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.SystemFailure;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.cli.commands.GfshCommand;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.OperationInvoker;

public class StartPulseCommand implements GfshCommand {

  @CliCommand(value = CliStrings.START_PULSE, help = CliStrings.START_PULSE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GEODE_MANAGER,
      CliStrings.TOPIC_GEODE_JMX, CliStrings.TOPIC_GEODE_M_AND_M})
  public Result startPulse(@CliOption(key = CliStrings.START_PULSE__URL,
      unspecifiedDefaultValue = "http://localhost:7070/pulse",
      help = CliStrings.START_PULSE__URL__HELP) final String url) {
    try {
      if (StringUtils.isNotBlank(url)) {
        browse(URI.create(url));
        return ResultBuilder.createInfoResult(CliStrings.START_PULSE__RUN);
      } else {
        if (isConnectedAndReady()) {
          OperationInvoker operationInvoker = getGfsh().getOperationInvoker();

          ObjectName managerObjectName = (ObjectName) operationInvoker.getAttribute(
              ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN, "ManagerObjectName");

          String pulseURL =
              (String) operationInvoker.getAttribute(managerObjectName.toString(), "PulseURL");

          if (StringUtils.isNotBlank(pulseURL)) {
            browse(URI.create(pulseURL));
            return ResultBuilder
                .createInfoResult(CliStrings.START_PULSE__RUN + " with URL: " + pulseURL);
          } else {
            String pulseMessage = (String) operationInvoker
                .getAttribute(managerObjectName.toString(), "StatusMessage");
            return (StringUtils.isNotBlank(pulseMessage)
                ? ResultBuilder.createGemFireErrorResult(pulseMessage)
                : ResultBuilder.createGemFireErrorResult(CliStrings.START_PULSE__URL__NOTFOUND));
          }
        } else {
          return ResultBuilder.createUserErrorResult(CliStrings
              .format(CliStrings.GFSH_MUST_BE_CONNECTED_FOR_LAUNCHING_0, "GemFire Pulse"));
        }
      }
    } catch (Exception e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(
          String.format(CliStrings.START_PULSE__ERROR, toString(t, false)));
    }
  }

  private void browse(URI uri) throws IOException {
    assertState(Desktop.isDesktopSupported(),
        String.format(CliStrings.DESKTOP_APP_RUN_ERROR_MESSAGE, System.getProperty("os.name")));
    Desktop.getDesktop().browse(uri);
  }

}
