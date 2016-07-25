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
package com.gemstone.gemfire.management.internal.web.controllers;

import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The ShellCommandsController class implements GemFire REST API calls for Gfsh Shell Commands.
 * <p/>
 * @see com.gemstone.gemfire.management.internal.cli.commands.ClientCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since GemFire 8.0
 */
@Controller("clientController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class ClientCommandsController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.GET, value = "/clients")
  @ResponseBody
	public String listClients() {
    return processCommand(CliStrings.LIST_CLIENTS);
  }

  @RequestMapping(method = RequestMethod.GET, value = "/clients/{clientID}")
  @ResponseBody
  public String describeClient(@PathVariable("clientID") final String clientId) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESCRIBE_CLIENT);
    command.addOption(CliStrings.DESCRIBE_CLIENT__ID, decode(clientId));
    return processCommand(command.toString());
  }

}
