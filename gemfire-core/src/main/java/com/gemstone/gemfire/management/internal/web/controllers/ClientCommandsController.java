/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.ClientCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 8.0
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
