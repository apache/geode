/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The LauncherLifecycleCommandsController class implements REST API calls for the Gfsh Launcher Lifecycle commands.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.LauncherLifecycleCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 8.0
 */
@Controller("launcherLifecycleController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class LauncherLifecycleCommandsController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.GET, value = "/members/{name}/locator")
  @ResponseBody
  public String statusLocator(@PathVariable("name") final String memberNameId) {
    return getMemberMXBean(memberNameId).status();
  }

  @RequestMapping(method = RequestMethod.GET, value = "/members/{name}/server")
  @ResponseBody
  public String statusServer(@PathVariable("name") final String memberNameId) {
    return getMemberMXBean(memberNameId).status();
  }

}
