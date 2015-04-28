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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The ClusterCommandsController class implements GemFire REST API web service calls for the Gfsh Cluster
 * (System)-based commands.
 * <p/>
 * @author John Blum
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @since 8.0
 */
@Controller("clusterController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class ClusterCommandsController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.GET, value = "/cluster")
  @ResponseBody
  public String statusSystem() {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.STATUS_SYSTEM);
    return processCommand(command.toString());
  }

}
