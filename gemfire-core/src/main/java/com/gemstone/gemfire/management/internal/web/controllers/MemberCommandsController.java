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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The MemberCommandsController class implements GemFire Management REST API web service endpoints for the
 * Gfsh Member Commands.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.MemberCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 8.0
 */
@Controller("memberController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class MemberCommandsController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.GET, value = "/members")
  @ResponseBody
  //public String listMembers(@RequestBody MultiValueMap<String, String> requestParameters) {
  //public String listMembers(@RequestParam(value = "group", required = false) final String groupName,
  //                          @RequestParam(value = "group", required = false) final String[] groupNames) {
  public String listMembers(@RequestParam(value = CliStrings.LIST_MEMBER__GROUP, required = false) final String groupName) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.LIST_MEMBER);

    //logger.info(String.format("Request Body: %1$s", requestParameters));
    //logger.info(String.format("Request Parameter (group): %1$s", groupName));
    //logger.info(String.format("Request Parameter (group) as array: %1$s", ArrayUtils.toString(groupNames)));

    //final String groupName = requestParameters.getFirst("group");

    if (hasValue(groupName)) {
      command.addOption(CliStrings.LIST_MEMBER__GROUP, groupName);
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/members/{name}")
  @ResponseBody
  public String describeMember(@PathVariable("name") final String memberNameId) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESCRIBE_MEMBER);
    command.addOption(CliStrings.DESCRIBE_MEMBER__IDENTIFIER, decode(memberNameId));
    return processCommand(command.toString());
  }

}
