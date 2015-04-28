/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.controllers;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The DurableClientCommandsController class implements GemFire Management REST API web service endpoints for the
 * durable client/CQs Gfsh commands.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.DurableClientCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 8.0
 */
@Controller("durableClientController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class DurableClientCommandsController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.GET, value = "/durable-clients/{durable-client-id}/cqs")
  @ResponseBody
  public String listDurableClientContinuousQueries(@PathVariable("durable-client-id") final String durableClientId,
                                                   @RequestParam(value = CliStrings.LIST_DURABLE_CQS__MEMBER, required = false) final String memberNameId,
                                                   @RequestParam(value = CliStrings.LIST_DURABLE_CQS__GROUP, required = false) final String[] groups)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.LIST_DURABLE_CQS);

    command.addOption(CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, decode(durableClientId));

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.LIST_DURABLE_CQS__MEMBER, memberNameId);
    }

    if (hasValue(groups)) {
      command.addOption(CliStrings.LIST_DURABLE_CQS__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/durable-clients/{durable-client-id}/cqs/events")
  @ResponseBody
  public String countDurableClientContinuousQueryEvents(@PathVariable("durable-client-id") final String durableClientId,
                                                        @RequestParam(value = CliStrings.COUNT_DURABLE_CQ_EVENTS__MEMBER, required = false) final String memberNameId,
                                                        @RequestParam(value = CliStrings.COUNT_DURABLE_CQ_EVENTS__GROUP, required = false) final String[] groups)
  {
    return internalCountDurableClientContinuousQueryEvents(decode(durableClientId), null, memberNameId, groups);
  }

  @RequestMapping(method = RequestMethod.GET, value = "/durable-clients/{durable-client-id}/cqs/{durable-cq-name}/events")
  @ResponseBody
  public String countDurableClientContinuousQueryEvents(@PathVariable("durable-client-id") final String durableClientId,
                                                        @PathVariable("durable-cq-name") final String durableCqName,
                                                        @RequestParam(value = CliStrings.COUNT_DURABLE_CQ_EVENTS__MEMBER, required = false) final String memberNameId,
                                                        @RequestParam(value = CliStrings.COUNT_DURABLE_CQ_EVENTS__GROUP, required = false) final String[] groups)
  {
    return internalCountDurableClientContinuousQueryEvents(decode(durableClientId), decode(durableCqName), memberNameId, groups);
  }

  protected String internalCountDurableClientContinuousQueryEvents(final String durableClientId,
                                                                   final String cqName,
                                                                   final String memberNameId,
                                                                   final String[] groups)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);

    command.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, durableClientId);

    if (hasValue(cqName)) {
      command.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME, cqName);
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__MEMBER, memberNameId);
    }

    if (hasValue(groups)) {
      command.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/durable-clients/{durable-client-id}", params = "op=close")
  @ResponseBody
  public String closeDurableClient(@PathVariable("durable-client-id") final String durableClientId,
                                   @RequestParam(value = CliStrings.CLOSE_DURABLE_CLIENTS__MEMBER, required = false) final String memberNameId,
                                   @RequestParam(value = CliStrings.CLOSE_DURABLE_CLIENTS__GROUP, required = false) final String[] groups)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CLIENTS);

    command.addOption(CliStrings.CLOSE_DURABLE_CLIENTS__CLIENT__ID, decode(durableClientId));

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.CLOSE_DURABLE_CLIENTS__MEMBER, memberNameId);
    }

    if (hasValue(groups)) {
      command.addOption(CliStrings.CLOSE_DURABLE_CLIENTS__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/durable-clients/{durable-client-id}/cqs/{durable-cq-name}", params = "op=close")
  @ResponseBody
  public String closeDurableContinuousQuery(@PathVariable("durable-client-id") final String durableClientId,
                                            @PathVariable("durable-cq-name")final String durableCqName,
                                            @RequestParam(value = CliStrings.CLOSE_DURABLE_CQS__MEMBER, required = false) final String memberNameId,
                                            @RequestParam(value = CliStrings.CLOSE_DURABLE_CQS__GROUP, required = false) final String[] groups)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CQS);

    command.addOption(CliStrings.CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID, decode(durableClientId));
    command.addOption(CliStrings.CLOSE_DURABLE_CQS__NAME, decode(durableCqName));

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.CLOSE_DURABLE_CQS__MEMBER, memberNameId);
    }

    if (hasValue(groups)) {
      command.addOption(CliStrings.CLOSE_DURABLE_CQS__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

}
