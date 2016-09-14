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

package org.apache.geode.management.internal.web.controllers;

import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The WanCommandsController class implements the GemFire Management REST API web service endpoints for the
 * Gfsh WAN Commands.
 * 
 * @see org.apache.geode.management.internal.cli.commands.WanCommands
 * @see org.apache.geode.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since GemFire 8.0
 */
@Controller("wanController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class WanCommandsController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.GET, value = "/gateways")
  @ResponseBody
  public String listGateways(@RequestParam(value = CliStrings.LIST_GATEWAY__GROUP, required = false) final String[] groups,
                             @RequestParam(value = CliStrings.LIST_GATEWAY__MEMBER, required = false) final String[] members)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.LIST_GATEWAY);

    if (hasValue(groups)) {
      command.addOption(CliStrings.LIST_GATEWAY__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.LIST_GATEWAY__MEMBER, StringUtils.concat(members, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/gateways/receivers")
  @ResponseBody
  public String createGatewayReceiver(@RequestParam(value = CliStrings.CREATE_GATEWAYRECEIVER__GROUP, required = false) final String[] groups,
                                      @RequestParam(value = CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART, required = false) final Boolean manualStart,
                                      @RequestParam(value = CliStrings.CREATE_GATEWAYRECEIVER__MEMBER, required = false) final String[] members,
                                      @RequestParam(value = CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT, required = false) final Integer startPort,
                                      @RequestParam(value = CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT, required = false) final Integer endPort,
                                      @RequestParam(value = CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS, required = false) final String bindAddress,
                                      @RequestParam(value = CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS, required = false) final Integer maximumTimeBetweenPings,
                                      @RequestParam(value = CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE, required = false) final Integer socketBufferSize,
                                      @RequestParam(value = CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER, required = false) final String[] gatewayTransportFilters)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYRECEIVER);

    if (hasValue(groups)) {
      command.addOption(CliStrings.CREATE_GATEWAYRECEIVER__GROUP, StringUtils.concat(groups,
        StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(manualStart)) {
      command.addOption(CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART, String.valueOf(manualStart));
    }
    
    if (hasValue(members)) {
      command.addOption(CliStrings.CREATE_GATEWAYRECEIVER__MEMBER, StringUtils.concat(members,
        StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(startPort)) {
      command.addOption(CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT, String.valueOf(startPort));
    }

    if (hasValue(endPort)) {
      command.addOption(CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT, String.valueOf(endPort));
    }

    if (hasValue(bindAddress)) {
      command.addOption(CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS, bindAddress);
    }

    if (hasValue(maximumTimeBetweenPings)) {
      command.addOption(CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS, String.valueOf(maximumTimeBetweenPings));
    }

    if (hasValue(socketBufferSize)) {
      command.addOption(CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE, String.valueOf(socketBufferSize));
    }

    if (hasValue(gatewayTransportFilters)) {
      command.addOption(CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER, StringUtils.concat(
        gatewayTransportFilters, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/gateways/senders")
  @ResponseBody
  public String createGatewaySender(@RequestParam(CliStrings.CREATE_GATEWAYSENDER__ID) final String gatewaySenderId,
                                    @RequestParam(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID) final Integer remoteDistributedSystemId,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__GROUP, required = false) final String[] groups,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__MEMBER, required = false) final String[] members,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__PARALLEL, required = false) final Boolean parallel,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, required = false) final Boolean manualStart,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE, required = false) final Integer socketBufferSize,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT, required = false) final Integer socketReadTimeout,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION, required = false) final Boolean enableBatchConflation,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE, required = false) final Integer batchSize,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL, required = false) final Integer batchTimeInterval,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE, required = false) final Boolean enablePersistence,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__DISKSTORENAME, required = false) final String diskStoreName,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__DISKSYNCHRONOUS, required = false) final Boolean diskSynchronous,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY, required = false) final Integer maxQueueMemory,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD, required = false) final Integer alertThreshold,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS, required = false) final Integer dispatcherThreads,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY, required = false) final String orderPolicy,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER, required = false) final String[] gatewayEventFilters,
                                    @RequestParam(value = CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER, required = false) final String[] gatewayTransportFilters)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);

    command.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, gatewaySenderId);
    command.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, String.valueOf(remoteDistributedSystemId));

    if (hasValue(groups)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__MEMBER, StringUtils.concat(members, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(parallel)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, String.valueOf(parallel));
    }

    if (hasValue(manualStart)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, String.valueOf(manualStart));
    }

    if (hasValue(socketBufferSize)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE, String.valueOf(socketBufferSize));
    }

    if (hasValue(socketReadTimeout)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT, String.valueOf(socketReadTimeout));
    }

    if (hasValue(enableBatchConflation)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION, String.valueOf(enableBatchConflation));
    }

    if (hasValue(batchSize)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE, String.valueOf(batchSize));
    }

    if (hasValue(batchTimeInterval)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL, String.valueOf(batchTimeInterval));
    }

    if (hasValue(enablePersistence)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE, String.valueOf(enablePersistence));
    }

    if (hasValue(diskStoreName)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__DISKSTORENAME, diskStoreName);
    }

    if (hasValue(diskSynchronous)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__DISKSYNCHRONOUS, String.valueOf(diskSynchronous));
    }

    if (hasValue(maxQueueMemory)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY, String.valueOf(maxQueueMemory));
    }

    if (hasValue(alertThreshold)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD, String.valueOf(alertThreshold));
    }

    if (hasValue(dispatcherThreads)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS, String.valueOf(dispatcherThreads));
    }

    if (hasValue(orderPolicy)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY, orderPolicy);
    }

    if (hasValue(gatewayEventFilters)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER, StringUtils.concat(gatewayEventFilters,
        StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(gatewayTransportFilters)) {
      command.addOption(CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER, StringUtils.concat(
        gatewayTransportFilters, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/gateways/senders/{id}", params = "op=load-balance")
  @ResponseBody
  public String loadBalanceGatewaySender(@PathVariable("id") final String gatewaySenderId) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.LOAD_BALANCE_GATEWAYSENDER);

    command.addOption(CliStrings.LOAD_BALANCE_GATEWAYSENDER__ID, decode(gatewaySenderId));

    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(method = RequestMethod.POST, value = "/gateways/senders/{id}", params = "op=pause")
  @ResponseBody
  public String pauseGatewaySender(@PathVariable("id") final String gatewaySenderId,
                                   @RequestParam(value = CliStrings.PAUSE_GATEWAYSENDER__GROUP, required = false) final String[] groups,
                                   @RequestParam(value = CliStrings.PAUSE_GATEWAYSENDER__MEMBER, required = false) final String[] members)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.PAUSE_GATEWAYSENDER);

    command.addOption(CliStrings.PAUSE_GATEWAYSENDER__ID, decode(gatewaySenderId));

    if (hasValue(groups)) {
      command.addOption(CliStrings.PAUSE_GATEWAYSENDER__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.PAUSE_GATEWAYSENDER__MEMBER, StringUtils.concat(members, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(method = RequestMethod.POST, value = "/gateways/senders/{id}", params = "op=resume")
  @ResponseBody
  public String resumeGatewaySender(@PathVariable("id") final String gatewaySenderId,
                                    @RequestParam(value = CliStrings.RESUME_GATEWAYSENDER__GROUP, required = false) final String[] groups,
                                    @RequestParam(value = CliStrings.RESUME_GATEWAYSENDER__MEMBER, required = false) final String[] members)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.RESUME_GATEWAYSENDER);

    command.addOption(CliStrings.RESUME_GATEWAYSENDER__ID, decode(gatewaySenderId));

    if (hasValue(groups)) {
      command.addOption(CliStrings.RESUME_GATEWAYSENDER__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.RESUME_GATEWAYSENDER__MEMBER, StringUtils.concat(members, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(method = RequestMethod.POST, value = "/gateways/receivers", params = "op=start")
  @ResponseBody
  public String startGatewayReceiver(@RequestParam(value = CliStrings.START_GATEWAYRECEIVER__GROUP, required = false) final String[] groups,
                                     @RequestParam(value = CliStrings.START_GATEWAYRECEIVER__MEMBER, required = false) final String[] members)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.START_GATEWAYRECEIVER);

    if (hasValue(groups)) {
      command.addOption(CliStrings.START_GATEWAYRECEIVER__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.START_GATEWAYRECEIVER__MEMBER, StringUtils.concat(members, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(method = RequestMethod.POST, value = "/gateways/senders", params = "op=start")
  @ResponseBody
  public String startGatewaySender(@RequestParam(CliStrings.START_GATEWAYSENDER__ID) final String gatewaySenderId,
                                   @RequestParam(value = CliStrings.START_GATEWAYSENDER__GROUP, required = false) final String[] groups,
                                   @RequestParam(value = CliStrings.START_GATEWAYSENDER__MEMBER, required = false) final String[] members)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER);

    command.addOption(CliStrings.START_GATEWAYSENDER__ID, gatewaySenderId);

    if (hasValue(groups)) {
      command.addOption(CliStrings.START_GATEWAYSENDER__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.START_GATEWAYSENDER__MEMBER, StringUtils.concat(members, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/gateways/receivers")
  @ResponseBody
  public String statusGatewayReceivers(@RequestParam(value = CliStrings.STATUS_GATEWAYRECEIVER__GROUP, required = false) final String[] groups,
                                       @RequestParam(value = CliStrings.STATUS_GATEWAYRECEIVER__MEMBER, required = false) final String[] members)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.STATUS_GATEWAYRECEIVER);

    if (hasValue(groups)) {
      command.addOption(CliStrings.STATUS_GATEWAYRECEIVER__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.STATUS_GATEWAYRECEIVER__MEMBER, StringUtils.concat(members, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/gateways/senders/{id}")
  @ResponseBody
  public String statusGatewaySenders(@PathVariable("id") final String gatewaySenderId,
                                     @RequestParam(value = CliStrings.STATUS_GATEWAYSENDER__GROUP, required = false) final String[] groups,
                                     @RequestParam(value = CliStrings.STATUS_GATEWAYSENDER__MEMBER, required = false) final String[] members)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.STATUS_GATEWAYSENDER);

    command.addOption(CliStrings.STATUS_GATEWAYSENDER__ID, decode(gatewaySenderId));

    if (hasValue(groups)) {
      command.addOption(CliStrings.STATUS_GATEWAYSENDER__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.STATUS_GATEWAYSENDER__MEMBER, StringUtils.concat(members, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/gateways/receivers", params = "op=stop")
  @ResponseBody
  public String stopGatewayReceiver(@RequestParam(value = CliStrings.STOP_GATEWAYRECEIVER__GROUP, required = false) final String[] groups,
                                    @RequestParam(value = CliStrings.STOP_GATEWAYRECEIVER__MEMBER, required = false) final String[] members)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.STOP_GATEWAYRECEIVER);

    if (hasValue(groups)) {
      command.addOption(CliStrings.STOP_GATEWAYRECEIVER__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.STOP_GATEWAYRECEIVER__MEMBER, StringUtils.concat(members, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/gateways/senders/{id}", params = "op=stop")
  @ResponseBody
  public String stopGatewaySender(@PathVariable("id") final String gatewaySenderId,
                                  @RequestParam(value = CliStrings.STOP_GATEWAYRECEIVER__GROUP, required = false) final String[] groups,
                                  @RequestParam(value = CliStrings.STOP_GATEWAYRECEIVER__MEMBER, required = false) final String[] members)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.STOP_GATEWAYSENDER);

    command.addOption(CliStrings.STOP_GATEWAYSENDER__ID, decode(gatewaySenderId));

    if (hasValue(groups)) {
      command.addOption(CliStrings.STOP_GATEWAYSENDER__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.STOP_GATEWAYSENDER__MEMBER, StringUtils.concat(members, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

}
