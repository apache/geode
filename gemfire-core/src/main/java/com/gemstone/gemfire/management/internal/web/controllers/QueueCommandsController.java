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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The QueueCommandsController class implements GemFire Management REST API web service endpoints for the
 * Gfsh Queue Commands.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.QueueCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 8.0
 */
@Controller("queueController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class QueueCommandsController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.POST, value = "/async-event-queues")
  @ResponseBody
  public String createAsyncEventQueue(@RequestParam(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID) final String asyncEventQueueId,
                                      @RequestParam(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER) final String listener,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE, required = false) final String[] listenerParametersValues,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP, required = false) final String[] groups,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PARALLEL, defaultValue = "false") final Boolean parallel,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION, defaultValue = "false") final Boolean enableBatchConflation,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE, defaultValue = "100") final Integer batchSize,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL, defaultValue = "1000") final Integer batchTimeInterval,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT, defaultValue = "false") final Boolean persistent,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE, required = false) final String diskStore,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS, defaultValue = "true") final Boolean diskSynchronous,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY, defaultValue = "100") final Integer maxQueueMemory,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS, defaultValue = "1") final Integer dispatcherThreads,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ORDERPOLICY, defaultValue = "KEY") final String orderPolicy,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__GATEWAYEVENTFILTER, required = false) final String[] gatewayEventFilters,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__SUBSTITUTION_FILTER, required = false) final String gatewaySubstitutionFilter)

  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_ASYNC_EVENT_QUEUE);

    command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, asyncEventQueueId);
    command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER, listener);

    if (hasValue(listenerParametersValues)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE, StringUtils.concat(
        listenerParametersValues, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(groups)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP, StringUtils.concat(groups,
        StringUtils.COMMA_DELIMITER));
    }

    command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PARALLEL, String.valueOf(Boolean.TRUE.equals(parallel)));
    command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION, String.valueOf(Boolean.TRUE.equals(enableBatchConflation)));

    if (hasValue(batchSize)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE, String.valueOf(batchSize));
    }

    if (hasValue(batchTimeInterval)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL, String.valueOf(batchTimeInterval));
    }

    command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT, String.valueOf(Boolean.TRUE.equals(persistent)));

    if (hasValue(diskStore)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE, diskStore);
    }

    command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS, String.valueOf(Boolean.TRUE.equals(diskSynchronous)));

    if (hasValue(maxQueueMemory)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY, String.valueOf(maxQueueMemory));
    }

    if (hasValue(dispatcherThreads)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS, String.valueOf(dispatcherThreads));
    }

    if (hasValue(orderPolicy)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ORDERPOLICY, orderPolicy);
    }

    if (hasValue(gatewayEventFilters)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__GATEWAYEVENTFILTER, StringUtils.concat(
          gatewayEventFilters, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(gatewaySubstitutionFilter)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__SUBSTITUTION_FILTER, gatewaySubstitutionFilter);
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/async-event-queues")
  @ResponseBody
  public String listAsyncEventQueues() {
    return processCommand(CliStrings.LIST_ASYNC_EVENT_QUEUES);
  }

}
