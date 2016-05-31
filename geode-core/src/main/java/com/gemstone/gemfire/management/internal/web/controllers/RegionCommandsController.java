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

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.request.WebRequest;

/**
 * The RegionCommands class implements GemFire Management REST API web service endpoints for the Gfsh Region Commands.
 * <p/>
 * @see com.gemstone.gemfire.management.internal.cli.commands.RegionCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since GemFire 8.0
 */
@Controller("regionController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class RegionCommandsController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.GET, value = "/regions")
  @ResponseBody
  public String listRegions(@RequestParam(value = CliStrings.LIST_REGION__GROUP, required = false) final String groupName,
                            @RequestParam(value = CliStrings.LIST_REGION__MEMBER, required = false) final String memberNameId)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.LIST_REGION);

    if (hasValue(groupName)) {
      command.addOption(CliStrings.LIST_REGION__GROUP, groupName);
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.LIST_REGION__MEMBER, memberNameId);
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/regions/{name}")
  @ResponseBody
  public String describeRegion(@PathVariable("name") final String regionNamePath) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESCRIBE_REGION);
    command.addOption(CliStrings.DESCRIBE_REGION__NAME, decode(regionNamePath));
    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.PUT, value = "/regions/{name}")
  @ResponseBody
  public String alterRegion(final WebRequest request,
                            @PathVariable("name") final String regionNamePath,
                            @RequestParam(value = CliStrings.ALTER_REGION__GROUP, required = false) final String[] groups,
                            @RequestParam(value = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME, required = false) final Integer entryIdleTimeExpiration,
                            @RequestParam(value = CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION, required = false) final String entryIdleTimeExpirationAction,
                            @RequestParam(value = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE, required = false) final Integer entryTimeToLiveExpiration,
                            @RequestParam(value = CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION, required = false) final String entryTimeToLiveExpirationAction,
                            @RequestParam(value = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME, required = false) final Integer regionIdleTimeExpiration,
                            @RequestParam(value = CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION, required = false) final String regionIdleTimeExpirationAction,
                            @RequestParam(value = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTL, required = false) final Integer regionTimeToLiveExpiration,
                            @RequestParam(value = CliStrings.ALTER_REGION__REGIONEXPIRATIONTTLACTION, required = false) final String regionTimeToLiveExpirationAction,
                            @RequestParam(value = CliStrings.ALTER_REGION__CACHELISTENER, required = false) final String[] cacheListeners,
                            @RequestParam(value = CliStrings.ALTER_REGION__CACHELOADER, required = false) final String cacheLoader,
                            @RequestParam(value = CliStrings.ALTER_REGION__CACHEWRITER, required = false) final String cacheWriter,
                            @RequestParam(value = CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, required = false) final String[] asyncEventQueueIds,
                            @RequestParam(value = CliStrings.ALTER_REGION__GATEWAYSENDERID, required = false) final String[] gatewaySenderIds,
                            @RequestParam(value = CliStrings.ALTER_REGION__CLONINGENABLED, required = false) final Boolean enableCloning,
                            @RequestParam(value = CliStrings.ALTER_REGION__EVICTIONMAX, required = false) final Integer evictionMax)
  {
    //logRequest(request);

    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.ALTER_REGION);

    command.addOption(CliStrings.ALTER_REGION__REGION, decode(regionNamePath));

    if (hasValue(groups)) {
      command.addOption(CliStrings.ALTER_REGION__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    addCommandOption(request, command, CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME, entryIdleTimeExpiration);
    addCommandOption(request, command, CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION, entryIdleTimeExpirationAction);
    addCommandOption(request, command, CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE, entryTimeToLiveExpiration);
    addCommandOption(request, command, CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION, entryTimeToLiveExpirationAction);
    addCommandOption(request, command, CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME, regionIdleTimeExpiration);
    addCommandOption(request, command, CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION, regionIdleTimeExpirationAction);
    addCommandOption(request, command, CliStrings.ALTER_REGION__REGIONEXPIRATIONTTL, regionTimeToLiveExpiration);
    addCommandOption(request, command, CliStrings.ALTER_REGION__REGIONEXPIRATIONTTLACTION, regionTimeToLiveExpirationAction);
    addCommandOption(request, command, CliStrings.ALTER_REGION__CACHELISTENER, cacheListeners);
    addCommandOption(request, command, CliStrings.ALTER_REGION__CACHELOADER, cacheLoader);
    addCommandOption(request, command, CliStrings.ALTER_REGION__CACHEWRITER, cacheWriter);
    addCommandOption(request, command, CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, asyncEventQueueIds);
    addCommandOption(request, command, CliStrings.ALTER_REGION__GATEWAYSENDERID, gatewaySenderIds);

    if (Boolean.TRUE.equals(enableCloning)) {
      command.addOption(CliStrings.ALTER_REGION__CLONINGENABLED, String.valueOf(enableCloning));
    }

    if (hasValue(evictionMax)) {
      command.addOption(CliStrings.ALTER_REGION__EVICTIONMAX, String.valueOf(evictionMax));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/regions")
  @ResponseBody
  public String createRegion(@RequestParam("name") final String regionNamePath,
                             @RequestParam(value = CliStrings.CREATE_REGION__REGIONSHORTCUT, required = false) final String regionType,
                             @RequestParam(value = CliStrings.CREATE_REGION__USEATTRIBUTESFROM, required = false) final String regionTemplate,
                             @RequestParam(value = CliStrings.CREATE_REGION__GROUP, required = false) final String[] groups,
                             @RequestParam(value = CliStrings.CREATE_REGION__SKIPIFEXISTS, defaultValue = "true") final Boolean skipIfExists,
                             @RequestParam(value = CliStrings.CREATE_REGION__KEYCONSTRAINT, required = false) final String keyConstraint,
                             @RequestParam(value = CliStrings.CREATE_REGION__VALUECONSTRAINT, required = false) final String valueConstraint,
                             @RequestParam(value = CliStrings.CREATE_REGION__STATISTICSENABLED, required = false) final Boolean enableStatistics,
                             @RequestParam(value = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIME, required = false) final Integer entryIdleTimeExpiration,
                             @RequestParam(value = CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIMEACTION, required = false) final String entryIdleTimeExpirationAction,
                             @RequestParam(value = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTIMETOLIVE, required = false) final Integer entryTimeToLiveExpiration,
                             @RequestParam(value = CliStrings.CREATE_REGION__ENTRYEXPIRATIONTTLACTION, required = false) final String entryTimeToLiveExpirationAction,
                             @RequestParam(value = CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIME, required = false) final Integer regionIdleTimeExpiration,
                             @RequestParam(value = CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIMEACTION, required = false) final String regionIdleTimeExpirationAction,
                             @RequestParam(value = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTL, required = false) final Integer regionTimeToLiveExpiration,
                             @RequestParam(value = CliStrings.CREATE_REGION__REGIONEXPIRATIONTTLACTION, required = false) final String regionTimeToLiveExpirationAction,
                             @RequestParam(value = CliStrings.CREATE_REGION__DISKSTORE, required = false) final String diskStore,
                             @RequestParam(value = CliStrings.CREATE_REGION__DISKSYNCHRONOUS, required = false) final Boolean enableSynchronousDisk,
                             @RequestParam(value = CliStrings.CREATE_REGION__ENABLEASYNCCONFLATION, required = false) final Boolean enableAsynchronousConflation,
                             @RequestParam(value = CliStrings.CREATE_REGION__ENABLESUBSCRIPTIONCONFLATION, required = false) final Boolean enableSubscriptionConflation,
                             @RequestParam(value = CliStrings.CREATE_REGION__CACHELISTENER, required = false) final String[] cacheListeners,
                             @RequestParam(value = CliStrings.CREATE_REGION__CACHELOADER, required = false) final String cacheLoader,
                             @RequestParam(value = CliStrings.CREATE_REGION__CACHEWRITER, required = false) final String cacheWriter,
                             @RequestParam(value = CliStrings.CREATE_REGION__ASYNCEVENTQUEUEID, required = false) final String[] asyncEventQueueIds,
                             @RequestParam(value = CliStrings.CREATE_REGION__GATEWAYSENDERID, required = false) final String[] gatewaySenderIds,
                             @RequestParam(value = CliStrings.CREATE_REGION__CONCURRENCYCHECKSENABLED, required = false) final Boolean enableConcurrencyChecks,
                             @RequestParam(value = CliStrings.CREATE_REGION__CLONINGENABLED, required = false) final Boolean enableCloning,
                             @RequestParam(value = CliStrings.CREATE_REGION__CONCURRENCYLEVEL, required = false) final Integer concurrencyLevel,
                             @RequestParam(value = CliStrings.CREATE_REGION__COLOCATEDWITH, required = false) final String colocatedWith,
                             @RequestParam(value = CliStrings.CREATE_REGION__LOCALMAXMEMORY, required = false) final Integer localMaxMemory,
                             @RequestParam(value = CliStrings.CREATE_REGION__RECOVERYDELAY, required = false) final Long recoveryDelay,
                             @RequestParam(value = CliStrings.CREATE_REGION__REDUNDANTCOPIES, required = false) final Integer redundantCopies,
                             @RequestParam(value = CliStrings.CREATE_REGION__STARTUPRECOVERYDDELAY, required = false) final Long startupRecoveryDelay,
                             @RequestParam(value = CliStrings.CREATE_REGION__TOTALMAXMEMORY, required = false) final Long totalMaxMemory,
                             @RequestParam(value = CliStrings.CREATE_REGION__TOTALNUMBUCKETS, required = false) final Integer totalNumBuckets,
                             @RequestParam(value = CliStrings.CREATE_REGION__COMPRESSOR, required = false) final String compressor)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_REGION);

    assertArgument(hasValue(regionNamePath), "Region name/path is required!");

    assertArgument(hasValue(regionType) || hasValue(regionTemplate),
      "Either Region type or template-region must be specified!");

    command.addOption(CliStrings.CREATE_REGION__REGION, regionNamePath);

    if (hasValue(regionType)) {
      command.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionType);
    }
    else {
      command.addOption(CliStrings.CREATE_REGION__USEATTRIBUTESFROM, regionTemplate);
    }

    if (hasValue(groups)) {
      command.addOption(CliStrings.CREATE_REGION__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    command.addOption(CliStrings.CREATE_REGION__SKIPIFEXISTS, String.valueOf(Boolean.TRUE.equals(skipIfExists)));

    if (hasValue(keyConstraint)) {
      command.addOption(CliStrings.CREATE_REGION__KEYCONSTRAINT, keyConstraint);
    }

    if (hasValue(valueConstraint)) {
      command.addOption(CliStrings.CREATE_REGION__VALUECONSTRAINT, valueConstraint);
    }

    if (Boolean.TRUE.equals(enableStatistics)) {
      command.addOption(CliStrings.CREATE_REGION__STATISTICSENABLED, String.valueOf(enableStatistics));
    }

    if (hasValue(entryIdleTimeExpiration)) {
      command.addOption(CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIME, String.valueOf(entryIdleTimeExpiration));
    }

    if (hasValue(entryIdleTimeExpirationAction)) {
      command.addOption(CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIMEACTION, entryIdleTimeExpirationAction);
    }

    if (hasValue(entryTimeToLiveExpiration)) {
      command.addOption(CliStrings.CREATE_REGION__ENTRYEXPIRATIONTIMETOLIVE, String.valueOf(entryTimeToLiveExpiration));
    }

    if (hasValue(entryTimeToLiveExpirationAction)) {
      command.addOption(CliStrings.CREATE_REGION__ENTRYEXPIRATIONTTLACTION, entryTimeToLiveExpirationAction);
    }

    if (hasValue(regionIdleTimeExpiration)) {
      command.addOption(CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIME, String.valueOf(regionIdleTimeExpiration));
    }

    if (hasValue(regionIdleTimeExpirationAction)) {
      command.addOption(CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIMEACTION, regionIdleTimeExpirationAction);
    }

    if (hasValue(regionTimeToLiveExpiration)) {
      command.addOption(CliStrings.CREATE_REGION__REGIONEXPIRATIONTTL, String.valueOf(regionTimeToLiveExpiration));
    }

    if (hasValue(regionTimeToLiveExpirationAction)) {
      command.addOption(CliStrings.CREATE_REGION__REGIONEXPIRATIONTTLACTION, regionTimeToLiveExpirationAction);
    }

    if (hasValue(diskStore)) {
      command.addOption(CliStrings.CREATE_REGION__DISKSTORE, diskStore);
    }

    if (Boolean.TRUE.equals(enableSynchronousDisk)) {
      command.addOption(CliStrings.CREATE_REGION__DISKSYNCHRONOUS, String.valueOf(enableSynchronousDisk));
    }

    if (Boolean.TRUE.equals(enableAsynchronousConflation)) {
      command.addOption(CliStrings.CREATE_REGION__ENABLEASYNCCONFLATION, String.valueOf(enableAsynchronousConflation));
    }

    if (Boolean.TRUE.equals(enableSubscriptionConflation)) {
      command.addOption(CliStrings.CREATE_REGION__ENABLESUBSCRIPTIONCONFLATION,
        String.valueOf(enableSubscriptionConflation));
    }

    if (hasValue(cacheListeners)) {
      command.addOption(CliStrings.CREATE_REGION__CACHELISTENER, StringUtils.concat(cacheListeners,
        StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(cacheLoader)) {
      command.addOption(CliStrings.CREATE_REGION__CACHELOADER, cacheLoader);
    }

    if (hasValue(cacheWriter)) {
      command.addOption(CliStrings.CREATE_REGION__CACHEWRITER, cacheWriter);
    }

    if (hasValue(asyncEventQueueIds)) {
      command.addOption(CliStrings.CREATE_REGION__ASYNCEVENTQUEUEID, StringUtils.concat(asyncEventQueueIds,
        StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(gatewaySenderIds)) {
      command.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, StringUtils.concat(gatewaySenderIds,
        StringUtils.COMMA_DELIMITER));
    }

    if (Boolean.TRUE.equals(enableConcurrencyChecks)) {
      command.addOption(CliStrings.CREATE_REGION__CONCURRENCYCHECKSENABLED, String.valueOf(enableConcurrencyChecks));
    }

    if (Boolean.TRUE.equals(enableCloning)) {
      command.addOption(CliStrings.CREATE_REGION__CLONINGENABLED, String.valueOf(enableCloning));
    }

    if (hasValue(concurrencyLevel)) {
      command.addOption(CliStrings.CREATE_REGION__CONCURRENCYLEVEL, String.valueOf(concurrencyLevel));
    }

    if (hasValue(colocatedWith)) {
      command.addOption(CliStrings.CREATE_REGION__COLOCATEDWITH, colocatedWith);
    }

    if (hasValue(localMaxMemory)) {
      command.addOption(CliStrings.CREATE_REGION__LOCALMAXMEMORY, String.valueOf(localMaxMemory));
    }

    if (hasValue(recoveryDelay)) {
      command.addOption(CliStrings.CREATE_REGION__RECOVERYDELAY, String.valueOf(recoveryDelay));
    }

    if (hasValue(redundantCopies)) {
      command.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, String.valueOf(redundantCopies));
    }

    if (hasValue(startupRecoveryDelay)) {
      command.addOption(CliStrings.CREATE_REGION__STARTUPRECOVERYDDELAY, String.valueOf(startupRecoveryDelay));
    }

    if (hasValue(totalMaxMemory)) {
      command.addOption(CliStrings.CREATE_REGION__TOTALMAXMEMORY, String.valueOf(totalMaxMemory));
    }

    if (hasValue(totalNumBuckets)) {
      command.addOption(CliStrings.CREATE_REGION__TOTALNUMBUCKETS, String.valueOf(totalNumBuckets));
    }

    if (hasValue(compressor)) {
      command.addOption(CliStrings.CREATE_REGION__COMPRESSOR, compressor);
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.DELETE, value = "/regions/{name}")
  @ResponseBody
  public String destroyRegion(@PathVariable("name") final String regionNamePath) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESTROY_REGION);
    command.addOption(CliStrings.DESTROY_REGION__REGION, decode(regionNamePath));
    return processCommand(command.toString());
  }

}
