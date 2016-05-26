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

import java.util.concurrent.Callable;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The MiscellaneousCommandsController class implements GemFire Management REST API web service endpoints for the
 * Gfsh Miscellaneous Commands.
 * <p/>
 * @see com.gemstone.gemfire.management.internal.cli.commands.MiscellaneousCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since GemFire 8.0
 */
@Controller("miscellaneousController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class MiscellaneousCommandsController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.GET, value = "/logs")
  public Callable<ResponseEntity<String>> exportLogs(@RequestParam(CliStrings.EXPORT_LOGS__DIR) final String directory,
                                                     @RequestParam(value = CliStrings.EXPORT_LOGS__GROUP, required = false) final String[] groups,
                                                     @RequestParam(value = CliStrings.EXPORT_LOGS__MEMBER, required = false) final String memberNameId,
                                                     @RequestParam(value = CliStrings.EXPORT_LOGS__LOGLEVEL, required = false) final String logLevel,
                                                     @RequestParam(value = CliStrings.EXPORT_LOGS__UPTO_LOGLEVEL, defaultValue = "false") final Boolean onlyLogLevel,
                                                     @RequestParam(value = CliStrings.EXPORT_LOGS__MERGELOG, defaultValue = "false") final Boolean mergeLog,
                                                     @RequestParam(value = CliStrings.EXPORT_LOGS__STARTTIME, required = false) final String startTime,
                                                     @RequestParam(value = CliStrings.EXPORT_LOGS__ENDTIME, required = false) final String endTime)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.EXPORT_LOGS);

    command.addOption(CliStrings.EXPORT_LOGS__DIR, decode(directory));

    if (hasValue(groups)) {
      command.addOption(CliStrings.EXPORT_LOGS__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.EXPORT_LOGS__MEMBER, memberNameId);
    }

    if (hasValue(logLevel)) {
      command.addOption(CliStrings.EXPORT_LOGS__LOGLEVEL, logLevel);
    }

    command.addOption(CliStrings.EXPORT_LOGS__UPTO_LOGLEVEL, String.valueOf(Boolean.TRUE.equals(onlyLogLevel)));
    command.addOption(CliStrings.EXPORT_LOGS__MERGELOG, String.valueOf(Boolean.TRUE.equals(mergeLog)));

    if (hasValue(startTime)) {
      command.addOption(CliStrings.EXPORT_LOGS__STARTTIME, startTime);
    }

    if (hasValue(endTime)) {
      command.addOption(CliStrings.EXPORT_LOGS__ENDTIME, endTime);
    }

    return getProcessCommandCallable(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(method = RequestMethod.GET, value = "/stacktraces")
  @ResponseBody
  public String exportStackTraces(@RequestParam(value = CliStrings.EXPORT_STACKTRACE__FILE) final String file,
                                  @RequestParam(value = CliStrings.EXPORT_STACKTRACE__GROUP, required = false) final String groupName,
                                  @RequestParam(value = CliStrings.EXPORT_STACKTRACE__MEMBER, required = false) final String memberNameId) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);

    command.addOption(CliStrings.EXPORT_STACKTRACE__FILE, decode(file));

    if (hasValue(groupName)) {
      command.addOption(CliStrings.EXPORT_STACKTRACE__GROUP, groupName);
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.EXPORT_STACKTRACE__MEMBER, memberNameId);
    }

    return processCommand(command.toString());
  }

  // TODO add Async functionality
  @RequestMapping(method = RequestMethod.POST, value = "/gc")
  @ResponseBody
  public String gc(@RequestParam(value = CliStrings.GC__GROUP, required = false) final String[] groups) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.GC);

    if (hasValue(groups)) {
      command.addOption(CliStrings.GC__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  // TODO add Async functionality
  @RequestMapping(method = RequestMethod.POST, value = "/members/{member}/gc")
  @ResponseBody
  public String gc(@PathVariable("member") final String memberNameId) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.GC);
    command.addOption(CliStrings.GC__MEMBER, decode(memberNameId));
    return processCommand(command.toString());
  }

  // TODO add Async functionality
  @RequestMapping(method = RequestMethod.GET, value = "/netstat")
  @ResponseBody
  public String netstat(@RequestParam(value = CliStrings.NETSTAT__MEMBER, required= false) final String[] members,
                        @RequestParam(value = CliStrings.NETSTAT__GROUP, required = false) final String group,
                        @RequestParam(value = CliStrings.NETSTAT__FILE, required = false) final String file,
                        @RequestParam(value = CliStrings.NETSTAT__WITHLSOF, defaultValue = "false") final Boolean withLsof)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.NETSTAT);

    addCommandOption(null, command, CliStrings.NETSTAT__MEMBER, members);
    addCommandOption(null, command, CliStrings.NETSTAT__GROUP, group);
    addCommandOption(null, command, CliStrings.NETSTAT__FILE, file);
    addCommandOption(null, command, CliStrings.NETSTAT__WITHLSOF, withLsof);

    return processCommand(command.toString());
  }

  // TODO determine if Async functionality is required
  @RequestMapping(method = RequestMethod.GET, value = "/deadlocks")
  @ResponseBody
  public String showDeadLock(@RequestParam(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE) final String dependenciesFile) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHOW_DEADLOCK);
    command.addOption(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE, decode(dependenciesFile));
    return processCommand(command.toString());
  }

  // TODO determine if Async functionality is required
  @RequestMapping(method = RequestMethod.GET, value = "/members/{member}/log")
  @ResponseBody
  public String showLog(@PathVariable("member") final String memberNameId,
                        @RequestParam(value = CliStrings.SHOW_LOG_LINE_NUM, defaultValue = "0") final Integer lines)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHOW_LOG);

    command.addOption(CliStrings.SHOW_LOG_MEMBER, decode(memberNameId));
    command.addOption(CliStrings.SHOW_LOG_LINE_NUM, String.valueOf(lines));

    return processCommand(command.toString());
  }

  // TODO determine if Async functionality is required
  @RequestMapping(method = RequestMethod.GET, value = "/metrics")
  @ResponseBody
  public String showMetrics(@RequestParam(value = CliStrings.SHOW_METRICS__MEMBER, required = false) final String memberNameId,
                            @RequestParam(value = CliStrings.SHOW_METRICS__REGION, required = false) final String regionNamePath,
                            @RequestParam(value = CliStrings.SHOW_METRICS__FILE, required = false) final String file,
                            @RequestParam(value = CliStrings.SHOW_METRICS__CACHESERVER__PORT, required = false) final String cacheServerPort,
                            @RequestParam(value = CliStrings.SHOW_METRICS__CATEGORY, required = false) final String[] categories)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHOW_METRICS);

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.SHOW_METRICS__MEMBER, memberNameId);
    }

    if (hasValue(regionNamePath)) {
      command.addOption(CliStrings.SHOW_METRICS__REGION, regionNamePath);
    }

    if (hasValue(file)) {
      command.addOption(CliStrings.SHOW_METRICS__FILE, file);
    }

    if (hasValue(cacheServerPort)) {
      command.addOption(CliStrings.SHOW_METRICS__CACHESERVER__PORT, cacheServerPort);
    }

    if (hasValue(categories)) {
      command.addOption(CliStrings.SHOW_METRICS__CATEGORY, StringUtils.concat(categories, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/shutdown")
  @ResponseBody
  public String shutdown(@RequestParam(value = CliStrings.SHUTDOWN__TIMEOUT, defaultValue = "-1") final Integer timeout) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHUTDOWN);
    command.addOption(CliStrings.SHUTDOWN__TIMEOUT, String.valueOf(timeout));
    return processCommand(command.toString());
  }

  // TODO determine whether the {groups} and {members} path variables corresponding to the --groups and --members
  // command-line options in the 'change loglevel' Gfsh command actually accept multiple values, and...
  // TODO if so, then change the groups and members method parameters to String[] types.
  // TODO If not, then these options should be renamed!

  @RequestMapping(method = RequestMethod.POST, value = "/groups/{groups}/loglevel")
  @ResponseBody
  public String changeLogLevelForGroups(@PathVariable("groups") final String groups,
                                        @RequestParam(value = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL, required = true) final String logLevel)
  {
    return internalChangeLogLevel(groups, null, logLevel);
  }

  @RequestMapping(method = RequestMethod.POST, value = "/members/{members}/loglevel")
  @ResponseBody
  public String changeLogLevelForMembers(@PathVariable("members") final String members,
                                         @RequestParam(value = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL, required = true) final String logLevel)
  {
    return internalChangeLogLevel(null, members, logLevel);
  }

  @RequestMapping(method = RequestMethod.POST, value = "/members/{members}/groups/{groups}/loglevel")
  @ResponseBody
  public String changeLogLevelForMembersAndGroups(@PathVariable("members") final String members,
                                                  @PathVariable("groups") final String groups,
                                                  @RequestParam(value = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL) final String logLevel)
  {
    return internalChangeLogLevel(groups, members, logLevel);
  }

  // NOTE since "logLevel" is "required", then just set the option; no need to validate it's value.
  private String internalChangeLogLevel(final String groups, final String members, final String logLevel) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.CHANGE_LOGLEVEL);

    command.addOption(CliStrings.CHANGE_LOGLEVEL__LOGLEVEL, decode(logLevel));

    if (hasValue(groups)) {
      command.addOption(CliStrings.CHANGE_LOGLEVEL__GROUPS, decode(groups));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.CHANGE_LOGLEVEL__MEMBER, decode(members));
    }

    return processCommand(command.toString());
  }

}
