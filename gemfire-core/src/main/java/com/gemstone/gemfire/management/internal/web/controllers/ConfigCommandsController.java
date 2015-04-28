/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.controllers;

import java.io.IOException;
import java.util.concurrent.Callable;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.management.internal.web.util.ConvertUtils;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

/**
 * The ConfigCommandsController class implements GemFire Management REST API web service endpoints for the Gfsh
 * Config Commands.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.ConfigCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractMultiPartCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 8.0
 */
@Controller("configController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class ConfigCommandsController extends AbstractMultiPartCommandsController {

  @RequestMapping(method = RequestMethod.POST, value = "/config")
  @ResponseBody
  public String alterRuntime(@RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__GROUP, required = false) final String group,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__MEMBER, required = false) final String memberNameId,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, required = false) final Integer archiveDiskSpaceLimit,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, required = false) final Integer archiveFileSizeLimit,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, required = false) final Integer logDiskSpaceLimit,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, required = false) final Integer logFileSizeLimit,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, required = false) final String logLevel,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, required = false) final String statisticsArchiveFile,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, required = false) final Integer statisticsSampleRate,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, required = false) final Boolean enableStatistics,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__COPY__ON__READ, required = false) final Boolean copyOnRead,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__LOCK__LEASE, required = false) final Integer lockLease,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT, required = false) final Integer lockTimeout,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL, required = false) final Integer messageSyncInterval,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__SEARCH__TIMEOUT, required = false) final Integer searchTimeout)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__MEMBER, memberNameId);
    }

    if (hasValue(group)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__GROUP, group);
    }

    if (hasValue(archiveDiskSpaceLimit)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, Integer.toString(archiveDiskSpaceLimit));
    }

    if (hasValue(archiveFileSizeLimit)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, Integer.toString(archiveFileSizeLimit));
    }

    if (hasValue(logDiskSpaceLimit)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, Integer.toString(logDiskSpaceLimit));
    }

    if (hasValue(logFileSizeLimit)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, Integer.toString(logFileSizeLimit));
    }

    if (hasValue(logLevel)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, logLevel);
    }

    if (hasValue(statisticsArchiveFile)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, statisticsArchiveFile);
    }

    if (hasValue(statisticsSampleRate)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, Integer.toString(statisticsSampleRate));
    }

    if (hasValue(enableStatistics)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, Boolean.toString(enableStatistics));
    }

    if (hasValue(copyOnRead)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__COPY__ON__READ, Boolean.toString(copyOnRead));
    }

    if (hasValue(lockLease)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOCK__LEASE, Integer.toString(lockLease));
    }

    if (hasValue(lockTimeout)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT, Integer.toString(lockTimeout));
    }

    if (hasValue(messageSyncInterval)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL, Integer.toString(messageSyncInterval));
    }

    if (hasValue(searchTimeout)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__SEARCH__TIMEOUT, Integer.toString(searchTimeout));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/members/{member}/config")
  @ResponseBody
  public String describeConfig(@PathVariable("member") final String memberNameId,
                               @RequestParam(value = CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS, defaultValue = "true") final Boolean hideDefaults)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESCRIBE_CONFIG);

    command.addOption(CliStrings.DESCRIBE_CONFIG__MEMBER, decode(memberNameId));
    command.addOption(CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS, String.valueOf(hideDefaults));

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/config")
  public Callable<ResponseEntity<String>> exportConfig(@RequestParam(value = CliStrings.EXPORT_CONFIG__GROUP, required = false) final String[] groups,
                                                       @RequestParam(value = CliStrings.EXPORT_CONFIG__MEMBER, required = false) final String[] members,
                                                       @RequestParam(value = CliStrings.EXPORT_CONFIG__DIR, required = false) final String directory)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.EXPORT_CONFIG);

    if (hasValue(groups)) {
      command.addOption(CliStrings.EXPORT_CONFIG__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.EXPORT_CONFIG__MEMBER, StringUtils.concat(members, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(directory)) {
      command.addOption(CliStrings.EXPORT_CONFIG__DIR, decode(directory));
    }

    return new Callable<ResponseEntity<String>>() {
      @Override public ResponseEntity<String> call() throws Exception {
        return new ResponseEntity<String>(processCommand(command.toString()), HttpStatus.OK);
      }
    };
  }

  @RequestMapping(method = RequestMethod.GET, value = "/config/cluster")
  public Callable<ResponseEntity<String>> exportClusterConfig(@RequestParam(CliStrings.EXPORT_SHARED_CONFIG__FILE) final String zipFileName,
                                                             @RequestParam(value = CliStrings.EXPORT_SHARED_CONFIG__DIR, required = false) final String directory)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.EXPORT_SHARED_CONFIG);

    command.addOption(CliStrings.EXPORT_SHARED_CONFIG__FILE, zipFileName);

    if (hasValue(directory)) {
      command.addOption(CliStrings.EXPORT_SHARED_CONFIG__DIR, directory);
    }

    return new Callable<ResponseEntity<String>>() {
      @Override public ResponseEntity<String> call() throws Exception {
        return new ResponseEntity<String>(processCommand(command.toString()), HttpStatus.OK);
      }
    };
  }

  @RequestMapping(method = RequestMethod.POST, value = "/config/cluster")
  public Callable<ResponseEntity<String>> importClusterConfig(@RequestParam(RESOURCES_REQUEST_PARAMETER) final MultipartFile[] zipFileResources,
                                                             @RequestParam(value = CliStrings.IMPORT_SHARED_CONFIG__ZIP) final String zipFileName)
    throws IOException
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.IMPORT_SHARED_CONFIG);

    command.addOption(CliStrings.IMPORT_SHARED_CONFIG__ZIP, zipFileName);

    return new Callable<ResponseEntity<String>>() {
      @Override public ResponseEntity<String> call() throws Exception {
        return new ResponseEntity<String>(processCommand(command.toString(), ConvertUtils.convert(zipFileResources)), HttpStatus.OK);
      }
    };
  }

  @RequestMapping(method = RequestMethod.GET, value = "/services/cluster-config")
  @ResponseBody
  public String statusClusterConfig() {
    return processCommand(CliStrings.STATUS_SHARED_CONFIG);
  }

}
