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

import java.util.concurrent.Callable;

import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The DiskStoreCommandsController class implements GemFire Management REST API web service endpoints for the
 * Gfsh Disk Store Commands.
 * <p/>
 * @see org.apache.geode.management.internal.cli.commands.DiskStoreCommands
 * @see org.apache.geode.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since GemFire 8.0
 */
@Controller("diskStoreController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class DiskStoreCommandsController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.GET, value = "/diskstores")
  @ResponseBody
  public String listDiskStores() {
    return processCommand(CliStrings.LIST_DISK_STORE);
  }

  @RequestMapping(method = RequestMethod.POST, value = "/diskstores", params = "op=backup")
  public Callable<ResponseEntity<String>> backupDiskStore(@RequestParam(value = CliStrings.BACKUP_DISK_STORE__DISKDIRS) final String dir,
                                                          @RequestParam(value = CliStrings.BACKUP_DISK_STORE__BASELINEDIR, required = false) final String baselineDir)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.BACKUP_DISK_STORE);

    command.addOption(CliStrings.BACKUP_DISK_STORE__DISKDIRS, decode(dir));

    if (hasValue(baselineDir)) {
      command.addOption(CliStrings.BACKUP_DISK_STORE__BASELINEDIR, decode(baselineDir));
    }

    return getProcessCommandCallable(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/diskstores/{name}", params = "op=compact")
  public Callable<ResponseEntity<String>> compactDiskStore(@PathVariable("name") final String diskStoreNameId,
                                                           @RequestParam(value = CliStrings.COMPACT_DISK_STORE__GROUP, required = false) final String[] groups)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.COMPACT_DISK_STORE);

    command.addOption(CliStrings.COMPACT_DISK_STORE__NAME, decode(diskStoreNameId));

    if (hasValue(groups)) {
      command.addOption(CliStrings.COMPACT_DISK_STORE__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    return getProcessCommandCallable(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/diskstores")
  @ResponseBody
  public String createDiskStore(@RequestParam(CliStrings.CREATE_DISK_STORE__NAME) final String diskStoreNameId,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE) final String[] directoryAndSizes,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION, defaultValue = "false") final Boolean allowForceCompaction,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__AUTO_COMPACT, defaultValue = "true") final Boolean autoCompact,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__COMPACTION_THRESHOLD, defaultValue = "50") final Integer compactionThreshold,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__MAX_OPLOG_SIZE, defaultValue = "1024") final Integer maxOplogSize,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__QUEUE_SIZE, defaultValue = "0") final Integer queueSize,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__TIME_INTERVAL, defaultValue = "1000") final Long timeInterval,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__WRITE_BUFFER_SIZE, defaultValue = "32768") final Integer writeBufferSize,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__DISK_USAGE_WARNING_PCT, defaultValue = "90") final Float diskUsageWarningPercentage,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__DISK_USAGE_CRITICAL_PCT, defaultValue = "99") final Integer diskUsageCriticalPercentage,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__GROUP, required = false) final String[] groups)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);

    command.addOption(CliStrings.CREATE_DISK_STORE__NAME, diskStoreNameId);
    command.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, StringUtils.concat(directoryAndSizes, StringUtils.COMMA_DELIMITER));
    command.addOption(CliStrings.CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION, String.valueOf(Boolean.TRUE.equals(allowForceCompaction)));
    command.addOption(CliStrings.CREATE_DISK_STORE__AUTO_COMPACT, String.valueOf(Boolean.TRUE.equals(autoCompact)));
    command.addOption(CliStrings.CREATE_DISK_STORE__COMPACTION_THRESHOLD, String.valueOf(compactionThreshold));
    command.addOption(CliStrings.CREATE_DISK_STORE__MAX_OPLOG_SIZE, String.valueOf(maxOplogSize));
    command.addOption(CliStrings.CREATE_DISK_STORE__QUEUE_SIZE, String.valueOf(queueSize));
    command.addOption(CliStrings.CREATE_DISK_STORE__TIME_INTERVAL, String.valueOf(timeInterval));
    command.addOption(CliStrings.CREATE_DISK_STORE__WRITE_BUFFER_SIZE, String.valueOf(writeBufferSize));
    command.addOption(CliStrings.CREATE_DISK_STORE__DISK_USAGE_WARNING_PCT, String.valueOf(diskUsageWarningPercentage));
    command.addOption(CliStrings.CREATE_DISK_STORE__DISK_USAGE_CRITICAL_PCT, String.valueOf(diskUsageCriticalPercentage));

    if (hasValue(groups)) {
      command.addOption(CliStrings.CREATE_DISK_STORE__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/diskstores/{name}")
  @ResponseBody
  public String describeDiskStore(@PathVariable("name") final String diskStoreNameId,
                                  @RequestParam(CliStrings.DESCRIBE_DISK_STORE__MEMBER) final String memberNameId)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESCRIBE_DISK_STORE);
    command.addOption(CliStrings.DESCRIBE_DISK_STORE__MEMBER, memberNameId);
    command.addOption(CliStrings.DESCRIBE_DISK_STORE__NAME, decode(diskStoreNameId));
    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(method = RequestMethod.DELETE, value = "/diskstores/{name}")
  @ResponseBody
  public String destroyDiskStore(@PathVariable("name") final String diskStoreNameId,
                                 @RequestParam(value = CliStrings.DESTROY_DISK_STORE__GROUP, required = false) final String[] groups)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESTROY_DISK_STORE);

    command.addOption(CliStrings.DESTROY_DISK_STORE__NAME, decode(diskStoreNameId));

    if (hasValue(groups)) {
      command.addOption(CliStrings.DESTROY_DISK_STORE__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(method = RequestMethod.POST, value = "/diskstores/{id}", params = "op=revoke")
  @ResponseBody
  public String revokeMissingDiskStore(@PathVariable("id") final String diskStoreId) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.REVOKE_MISSING_DISK_STORE);
    command.addOption(CliStrings.REVOKE_MISSING_DISK_STORE__ID, decode(diskStoreId));
    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/diskstores/missing")
  @ResponseBody
  public String showMissingDiskStores() {
    return processCommand(CliStrings.SHOW_MISSING_DISK_STORE);
  }

}
