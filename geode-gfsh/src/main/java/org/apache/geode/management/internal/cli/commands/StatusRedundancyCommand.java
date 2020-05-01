/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.i18n.CliStrings.REDUNDANCY_EXCLUDE_REGION;
import static org.apache.geode.management.internal.i18n.CliStrings.REDUNDANCY_EXCLUDE_REGION_HELP;
import static org.apache.geode.management.internal.i18n.CliStrings.REDUNDANCY_INCLUDE_REGION;
import static org.apache.geode.management.internal.i18n.CliStrings.REDUNDANCY_INCLUDE_REGION_HELP;
import static org.apache.geode.management.internal.i18n.CliStrings.STATUS_REDUNDANCY;
import static org.apache.geode.management.internal.i18n.CliStrings.STATUS_REDUNDANCY_HELP;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class StatusRedundancyCommand extends RedundancyCommand {
  @CliCommand(value = STATUS_REDUNDANCY, help = STATUS_REDUNDANCY_HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.READ)
  public ResultModel executeStatusRedundancy(
      @CliOption(key = REDUNDANCY_INCLUDE_REGION,
          help = REDUNDANCY_INCLUDE_REGION_HELP) String[] includeRegions,
      @CliOption(key = REDUNDANCY_EXCLUDE_REGION,
          help = REDUNDANCY_EXCLUDE_REGION_HELP) String[] excludeRegions) {

    return super.execute(includeRegions, excludeRegions, false, true);
  }
}
