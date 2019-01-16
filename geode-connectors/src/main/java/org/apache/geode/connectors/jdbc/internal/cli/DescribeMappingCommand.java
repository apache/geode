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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.apache.geode.connectors.util.internal.MappingConstants.REGION_NAME;

import java.util.Set;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.connectors.util.internal.DescribeMappingResult;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class DescribeMappingCommand extends GfshCommand {
  static final String DESCRIBE_MAPPING = "describe jdbc-mapping";
  private static final String DESCRIBE_MAPPING__HELP =
      EXPERIMENTAL + "Describe the specified JDBC mapping";
  private static final String DESCRIBE_MAPPING__REGION_NAME = REGION_NAME;
  private static final String DESCRIBE_MAPPING__REGION_NAME__HELP =
      "Region name of the JDBC mapping to be described.";

  public static final String RESULT_SECTION_NAME = "MappingDescription";

  @CliCommand(value = DESCRIBE_MAPPING, help = DESCRIBE_MAPPING__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel describeMapping(@CliOption(key = DESCRIBE_MAPPING__REGION_NAME,
      mandatory = true, help = DESCRIBE_MAPPING__REGION_NAME__HELP) String regionName) {
    if (regionName.startsWith("/")) {
      regionName = regionName.substring(1);
    }

    DescribeMappingResult describeMappingResult = null;

    Set<DistributedMember> members = findMembers(null, null);
    if (members.size() > 0) {
      DistributedMember targetMember = members.iterator().next();
      CliFunctionResult result = executeFunctionAndGetFunctionResult(
          new DescribeMappingFunction(), regionName, targetMember);
      if (result != null) {
        describeMappingResult = (DescribeMappingResult) result.getResultObject();
      }
    } else {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    if (describeMappingResult == null) {
      throw new EntityNotFoundException(
          EXPERIMENTAL + "\n" + "JDBC mapping for region '" + regionName + "' not found");
    }

    ResultModel resultModel = new ResultModel();
    fillResultData(describeMappingResult, resultModel);
    resultModel.setHeader(EXPERIMENTAL);
    return resultModel;
  }

  private void fillResultData(DescribeMappingResult describeMappingResult,
      ResultModel resultModel) {
    DataResultModel sectionModel = resultModel.addData(RESULT_SECTION_NAME);
    describeMappingResult.getAttributeMap().forEach(sectionModel::addData);
  }

  @CliAvailabilityIndicator({DESCRIBE_MAPPING})
  public boolean commandAvailable() {
    return isOnlineCommandAvailable();
  }
}
