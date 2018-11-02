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

import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__CONNECTION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__PDX_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__REGION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__TABLE_NAME;

import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class DescribeMappingCommand extends GfshCommand {
  static final String DESCRIBE_MAPPING = "describe jdbc-mapping";
  static final String DESCRIBE_MAPPING__HELP = EXPERIMENTAL + "Describe the specified jdbc mapping";
  static final String DESCRIBE_MAPPING__REGION_NAME = "region";
  static final String DESCRIBE_MAPPING__REGION_NAME__HELP =
      "Region name of the jdbc mapping to be described.";

  static final String RESULT_SECTION_NAME = "MappingDescription";

  @CliCommand(value = DESCRIBE_MAPPING, help = DESCRIBE_MAPPING__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel describeMapping(@CliOption(key = DESCRIBE_MAPPING__REGION_NAME,
      mandatory = true, help = DESCRIBE_MAPPING__REGION_NAME__HELP) String regionName) {
    RegionMapping mapping = null;

    Set<DistributedMember> members = findMembers(null, null);
    if (members.size() > 0) {
      DistributedMember targetMember = members.iterator().next();
      CliFunctionResult result = executeFunctionAndGetFunctionResult(
          new DescribeMappingFunction(), regionName, targetMember);
      if (result != null) {
        mapping = (RegionMapping) result.getResultObject();
      }
    } else {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    if (mapping == null) {
      throw new EntityNotFoundException(
          EXPERIMENTAL + "\n" + "mapping for region '" + regionName + "' not found");
    }

    ResultModel resultModel = new ResultModel();
    fillResultData(mapping, resultModel);
    resultModel.setHeader(EXPERIMENTAL);
    return resultModel;
  }

  private void fillResultData(RegionMapping mapping, ResultModel resultModel) {
    DataResultModel sectionModel = resultModel.addData(RESULT_SECTION_NAME);
    sectionModel.addData(CREATE_MAPPING__REGION_NAME, mapping.getRegionName());
    sectionModel.addData(CREATE_MAPPING__CONNECTION_NAME, mapping.getConnectionConfigName());
    sectionModel.addData(CREATE_MAPPING__TABLE_NAME, mapping.getTableName());
    sectionModel.addData(CREATE_MAPPING__PDX_NAME, mapping.getPdxName());
  }
}
