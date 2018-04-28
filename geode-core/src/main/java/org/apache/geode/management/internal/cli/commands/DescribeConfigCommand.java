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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.util.ArgumentRedactor;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.MemberConfigurationInfo;
import org.apache.geode.management.internal.cli.functions.GetMemberConfigInformationFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DescribeConfigCommand extends InternalGfshCommand {
  private final GetMemberConfigInformationFunction getMemberConfigFunction =
      new GetMemberConfigInformationFunction();

  @CliCommand(value = {CliStrings.DESCRIBE_CONFIG}, help = CliStrings.DESCRIBE_CONFIG__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel describeConfig(
      @CliOption(key = CliStrings.MEMBER, optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.DESCRIBE_CONFIG__MEMBER__HELP, mandatory = true) String memberNameOrId,
      @CliOption(key = CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS,
          help = CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS__HELP, unspecifiedDefaultValue = "true",
          specifiedDefaultValue = "true") boolean hideDefaults) {

    ResultModel result = new ResultModel();
    try {
      DistributedMember targetMember = null;

      if (memberNameOrId != null && !memberNameOrId.isEmpty()) {
        targetMember = getMember(memberNameOrId);
      }

      ResultCollector<?, ?> rc =
          executeFunction(getMemberConfigFunction, hideDefaults, targetMember);
      ArrayList<?> output = (ArrayList<?>) rc.getResult();
      Object obj = output.get(0);

      if (obj != null && obj instanceof MemberConfigurationInfo) {
        MemberConfigurationInfo memberConfigInfo = (MemberConfigurationInfo) obj;

        result
            .setHeader(CliStrings.format(CliStrings.DESCRIBE_CONFIG__HEADER__TEXT, memberNameOrId));

        List<String> jvmArgsList = memberConfigInfo.getJvmInputArguments();
        TabularResultModel jvmInputArgs = result.addTable();

        for (String jvmArg : jvmArgsList) {
          // This redaction should be redundant, since jvmArgs should have already been redacted in
          // MemberConfigurationInfo. Still, better redundant than missing.
          jvmInputArgs.accumulate("JVM command line arguments", ArgumentRedactor.redact(jvmArg));
        }

        addSection(result, memberConfigInfo.getGfePropsSetUsingApi(),
            "GemFire properties defined using the API");
        addSection(result, memberConfigInfo.getGfePropsRuntime(),
            "GemFire properties defined at the runtime");
        addSection(result, memberConfigInfo.getGfePropsSetFromFile(),
            "GemFire properties defined with the property file");
        addSection(result, memberConfigInfo.getGfePropsSetWithDefaults(),
            "GemFire properties using default values");
        addSection(result, memberConfigInfo.getCacheAttributes(), "Cache attributes");

        List<Map<String, String>> cacheServerAttributesList =
            memberConfigInfo.getCacheServerAttributes();

        if (cacheServerAttributesList != null && !cacheServerAttributesList.isEmpty()) {
          for (Map<String, String> cacheServerAttributes : cacheServerAttributesList) {
            addSection(result, cacheServerAttributes, "Cache-server attributes");
          }
        }
      }

    } catch (FunctionInvocationTargetException e) {
      result.createCommandProcessingError(CliStrings
          .format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, CliStrings.DESCRIBE_CONFIG));
    } catch (Exception e) {
      result.createError(e.getMessage());
      result.setStatus(Result.Status.ERROR);
    }
    return result;
  }

  private void addSection(ResultModel model, Map<String, String> attrMap, String headerText) {
    if (attrMap != null && !attrMap.isEmpty()) {
      DataResultModel dataSection = model.addData();
      dataSection.setHeader(headerText);
      Set<String> attributes = new TreeSet<>(attrMap.keySet());

      for (String attribute : attributes) {
        String attributeValue = attrMap.get(attribute);
        dataSection.addData(attribute, attributeValue);
      }
    }
  }

}
