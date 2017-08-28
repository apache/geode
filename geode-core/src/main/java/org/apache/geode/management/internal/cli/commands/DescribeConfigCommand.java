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
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.MemberConfigurationInfo;
import org.apache.geode.management.internal.cli.functions.GetMemberConfigInformationFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DescribeConfigCommand implements GfshCommand {
  private final GetMemberConfigInformationFunction getMemberConfigFunction =
      new GetMemberConfigInformationFunction();

  @CliCommand(value = {CliStrings.DESCRIBE_CONFIG}, help = CliStrings.DESCRIBE_CONFIG__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result describeConfig(
      @CliOption(key = CliStrings.MEMBER, optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.DESCRIBE_CONFIG__MEMBER__HELP, mandatory = true) String memberNameOrId,
      @CliOption(key = CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS,
          help = CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS__HELP, unspecifiedDefaultValue = "true",
          specifiedDefaultValue = "true") boolean hideDefaults) {

    Result result = null;
    try {
      DistributedMember targetMember = null;

      if (memberNameOrId != null && !memberNameOrId.isEmpty()) {
        targetMember = CliUtil.getDistributedMemberByNameOrId(memberNameOrId);
      }
      if (targetMember != null) {
        ResultCollector<?, ?> rc =
            CliUtil.executeFunction(getMemberConfigFunction, hideDefaults, targetMember);
        ArrayList<?> output = (ArrayList<?>) rc.getResult();
        Object obj = output.get(0);

        if (obj != null && obj instanceof MemberConfigurationInfo) {
          MemberConfigurationInfo memberConfigInfo = (MemberConfigurationInfo) obj;

          CompositeResultData crd = ResultBuilder.createCompositeResultData();
          crd.setHeader(
              CliStrings.format(CliStrings.DESCRIBE_CONFIG__HEADER__TEXT, memberNameOrId));

          List<String> jvmArgsList = memberConfigInfo.getJvmInputArguments();
          TabularResultData jvmInputArgs = crd.addSection().addSection().addTable();

          for (String jvmArg : jvmArgsList) {
            jvmInputArgs.accumulate("JVM command line arguments", jvmArg);
          }

          addSection(crd, memberConfigInfo.getGfePropsSetUsingApi(),
              "GemFire properties defined using the API");
          addSection(crd, memberConfigInfo.getGfePropsRuntime(),
              "GemFire properties defined at the runtime");
          addSection(crd, memberConfigInfo.getGfePropsSetFromFile(),
              "GemFire properties defined with the property file");
          addSection(crd, memberConfigInfo.getGfePropsSetWithDefaults(),
              "GemFire properties using default values");
          addSection(crd, memberConfigInfo.getCacheAttributes(), "Cache attributes");

          List<Map<String, String>> cacheServerAttributesList =
              memberConfigInfo.getCacheServerAttributes();

          if (cacheServerAttributesList != null && !cacheServerAttributesList.isEmpty()) {
            CompositeResultData.SectionResultData cacheServerSection = crd.addSection();
            cacheServerSection.setHeader("Cache-server attributes");

            for (Map<String, String> cacheServerAttributes : cacheServerAttributesList) {
              addSubSection(cacheServerSection, cacheServerAttributes);
            }
          }
          result = ResultBuilder.buildResult(crd);
        }

      } else {
        ErrorResultData erd = ResultBuilder.createErrorResultData();
        erd.addLine(CliStrings.format(CliStrings.DESCRIBE_CONFIG__MEMBER__NOT__FOUND,
            new Object[] {memberNameOrId}));
        result = ResultBuilder.buildResult(erd);
      }
    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createGemFireErrorResult(CliStrings
          .format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, CliStrings.DESCRIBE_CONFIG));
    } catch (Exception e) {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      erd.addLine(e.getMessage());
      result = ResultBuilder.buildResult(erd);
    }
    return result;
  }

  private void addSection(CompositeResultData crd, Map<String, String> attrMap, String headerText) {
    if (attrMap != null && !attrMap.isEmpty()) {
      CompositeResultData.SectionResultData section = crd.addSection();
      section.setHeader(headerText);
      section.addSeparator('.');
      Set<String> attributes = new TreeSet<>(attrMap.keySet());

      for (String attribute : attributes) {
        String attributeValue = attrMap.get(attribute);
        section.addData(attribute, attributeValue);
      }
    }
  }

  private void addSubSection(CompositeResultData.SectionResultData section,
      Map<String, String> attrMap) {
    if (!attrMap.isEmpty()) {
      CompositeResultData.SectionResultData subSection = section.addSection();
      Set<String> attributes = new TreeSet<>(attrMap.keySet());
      subSection.setHeader("");

      for (String attribute : attributes) {
        String attributeValue = attrMap.get(attribute);
        subSection.addData(attribute, attributeValue);
      }
    }
  }
}
