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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.domain.FixedPartitionAttributesInfo;
import org.apache.geode.management.internal.cli.domain.RegionDescription;
import org.apache.geode.management.internal.cli.domain.RegionDescriptionPerMember;
import org.apache.geode.management.internal.cli.functions.GetRegionDescriptionFunction;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.cli.util.RegionAttributesNames;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DescribeRegionCommand extends GfshCommand {
  public static final Logger logger = LogService.getLogger();

  @Immutable
  private static final GetRegionDescriptionFunction getRegionDescription =
      new GetRegionDescriptionFunction();

  @CliCommand(value = {CliStrings.DESCRIBE_REGION}, help = CliStrings.DESCRIBE_REGION__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel describeRegion(
      @CliOption(key = CliStrings.DESCRIBE_REGION__NAME, optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.DESCRIBE_REGION__NAME__HELP, mandatory = true) String regionName) {

    List<?> resultList = getFunctionResultFromMembers(regionName);

    // Log any errors received.
    resultList.stream().filter(Throwable.class::isInstance).map(Throwable.class::cast)
        .forEach(t -> logger.info(t.getMessage(), t));

    // Region descriptions are grouped on name, scope, data-policy and member-type (accessor vs
    // hosting member).
    Map<String, List<RegionDescriptionPerMember>> perTypeDescriptions =
        resultList.stream().filter(RegionDescriptionPerMember.class::isInstance)
            .map(RegionDescriptionPerMember.class::cast)
            .collect(Collectors.groupingBy(this::descriptionGrouper));

    List<RegionDescription> regionDescriptions = new ArrayList<>();

    for (List<RegionDescriptionPerMember> regionDescPerMemberType : perTypeDescriptions.values()) {
      RegionDescription regionDescription = new RegionDescription();
      for (RegionDescriptionPerMember regionDescPerMember : regionDescPerMemberType) {
        regionDescription.add(regionDescPerMember);
      }
      // No point in displaying the scope for PR's
      if (regionDescription.isPartition()) {
        regionDescription.getCndRegionAttributes().remove(RegionAttributesNames.SCOPE);
      } else {
        String scope = regionDescription.getCndRegionAttributes().get(RegionAttributesNames.SCOPE);
        if (scope != null) {
          scope = scope.toLowerCase().replace('_', '-');
          regionDescription.getCndRegionAttributes().put(RegionAttributesNames.SCOPE, scope);
        }
      }
      regionDescriptions.add(regionDescription);
    }

    return buildDescriptionResult(regionName, regionDescriptions);
  }

  private String descriptionGrouper(RegionDescriptionPerMember perTypeDesc) {
    return perTypeDesc.getName() + perTypeDesc.getScope() + perTypeDesc.getDataPolicy()
        + perTypeDesc.isAccessor();
  }

  List<?> getFunctionResultFromMembers(String regionName) {
    ResultCollector<?, ?> rc =
        executeFunction(getRegionDescription, regionName, getAllNormalMembers());

    return (List<?>) rc.getResult();
  }

  public ResultModel buildDescriptionResult(String regionName,
      List<RegionDescription> regionDescriptions) {
    if (regionDescriptions.isEmpty()) {
      return ResultModel
          .createError(CliStrings.format(CliStrings.REGION_NOT_FOUND, regionName));
    }

    ResultModel result = new ResultModel();
    int sectionId = 0;
    for (RegionDescription regionDescription : regionDescriptions) {
      sectionId++;

      DataResultModel regionSection = result.addData("region-" + sectionId);
      regionSection.addData("Name", regionDescription.getName());

      String dataPolicy =
          regionDescription.getDataPolicy().toString().toLowerCase().replace('_', ' ');
      regionSection.addData("Data Policy", dataPolicy);

      String memberType;

      if (regionDescription.isAccessor()) {
        memberType = CliStrings.DESCRIBE_REGION__ACCESSOR__MEMBER;
      } else {
        memberType = CliStrings.DESCRIBE_REGION__HOSTING__MEMBER;
      }
      regionSection.addData(memberType,
          StringUtils.join(regionDescription.getHostingMembers(), '\n'));

      TabularResultModel commonNonDefaultAttrTable = result.addTable("non-default-" + sectionId);

      commonNonDefaultAttrTable.setHeader(CliStrings
          .format(CliStrings.DESCRIBE_REGION__NONDEFAULT__COMMONATTRIBUTES__HEADER, memberType));
      // Common Non Default Region Attributes
      Map<String, String> cndRegionAttrsMap = regionDescription.getCndRegionAttributes();

      // Common Non Default Eviction Attributes
      Map<String, String> cndEvictionAttrsMap = regionDescription.getCndEvictionAttributes();

      // Common Non Default Partition Attributes
      Map<String, String> cndPartitionAttrsMap = regionDescription.getCndPartitionAttributes();

      writeCommonAttributesToTable(commonNonDefaultAttrTable,
          CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__REGION, cndRegionAttrsMap);
      writeCommonAttributesToTable(commonNonDefaultAttrTable,
          CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__EVICTION, cndEvictionAttrsMap);
      writeCommonAttributesToTable(commonNonDefaultAttrTable,
          CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__PARTITION, cndPartitionAttrsMap);

      // Member-wise non default Attributes
      Map<String, RegionDescriptionPerMember> regDescPerMemberMap =
          regionDescription.getRegionDescriptionPerMemberMap();
      Set<String> members = regDescPerMemberMap.keySet();

      TabularResultModel table = result.addTable("member-non-default-" + sectionId);

      boolean setHeader = false;
      for (String member : members) {
        RegionDescriptionPerMember regDescPerMem = regDescPerMemberMap.get(member);
        Map<String, String> ndRa = regDescPerMem.getNonDefaultRegionAttributes();
        Map<String, String> ndEa = regDescPerMem.getNonDefaultEvictionAttributes();
        Map<String, String> ndPa = regDescPerMem.getNonDefaultPartitionAttributes();

        // Get all the member-specific non-default attributes by removing the common keys
        ndRa.keySet().removeAll(cndRegionAttrsMap.keySet());
        ndEa.keySet().removeAll(cndEvictionAttrsMap.keySet());
        ndPa.keySet().removeAll(cndPartitionAttrsMap.keySet());

        // Scope is not valid for PR's
        if (regionDescription.isPartition()) {
          if (ndRa.get(RegionAttributesNames.SCOPE) != null) {
            ndRa.remove(RegionAttributesNames.SCOPE);
          }
        }

        List<FixedPartitionAttributesInfo> fpaList = regDescPerMem.getFixedPartitionAttributes();

        if (!ndRa.isEmpty() || !ndEa.isEmpty() || !ndPa.isEmpty()
            || (fpaList != null && !fpaList.isEmpty())) {
          setHeader = true;
          boolean memberNameAdded;
          memberNameAdded = writeAttributesToTable(table,
              CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__REGION, ndRa, member, false);
          memberNameAdded = writeAttributesToTable(table,
              CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__EVICTION, ndEa, member, memberNameAdded);
          memberNameAdded =
              writeAttributesToTable(table, CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__PARTITION,
                  ndPa, member, memberNameAdded);

          writeFixedPartitionAttributesToTable(table, fpaList, member, memberNameAdded);
        }
      }

      if (setHeader) {
        table.setHeader(CliStrings.format(
            CliStrings.DESCRIBE_REGION__NONDEFAULT__PERMEMBERATTRIBUTES__HEADER, memberType));
      }
    }

    return result;
  }

  private void writeCommonAttributesToTable(TabularResultModel table, String attributeType,
      Map<String, String> attributesMap) {
    if (!attributesMap.isEmpty()) {
      Set<String> attributes = attributesMap.keySet();
      boolean isTypeAdded = false;
      final String blank = "";

      for (String attributeName : attributes) {
        String attributeValue = attributesMap.get(attributeName);
        String type;

        if (!isTypeAdded) {
          type = attributeType;
          isTypeAdded = true;
        } else {
          type = blank;
        }
        writeCommonAttributeToTable(table, type, attributeName, attributeValue);
      }
    }
  }

  private void writeFixedPartitionAttributesToTable(TabularResultModel table,
      List<FixedPartitionAttributesInfo> fpaList, String member, boolean isMemberNameAdded) {

    if (fpaList != null) {
      boolean isTypeAdded = false;
      final String blank = "";

      Iterator<FixedPartitionAttributesInfo> fpaIter = fpaList.iterator();
      String type, memName;

      while (fpaIter.hasNext()) {
        FixedPartitionAttributesInfo fpa = fpaIter.next();
        StringBuilder fpaBuilder = new StringBuilder();
        fpaBuilder.append(fpa.getPartitionName());
        fpaBuilder.append(',');

        if (fpa.isPrimary()) {
          fpaBuilder.append("Primary");
        } else {
          fpaBuilder.append("Secondary");
        }
        fpaBuilder.append(',');
        fpaBuilder.append(fpa.getNumBuckets());

        if (!isTypeAdded) {
          type = "";
          isTypeAdded = true;
        } else {
          type = blank;
        }

        if (!isMemberNameAdded) {
          memName = member;
          isMemberNameAdded = true;
        } else {
          memName = blank;
        }

        writeAttributeToTable(table, memName, type, "Fixed Partition", fpaBuilder.toString());
      }
    }

  }

  private boolean writeAttributesToTable(TabularResultModel table, String attributeType,
      Map<String, String> attributesMap, String member, boolean isMemberNameAdded) {
    if (!attributesMap.isEmpty()) {
      Set<String> attributes = attributesMap.keySet();
      boolean isTypeAdded = false;
      final String blank = "";

      for (String attributeName : attributes) {
        String attributeValue = attributesMap.get(attributeName);
        String type, memName;

        if (!isTypeAdded) {
          type = attributeType;
          isTypeAdded = true;
        } else {
          type = blank;
        }

        if (!isMemberNameAdded) {
          memName = member;
          isMemberNameAdded = true;
        } else {
          memName = blank;
        }

        writeAttributeToTable(table, memName, type, attributeName, attributeValue);
      }
    }

    return isMemberNameAdded;
  }

  private void writeAttributeToTable(TabularResultModel table, String member, String attributeType,
      String attributeName, String attributeValue) {

    final String blank = "";
    if (attributeValue != null) {
      // Tokenize the attributeValue
      String[] attributeValues = attributeValue.split(",");
      boolean isFirstValue = true;

      for (String value : attributeValues) {
        if (isFirstValue) {
          table.accumulate(CliStrings.DESCRIBE_REGION__MEMBER, member);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE, attributeType);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__NAME, attributeName);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__VALUE, value);
          isFirstValue = false;
        } else {
          table.accumulate(CliStrings.DESCRIBE_REGION__MEMBER, blank);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE, blank);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__NAME, blank);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__VALUE, value);
        }
      }
    }
  }

  private void writeCommonAttributeToTable(TabularResultModel table, String attributeType,
      String attributeName, String attributeValue) {
    final String blank = "";
    if (attributeValue != null) {
      String[] attributeValues = attributeValue.split(",");
      boolean isFirstValue = true;
      for (String value : attributeValues) {
        if (isFirstValue) {
          isFirstValue = false;
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE, attributeType);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__NAME, attributeName);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__VALUE, value);
        } else {
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE, blank);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__NAME, blank);
          table.accumulate(CliStrings.DESCRIBE_REGION__ATTRIBUTE__VALUE, value);
        }
      }
    }
  }
}
