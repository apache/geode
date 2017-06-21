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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.domain.FixedPartitionAttributesInfo;
import org.apache.geode.management.internal.cli.domain.RegionDescription;
import org.apache.geode.management.internal.cli.domain.RegionDescriptionPerMember;
import org.apache.geode.management.internal.cli.domain.RegionInformation;
import org.apache.geode.management.internal.cli.functions.GetRegionDescriptionFunction;
import org.apache.geode.management.internal.cli.functions.GetRegionsFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.CompositeResultData.SectionResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.util.RegionAttributesNames;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * Class containing implementation of commands based on region:
 * <ul>
 * <li>list region
 * <li>describe region
 * </ul>
 * 
 * @since GemFire 7.0
 */
public class RegionCommands implements GfshCommand {
  private static final GetRegionsFunction getRegionsFunction = new GetRegionsFunction();
  private static final GetRegionDescriptionFunction getRegionDescription =
      new GetRegionDescriptionFunction();

  @CliCommand(value = {CliStrings.LIST_REGION}, help = CliStrings.LIST_REGION__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result listRegion(
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.LIST_REGION__GROUP__HELP) String[] group,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.LIST_REGION__MEMBER__HELP) String[] memberNameOrId) {
    Result result = null;
    try {
      Set<RegionInformation> regionInfoSet = new LinkedHashSet<>();
      ResultCollector<?, ?> rc;

      Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrId);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      TabularResultData resultData = ResultBuilder.createTabularResultData();
      rc = CliUtil.executeFunction(getRegionsFunction, null, targetMembers);

      ArrayList<?> resultList = (ArrayList<?>) rc.getResult();

      if (resultList != null) {

        for (Object resultObj : resultList) {
          if (resultObj != null) {
            if (resultObj instanceof Object[]) {
              Object[] resultObjectArray = (Object[]) resultObj;
              for (Object regionInfo : resultObjectArray) {
                if (regionInfo instanceof RegionInformation) {
                  regionInfoSet.add((RegionInformation) regionInfo);
                }
              }
            }
          }
        }

        Set<String> regionNames = new TreeSet<>();

        for (RegionInformation regionInfo : regionInfoSet) {
          regionNames.add(regionInfo.getName());
          Set<String> subRegionNames = regionInfo.getSubRegionNames();

          regionNames.addAll(subRegionNames);
        }

        for (String regionName : regionNames) {
          resultData.accumulate("List of regions", regionName);
        }

        if (!regionNames.isEmpty()) {
          result = ResultBuilder.buildResult(resultData);

        } else {
          result = ResultBuilder.createInfoResult(CliStrings.LIST_REGION__MSG__NOT_FOUND);
        }
      }
    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createGemFireErrorResult(CliStrings
          .format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, CliStrings.LIST_REGION));
    } catch (Exception e) {
      result = ResultBuilder
          .createGemFireErrorResult(CliStrings.LIST_REGION__MSG__ERROR + " : " + e.getMessage());
    }
    return result;
  }

  @CliCommand(value = {CliStrings.DESCRIBE_REGION}, help = CliStrings.DESCRIBE_REGION__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result describeRegion(
      @CliOption(key = CliStrings.DESCRIBE_REGION__NAME, optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.DESCRIBE_REGION__NAME__HELP, mandatory = true) String regionName) {

    Result result;
    try {

      if (regionName == null || regionName.isEmpty()) {
        return ResultBuilder.createUserErrorResult("Please provide a region name");
      }

      if (regionName.equals(Region.SEPARATOR)) {
        return ResultBuilder.createUserErrorResult(CliStrings.INVALID_REGION_NAME);
      }

      InternalCache cache = getCache();
      ResultCollector<?, ?> rc =
          CliUtil.executeFunction(getRegionDescription, regionName, CliUtil.getAllMembers(cache));

      List<?> resultList = (List<?>) rc.getResult();

      // The returned result could be a region description with per member and /or single local
      // region
      Object[] results = resultList.toArray();
      List<RegionDescription> regionDescriptionList = new ArrayList<>();

      for (int i = 0; i < results.length; i++) {

        if (results[i] instanceof RegionDescriptionPerMember) {
          RegionDescriptionPerMember regionDescPerMember = (RegionDescriptionPerMember) results[i];

          if (regionDescPerMember != null) {
            RegionDescription regionDescription = new RegionDescription();
            regionDescription.add(regionDescPerMember);

            for (int j = i + 1; j < results.length; j++) {
              if (results[j] != null && results[j] instanceof RegionDescriptionPerMember) {
                RegionDescriptionPerMember preyRegionDescPerMember =
                    (RegionDescriptionPerMember) results[j];
                if (regionDescription.add(preyRegionDescPerMember)) {
                  results[j] = null;
                }
              }
            }
            regionDescriptionList.add(regionDescription);
          }
        } else if (results[i] instanceof Throwable) {
          Throwable t = (Throwable) results[i];
          LogWrapper.getInstance().info(t.getMessage(), t);
        }
      }

      if (regionDescriptionList.isEmpty()) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.format(CliStrings.REGION_NOT_FOUND, regionName));
      }

      CompositeResultData crd = ResultBuilder.createCompositeResultData();

      for (RegionDescription regionDescription : regionDescriptionList) {
        // No point in displaying the scope for PR's
        if (regionDescription.isPartition()) {
          regionDescription.getCndRegionAttributes().remove(RegionAttributesNames.SCOPE);
        } else {
          String scope =
              regionDescription.getCndRegionAttributes().get(RegionAttributesNames.SCOPE);
          if (scope != null) {
            scope = scope.toLowerCase().replace('_', '-');
            regionDescription.getCndRegionAttributes().put(RegionAttributesNames.SCOPE, scope);
          }
        }
        SectionResultData regionSection = crd.addSection();
        regionSection.addSeparator('-');
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
            CliUtil.convertStringSetToString(regionDescription.getHostingMembers(), '\n'));
        regionSection.addSeparator('.');

        TabularResultData commonNonDefaultAttrTable = regionSection.addSection().addTable();

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

        TabularResultData table = regionSection.addSection().addTable();

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

          if (!(ndRa.isEmpty() && ndEa.isEmpty() && ndPa.isEmpty()) || fpaList != null) {
            setHeader = true;
            boolean memberNameAdded;
            memberNameAdded = writeAttributesToTable(table,
                CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__REGION, ndRa, member, false);
            memberNameAdded =
                writeAttributesToTable(table, CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__EVICTION,
                    ndEa, member, memberNameAdded);
            memberNameAdded = writeAttributesToTable(table,
                CliStrings.DESCRIBE_REGION__ATTRIBUTE__TYPE__PARTITION, ndPa, member,
                memberNameAdded);

            writeFixedPartitionAttributesToTable(table, "", fpaList, member, memberNameAdded);
          }
        }

        if (setHeader) {
          table.setHeader(CliStrings.format(
              CliStrings.DESCRIBE_REGION__NONDEFAULT__PERMEMBERATTRIBUTES__HEADER, memberType));
        }
      }

      result = ResultBuilder.buildResult(crd);
    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createGemFireErrorResult(CliStrings
          .format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, CliStrings.DESCRIBE_REGION));
    } catch (Exception e) {
      String errorMessage = CliStrings.format(CliStrings.EXCEPTION_CLASS_AND_MESSAGE,
          e.getClass().getName(), e.getMessage());
      result = ResultBuilder.createGemFireErrorResult(errorMessage);
    }
    return result;
  }

  private void writeCommonAttributesToTable(TabularResultData table, String attributeType,
      Map<String, String> attributesMap) {
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
        writeCommonAttributeToTable(table, type, attributeName, attributeValue);
      }
    }
  }

  private boolean writeFixedPartitionAttributesToTable(TabularResultData table,
      String attributeType, List<FixedPartitionAttributesInfo> fpaList, String member,
      boolean isMemberNameAdded) {

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

        writeAttributeToTable(table, memName, type, "Fixed Partition", fpaBuilder.toString());
      }
    }

    return isMemberNameAdded;
  }

  private boolean writeAttributesToTable(TabularResultData table, String attributeType,
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

  public void writeAttributeToTable(TabularResultData table, String member, String attributeType,
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


  private void writeCommonAttributeToTable(TabularResultData table, String attributeType,
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

  public void addChildSection(SectionResultData parentSection, Map<String, String> map,
      String header) {
    if (!map.isEmpty()) {
      Set<String> attributes = map.keySet();
      SectionResultData section = parentSection.addSection();
      section.setHeader(header);
      for (String attribute : attributes) {
        section.addData(attribute, map.get(attribute));
      }
    }
  }
}
