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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.functions.DataCommandFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;

public class DataCommandsUtils {
  static boolean checkResultList(CompositeResultData rebalanceResultData, List resultList,
      DistributedMember member) {
    boolean toContinueForOtherMembers = false;
    if (CollectionUtils.isNotEmpty(resultList)) {
      for (Object object : resultList) {
        if (object instanceof Exception) {
          rebalanceResultData.addSection().addData(
              CliStrings.format(CliStrings.REBALANCE__MSG__NO_EXECUTION, member.getId()),
              ((Exception) object).getMessage());

          LogWrapper.getInstance().info(CliStrings.REBALANCE__MSG__NO_EXECUTION + member.getId()
              + " exception=" + ((Throwable) object).getMessage(), ((Throwable) object));

          toContinueForOtherMembers = true;
          break;
        } else if (object instanceof Throwable) {
          rebalanceResultData.addSection().addData(
              CliStrings.format(CliStrings.REBALANCE__MSG__NO_EXECUTION, member.getId()),
              ((Throwable) object).getMessage());

          LogWrapper.getInstance().info(CliStrings.REBALANCE__MSG__NO_EXECUTION + member.getId()
              + " exception=" + ((Throwable) object).getMessage(), ((Throwable) object));

          toContinueForOtherMembers = true;
          break;
        }
      }
    } else {
      LogWrapper.getInstance().info(
          "Rebalancing for member=" + member.getId() + ", resultList is either null or empty");
      rebalanceResultData.addSection().addData("Rebalancing for member=" + member.getId(),
          ", resultList is either null or empty");
      toContinueForOtherMembers = true;
    }
    return toContinueForOtherMembers;
  }

  static List<String> tokenize(String str, String separator) {
    StringTokenizer st = new StringTokenizer(str, separator);
    List<String> rstList = new ArrayList<>();
    while (st.hasMoreTokens()) {
      rstList.add(st.nextToken());

    }
    return rstList;
  }

  static CompositeResultData toCompositeResultData(CompositeResultData rebalanceResultData,
      ArrayList<String> rstlist, int index, boolean simulate, InternalCache cache) {
    int resultItemCount = 9;
    // add only if there are any valid regions in results
    if (rstlist.size() > resultItemCount && StringUtils.isNotEmpty(rstlist.get(resultItemCount))) {
      TabularResultData table1 = rebalanceResultData.addSection().addTable("Table" + index);
      String newLine = System.getProperty("line.separator");
      StringBuilder resultStr = new StringBuilder();
      resultStr.append(newLine);
      table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES);
      table1.accumulate("Value", rstlist.get(0));
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES).append(" = ")
          .append(rstlist.get(0)).append(newLine);
      table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM);
      table1.accumulate("Value", rstlist.get(1));
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM).append(" = ")
          .append(rstlist.get(1)).append(newLine);

      table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED);
      table1.accumulate("Value", rstlist.get(2));
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED).append(" = ")
          .append(rstlist.get(2)).append(newLine);

      table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES);
      table1.accumulate("Value", rstlist.get(3));
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES).append(" = ")
          .append(rstlist.get(3)).append(newLine);

      table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME);
      table1.accumulate("Value", rstlist.get(4));
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME).append(" = ")
          .append(rstlist.get(4)).append(newLine);

      table1.accumulate("Rebalanced Stats",
          CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED);
      table1.accumulate("Value", rstlist.get(5));
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED).append(" = ")
          .append(rstlist.get(5)).append(newLine);

      table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME);
      table1.accumulate("Value", rstlist.get(6));
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME).append(" = ")
          .append(rstlist.get(6)).append(newLine);

      table1.accumulate("Rebalanced Stats",
          CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED);
      table1.accumulate("Value", rstlist.get(7));
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED).append(" = ")
          .append(rstlist.get(7)).append(newLine);

      table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALTIME);
      table1.accumulate("Value", rstlist.get(8));
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALTIME).append(" = ").append(rstlist.get(8))
          .append(newLine);

      String headerText;
      if (simulate) {
        headerText = "Simulated partition regions ";
      } else {
        headerText = "Rebalanced partition regions ";
      }
      for (int i = resultItemCount; i < rstlist.size(); i++) {
        headerText = headerText + " " + rstlist.get(i);
      }
      table1.setHeader(headerText);
      cache.getLogger().info(headerText + resultStr);
    }
    return rebalanceResultData;
  }

  static Result makePresentationResult(DataCommandResult dataResult) {
    if (dataResult != null) {
      return dataResult.toCommandResult();
    } else {
      return ResultBuilder.createGemFireErrorResult("Error executing data command");
    }
  }

  public static Set<DistributedMember> getRegionAssociatedMembers(String region,
      final InternalCache cache, boolean returnAll) {
    DistributedMember member;
    if (StringUtils.isEmpty(region)) {
      return null;
    }
    DistributedRegionMXBean bean =
        ManagementService.getManagementService(cache).getDistributedRegionMXBean(region);
    if (bean == null) {
      // try with slash ahead
      bean = ManagementService.getManagementService(cache)
          .getDistributedRegionMXBean(Region.SEPARATOR + region);
    }
    if (bean == null) {
      return null;
    }
    String[] membersName = bean.getMembers();
    Set<DistributedMember> dsMembers = cache.getMembers();
    Set<DistributedMember> dsMembersWithThisMember = new HashSet<>();
    dsMembersWithThisMember.addAll(dsMembers);
    dsMembersWithThisMember.add(cache.getDistributedSystem().getDistributedMember());
    Iterator it = dsMembersWithThisMember.iterator();
    Set<DistributedMember> matchedMembers = new HashSet<>();

    if (membersName.length > 0) {
      while (it.hasNext()) {
        DistributedMember dsmember = (DistributedMember) it.next();
        for (String memberName : membersName) {
          String name = MBeanJMXAdapter.getMemberNameOrId(dsmember);
          if (name.equals(memberName)) {
            member = dsmember;
            matchedMembers.add(member);
            if (!returnAll) {
              return matchedMembers;
            }
          }
        }
      }
    }
    // try with function calls
    if (matchedMembers.size() == 0) {
      matchedMembers = CliUtil.getMembersForeRegionViaFunction(cache, region, true);
    }
    return matchedMembers;
  }

  static DataCommandResult callFunctionForRegion(DataCommandRequest request,
      DataCommandFunction putfn, Set<DistributedMember> members) {
    if (members.size() == 1) {
      DistributedMember member = members.iterator().next();
      ResultCollector collector =
          FunctionService.onMember(member).setArguments(request).execute(putfn);
      List list = (List) collector.getResult();
      Object object = list.get(0);
      if (object instanceof Throwable) {
        Throwable error = (Throwable) object;
        DataCommandResult result = new DataCommandResult();
        result.setErorr(error);
        result.setErrorString(error.getMessage());
        return result;
      }
      DataCommandResult result = (DataCommandResult) list.get(0);
      result.aggregate(null);
      return result;
    } else {
      ResultCollector collector =
          FunctionService.onMembers(members).setArguments(request).execute(putfn);
      List list = (List) collector.getResult();
      DataCommandResult result = null;
      for (Object object : list) {
        if (object instanceof Throwable) {
          Throwable error = (Throwable) object;
          result = new DataCommandResult();
          result.setErorr(error);
          result.setErrorString(error.getMessage());
          return result;
        }

        if (result == null) {
          result = (DataCommandResult) object;
          result.aggregate(null);
        } else {
          result.aggregate((DataCommandResult) object);
        }
      }
      return result;
    }
  }

  public static Set<DistributedMember> getQueryRegionsAssociatedMembers(Set<String> regions,
      final InternalCache cache, boolean returnAll) {
    LogWriter logger = cache.getLogger();
    Set<DistributedMember> members;
    Set<DistributedMember> newMembers = null;
    Iterator<String> iterator = regions.iterator();
    String region = iterator.next();
    members = getRegionAssociatedMembers(region, cache, true);
    if (logger.fineEnabled()) {
      logger.fine("Members for region " + region + " Members " + members);
    }
    List<String> regionAndingList = new ArrayList<>();
    regionAndingList.add(region);
    if (regions.size() == 1) {
      newMembers = members;
    } else {
      if (CollectionUtils.isNotEmpty(members)) {
        while (iterator.hasNext()) {
          region = iterator.next();
          newMembers = getRegionAssociatedMembers(region, cache, true);
          if (newMembers == null) {
            newMembers = new HashSet<>();
          }
          if (logger.fineEnabled()) {
            logger.fine("Members for region " + region + " Members " + newMembers);
          }
          regionAndingList.add(region);
          newMembers.retainAll(members);
          members = newMembers;
          if (logger.fineEnabled()) {
            logger.fine(
                "Members after anding for regions " + regionAndingList + " List : " + newMembers);
          }
        }
      }
    }
    members = new HashSet<>();
    if (newMembers == null) {
      return members;
    }
    for (DistributedMember newMember : newMembers) {
      members.add(newMember);
      if (!returnAll) {
        return members;
      }
    }
    return members;
  }
}
