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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.functions.RebalanceFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class RebalanceCommand extends GfshCommand {
  @CliCommand(value = CliStrings.REBALANCE, help = CliStrings.REBALANCE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel rebalance(
      @CliOption(key = CliStrings.REBALANCE__INCLUDEREGION,
          help = CliStrings.REBALANCE__INCLUDEREGION__HELP) String[] includeRegions,
      @CliOption(key = CliStrings.REBALANCE__EXCLUDEREGION,
          help = CliStrings.REBALANCE__EXCLUDEREGION__HELP) String[] excludeRegions,
      @CliOption(key = CliStrings.REBALANCE__TIMEOUT, unspecifiedDefaultValue = "-1",
          help = CliStrings.REBALANCE__TIMEOUT__HELP) long timeout,
      @CliOption(key = CliStrings.REBALANCE__SIMULATE, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.REBALANCE__SIMULATE__HELP) boolean simulate)
      throws Exception {

    ExecutorService commandExecutors =
        LoggingExecutors.newSingleThreadExecutor("RebalanceCommand", false);
    List<Future<ResultModel>> commandResult = new ArrayList<>();
    ResultModel result;
    try {
      commandResult.add(
          commandExecutors.submit(rebalanceCallable(includeRegions, excludeRegions, simulate)));

      Future<ResultModel> fs = commandResult.get(0);
      if (timeout > 0) {
        result = fs.get(timeout, TimeUnit.SECONDS);
      } else {
        result = fs.get();
      }
    } catch (TimeoutException timeoutException) {
      result = ResultModel.createInfo(CliStrings.REBALANCE__MSG__REBALANCE_WILL_CONTINUE);
    }

    return result;
  }

  public Callable<ResultModel> rebalanceCallable(String[] includeRegions, String[] excludeRegions,
      boolean simulate) {
    return new ExecuteRebalanceWithTimeout(includeRegions, excludeRegions, simulate,
        (InternalCache) getCache());
  }

  private boolean checkResultList(InfoResultModel errors, List resultList,
      DistributedMember member) {
    boolean toContinueForOtherMembers = false;
    if (CollectionUtils.isNotEmpty(resultList)) {
      for (Object object : resultList) {
        if (object instanceof Exception) {
          errors.addLine(
              CliStrings.format(CliStrings.REBALANCE__MSG__NO_EXECUTION, member.getId()) + ": " +
                  ((Exception) object).getMessage());

          LogWrapper.getInstance(getCache()).info(CliStrings.REBALANCE__MSG__NO_EXECUTION
              + member.getId() + " exception=" + ((Throwable) object).getMessage(),
              ((Throwable) object));

          toContinueForOtherMembers = true;
          break;
        } else if (object instanceof Throwable) {
          errors.addLine(
              CliStrings.format(CliStrings.REBALANCE__MSG__NO_EXECUTION, member.getId()) + ": " +
                  ((Throwable) object).getMessage());

          LogWrapper.getInstance(getCache()).info(CliStrings.REBALANCE__MSG__NO_EXECUTION
              + member.getId() + " exception=" + ((Throwable) object).getMessage(),
              ((Throwable) object));

          toContinueForOtherMembers = true;
          break;
        }
      }
    } else {
      LogWrapper.getInstance(getCache()).info(
          "Rebalancing for member=" + member.getId() + ", resultList is either null or empty");
      errors.addLine("Rebalancing for member=" + member.getId() +
          ", resultList is either null or empty");
      toContinueForOtherMembers = true;
    }
    return toContinueForOtherMembers;
  }

  private void toCompositeResultData(ResultModel result,
      List<String> rstlist, int index, boolean simulate, InternalCache cache) {
    final int resultItemCount = 9;

    if (rstlist.size() <= resultItemCount || StringUtils.isEmpty(rstlist.get(resultItemCount))) {
      return;
    }

    TabularResultModel table1 = result.addTable("Table" + index);
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
      headerText = "Simulated partition regions";
    } else {
      headerText = "Rebalanced partition regions";
    }
    for (int i = resultItemCount; i < rstlist.size(); i++) {
      headerText = headerText + " " + rstlist.get(i);
    }
    table1.setHeader(headerText);
    cache.getLogger().info(headerText + resultStr);
  }


  // TODO EY Move this to its own class
  private class ExecuteRebalanceWithTimeout implements Callable<ResultModel> {

    String[] includeRegions = null;
    String[] excludeRegions = null;
    boolean simulate;
    InternalCache cache = null;

    @Override
    public ResultModel call() throws Exception {
      ResultModel result = executeRebalanceWithTimeout(includeRegions, excludeRegions, simulate);

      // if the result contains only error section, i.e. no rebalance operation is done, mark this
      // command result to be error. This would happy if user hasn't specified any valid region. If
      // only one region specified is valid and rebalance is done, the result would be marked as
      // success.
      if (result.getSectionSize() == 1 && result.getInfoSection("error") != null) {
        result.setStatus(Result.Status.ERROR);
      }

      return result;
    }

    ExecuteRebalanceWithTimeout(String[] includedRegions, String[] excludedRegions,
        boolean toSimulate, InternalCache cache) {
      includeRegions = includedRegions;
      excludeRegions = excludedRegions;
      simulate = toSimulate;
      this.cache = cache;
    }

    private ResultModel executeRebalanceWithTimeout(String[] includeRegions,
        String[] excludeRegions,
        boolean simulate) {

      ResultModel result = new ResultModel();
      try {
        RebalanceOperation op;

        if (ArrayUtils.isNotEmpty(includeRegions)) {
          InfoResultModel errors = result.addInfo("error");
          int index = 0;

          for (String regionName : includeRegions) {

            // To be removed after region Name specification with "/" is fixed
            regionName = regionName.startsWith("/") ? regionName : ("/" + regionName);
            Region region = cache.getRegion(regionName);

            if (region == null) {
              DistributedMember member = getAssociatedMembers(regionName, cache);

              if (member == null) {
                errors.addLine(CliStrings.format(
                    CliStrings.REBALANCE__MSG__NO_ASSOCIATED_DISTRIBUTED_MEMBER, regionName));
                continue;
              }

              Function rebalanceFunction = new RebalanceFunction();
              Object[] functionArgs = new Object[3];
              functionArgs[0] = simulate ? "true" : "false";
              Set<String> setRegionName = new HashSet<>();
              setRegionName.add(regionName);
              functionArgs[1] = setRegionName;

              Set<String> excludeRegionSet = new HashSet<>();
              if (ArrayUtils.isNotEmpty(excludeRegions)) {
                Collections.addAll(excludeRegionSet, excludeRegions);
              }
              functionArgs[2] = excludeRegionSet;

              List resultList;
              try {
                resultList = (ArrayList) executeFunction(rebalanceFunction, functionArgs, member)
                    .getResult();
              } catch (Exception ex) {
                LogWrapper.getInstance(cache)
                    .info(CliStrings.format(
                        CliStrings.REBALANCE__MSG__EXCEPTION_IN_REBALANCE_FOR_MEMBER_0_Exception_1,
                        member.getId(), ex.getMessage()), ex);
                errors.addLine(CliStrings.format(
                    CliStrings.REBALANCE__MSG__EXCEPTION_IN_REBALANCE_FOR_MEMBER_0_Exception,
                    member.getId()) + ": " + ex.getMessage());
                continue;
              }

              if (checkResultList(errors, resultList, member)) {
                continue;
              }
              List<String> rstList = Arrays.asList(((String) resultList.get(0)).split(","));

              toCompositeResultData(result, rstList, index, simulate, cache);
            } else {

              ResourceManager manager = cache.getResourceManager();
              RebalanceFactory rbFactory = manager.createRebalanceFactory();
              Set<String> excludeRegionSet = new HashSet<>();
              if (excludeRegions != null) {
                Collections.addAll(excludeRegionSet, excludeRegions);
              }
              rbFactory.excludeRegions(excludeRegionSet);
              Set<String> includeRegionSet = new HashSet<>();
              includeRegionSet.add(regionName);
              rbFactory.includeRegions(includeRegionSet);

              if (simulate) {
                op = manager.createRebalanceFactory().simulate();
              } else {
                op = manager.createRebalanceFactory().start();
              }
              // Wait until the rebalance is complete and then get the results
              buildResultForRebalance(result, op.getResults(), index, simulate, cache);
            }
            index++;
          }
          LogWrapper.getInstance(cache).info("Rebalance returning result " + result);
          return result;
        } else {
          LogWrapper.getInstance(cache)
              .info("Starting Rebalance simulate=" + simulate + " result >> " + result);
          result = executeRebalanceOnDS(cache, String.valueOf(simulate), excludeRegions);
        }
      } catch (Exception e) {
        result = ResultModel.createError(e.getMessage());
      }
      LogWrapper.getInstance(cache).info("Rebalance returning result >>>" + result);
      return result;
    }
  }

  private DistributedMember getAssociatedMembers(String region, final InternalCache cache) {
    DistributedRegionMXBean bean =
        ManagementService.getManagementService(cache).getDistributedRegionMXBean(region);

    DistributedMember member = null;

    if (bean == null) {
      return null;
    }

    String[] membersName = bean.getMembers();
    Set<DistributedMember> dsMembers = getAllMembers();
    Iterator it = dsMembers.iterator();

    boolean matchFound = false;

    if (membersName.length > 1) {
      while (it.hasNext() && !matchFound) {
        DistributedMember dsmember = (DistributedMember) it.next();
        for (String memberName : membersName) {
          if (MBeanJMXAdapter.getMemberNameOrUniqueId(dsmember).equals(memberName)) {
            member = dsmember;
            matchFound = true;
            break;
          }
        }
      }
    }
    return member;
  }

  private void buildResultForRebalance(ResultModel resultModel,
      RebalanceResults results, int index, boolean simulate, InternalCache cache) {
    Set<PartitionRebalanceInfo> regions = results.getPartitionRebalanceDetails();
    Iterator iterator = regions.iterator();

    // add only if there are valid number of regions
    if (regions.size() > 0
        && StringUtils.isNotEmpty(((PartitionRebalanceInfo) iterator.next()).getRegionPath())) {
      final TabularResultModel resultData = resultModel.addTable("Table" + index);
      String newLine = System.getProperty("line.separator");
      StringBuilder resultStr = new StringBuilder();
      resultStr.append(newLine);

      resultData.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES);
      resultData.accumulate("Value", results.getTotalBucketCreateBytes() + "");
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES).append(" = ")
          .append(results.getTotalBucketCreateBytes()).append(newLine);

      resultData.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM);
      resultData.accumulate("Value", results.getTotalBucketCreateTime() + "");
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM).append(" = ")
          .append(results.getTotalBucketCreateTime()).append(newLine);

      resultData.accumulate("Rebalanced Stats",
          CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED);
      resultData.accumulate("Value", results.getTotalBucketCreatesCompleted() + "");
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED).append(" = ")
          .append(results.getTotalBucketCreatesCompleted()).append(newLine);

      resultData.accumulate("Rebalanced Stats",
          CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES);
      resultData.accumulate("Value", results.getTotalBucketTransferBytes() + "");
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES).append(" = ")
          .append(results.getTotalBucketTransferBytes()).append(newLine);

      resultData.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME);
      resultData.accumulate("Value", results.getTotalBucketTransferTime() + "");
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME).append(" = ")
          .append(results.getTotalBucketTransferTime()).append(newLine);

      resultData.accumulate("Rebalanced Stats",
          CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED);
      resultData.accumulate("Value", results.getTotalBucketTransfersCompleted() + "");
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED).append(" = ")
          .append(results.getTotalBucketTransfersCompleted()).append(newLine);

      resultData.accumulate("Rebalanced Stats",
          CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME);
      resultData.accumulate("Value", results.getTotalPrimaryTransferTime() + "");
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME).append(" = ")
          .append(results.getTotalPrimaryTransferTime()).append(newLine);

      resultData.accumulate("Rebalanced Stats",
          CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED);
      resultData.accumulate("Value", results.getTotalPrimaryTransfersCompleted() + "");
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED).append(" = ")
          .append(results.getTotalPrimaryTransfersCompleted()).append(newLine);

      resultData.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALTIME);
      resultData.accumulate("Value", results.getTotalTime() + "");
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALTIME).append(" = ")
          .append(results.getTotalTime()).append(newLine);

      Iterator<PartitionRebalanceInfo> it = regions.iterator();

      String headerText;

      if (simulate) {
        headerText = "Simulated partition regions ";
      } else {
        headerText = "Rebalanced partition regions ";
      }

      while (it.hasNext()) {
        PartitionRebalanceInfo rgn = it.next();
        headerText = headerText + " " + rgn.getRegionPath();
      }
      resultData.setHeader(resultData.getHeader() + headerText);

      cache.getLogger().info(headerText + resultStr);
    }
  }

  private ResultModel executeRebalanceOnDS(InternalCache cache, String simulate,
      String[] excludeRegionsList) {
    ResultModel resultModel = new ResultModel();
    int index = 1;
    InfoResultModel errors = resultModel.addInfo("errors");
    List<String> listExcludedRegion = new ArrayList<>();
    if (excludeRegionsList != null) {
      Collections.addAll(listExcludedRegion, excludeRegionsList);
    }
    List<MemberPRInfo> listMemberRegion = getMemberRegionList(cache, listExcludedRegion);

    if (listMemberRegion.size() == 0) {
      return ResultModel.createInfo(CliStrings.REBALANCE__MSG__NO_REBALANCING_REGIONS_ON_DS);
    }

    Iterator<MemberPRInfo> iterator = listMemberRegion.iterator();
    boolean flagToContinueWithRebalance = false;

    // check if list has some members that can be rebalanced
    while (iterator.hasNext()) {
      if (iterator.next().dsMemberList.size() > 1) {
        flagToContinueWithRebalance = true;
        break;
      }
    }

    if (!flagToContinueWithRebalance) {
      return ResultModel.createInfo(CliStrings.REBALANCE__MSG__NO_REBALANCING_REGIONS_ON_DS);
    }

    for (MemberPRInfo memberPR : listMemberRegion) {
      try {
        // check if there are more than one members associated with region for rebalancing
        if (memberPR.dsMemberList.size() > 1) {
          for (int i = 0; i < memberPR.dsMemberList.size(); i++) {
            DistributedMember dsMember = memberPR.dsMemberList.get(i);
            Function rebalanceFunction = new RebalanceFunction();
            Object[] functionArgs = new Object[3];
            functionArgs[0] = simulate;
            Set<String> regionSet = new HashSet<>();

            regionSet.add(memberPR.region);
            functionArgs[1] = regionSet;

            Set<String> excludeRegionSet = new HashSet<>();
            functionArgs[2] = excludeRegionSet;

            List resultList = null;

            try {
              if (checkMemberPresence(dsMember)) {
                resultList = (ArrayList) executeFunction(rebalanceFunction, functionArgs, dsMember)
                    .getResult();

                if (checkResultList(errors, resultList, dsMember)) {
                  continue;
                }

                List<String> rstList = Arrays.asList(((String) resultList.get(0)).split(","));
                toCompositeResultData(resultModel, rstList, index, simulate.equals("true"), cache);
                index++;

                // Rebalancing for region is done so break and continue with other region
                break;
              } else {
                if (i == memberPR.dsMemberList.size() - 1) {
                  errors.addLine(
                      CliStrings.format(
                          CliStrings.REBALANCE__MSG__NO_EXECUTION_FOR_REGION_0_ON_MEMBERS_1,
                          memberPR.region, listOfAllMembers(memberPR.dsMemberList)) + ", " +
                          CliStrings.REBALANCE__MSG__MEMBERS_MIGHT_BE_DEPARTED);
                } else {
                  continue;
                }
              }
            } catch (Exception ex) {
              if (i == memberPR.dsMemberList.size() - 1) {
                errors.addLine(
                    CliStrings.format(
                        CliStrings.REBALANCE__MSG__NO_EXECUTION_FOR_REGION_0_ON_MEMBERS_1,
                        memberPR.region, listOfAllMembers(memberPR.dsMemberList)) + ", " +
                        CliStrings.REBALANCE__MSG__REASON + ex.getMessage());
              } else {
                continue;
              }
            }

            if (checkResultList(errors, resultList, dsMember)) {
              continue;
            }

            List<String> rstList = Arrays.asList(((String) resultList.get(0)).split(","));
            toCompositeResultData(resultModel, rstList, index, simulate.equals("true"), cache);
            index++;
          }
        }
      } catch (Exception e) {
        return ResultModel.createError(e.getMessage());
      }
    }
    return resultModel;
  }

  private static class MemberPRInfo {
    ArrayList<DistributedMember> dsMemberList;
    public String region;

    MemberPRInfo() {
      region = "";
      dsMemberList = new ArrayList<>();
    }

    @Override
    public boolean equals(Object o2) {
      return o2 != null && this.region.equals(((MemberPRInfo) o2).region);
    }

    @Override
    public int hashCode() {
      return this.region.hashCode();
    }
  }

  private List<MemberPRInfo> getMemberRegionList(InternalCache cache,
      List<String> listExcludedRegion) {
    List<MemberPRInfo> listMemberPRInfo = new ArrayList<>();
    String[] listDSRegions =
        ManagementService.getManagementService(cache).getDistributedSystemMXBean().listRegions();
    final Set<DistributedMember> dsMembers = getAllMembers();

    for (String regionName : listDSRegions) {
      // check for excluded regions
      boolean excludedRegionMatch = false;
      for (String aListExcludedRegion : listExcludedRegion) {
        // this is needed since region name may start with / or without it
        // also
        String excludedRegion = aListExcludedRegion.trim();
        if (regionName.startsWith("/")) {
          if (!excludedRegion.startsWith("/")) {
            excludedRegion = "/" + excludedRegion;
          }
        }
        if (excludedRegion.startsWith("/")) {
          if (!regionName.startsWith("/")) {
            regionName = "/" + regionName;
          }
        }

        if (excludedRegion.equals(regionName)) {
          excludedRegionMatch = true;
          break;
        }
      }

      if (excludedRegionMatch) {
        // ignore this region
        continue;
      }

      if (!regionName.startsWith("/")) {
        regionName = Region.SEPARATOR + regionName;
      }
      // remove this prefix /
      DistributedRegionMXBean bean =
          ManagementService.getManagementService(cache).getDistributedRegionMXBean(regionName);

      if (bean != null) {
        if (bean.getRegionType().equals(DataPolicy.PARTITION.toString())
            || bean.getRegionType().equals(DataPolicy.PERSISTENT_PARTITION.toString())) {

          String[] memberNames = bean.getMembers();
          for (DistributedMember dsmember : dsMembers) {
            for (String memberName : memberNames) {
              if (MBeanJMXAdapter.getMemberNameOrUniqueId(dsmember).equals(memberName)) {
                MemberPRInfo memberAndItsPRRegions = new MemberPRInfo();
                memberAndItsPRRegions.region = regionName;
                memberAndItsPRRegions.dsMemberList.add(dsmember);
                if (listMemberPRInfo.contains(memberAndItsPRRegions)) {
                  // add member for appropriate region
                  int index = listMemberPRInfo.indexOf(memberAndItsPRRegions);
                  MemberPRInfo listMember = listMemberPRInfo.get(index);
                  listMember.dsMemberList.add(dsmember);
                } else {
                  listMemberPRInfo.add(memberAndItsPRRegions);
                }
                break;
              }
            }
          }
        }
      }
    }

    return listMemberPRInfo;
  }

  private boolean checkMemberPresence(DistributedMember dsMember) {
    // check if member's presence just before executing function
    // this is to avoid running a function on departed members #47248
    Set<DistributedMember> dsMemberList = getAllNormalMembers();
    return dsMemberList.contains(dsMember);
  }

  private String listOfAllMembers(ArrayList<DistributedMember> dsMemberList) {
    StringBuilder listMembersId = new StringBuilder();
    for (int j = 0; j < dsMemberList.size() - 1; j++) {
      listMembersId.append(dsMemberList.get(j).getId());
      listMembersId.append(" ; ");
    }
    return listMembersId.toString();
  }
}
