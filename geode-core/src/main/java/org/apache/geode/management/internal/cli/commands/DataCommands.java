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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.functions.DataCommandFunction;
import org.apache.geode.management.internal.cli.functions.DataCommandFunction.SelectExecStep;
import org.apache.geode.management.internal.cli.functions.ExportDataFunction;
import org.apache.geode.management.internal.cli.functions.ImportDataFunction;
import org.apache.geode.management.internal.cli.functions.RebalanceFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.multistep.CLIMultiStepHelper;
import org.apache.geode.management.internal.cli.multistep.CLIStep;
import org.apache.geode.management.internal.cli.multistep.MultiStepCommand;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.shiro.subject.Subject;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @since GemFire 7.0
 */
public class DataCommands implements GfshCommand {

  private final int resultItemCount = 9;

  private final ExportDataFunction exportDataFunction = new ExportDataFunction();

  private final ImportDataFunction importDataFunction = new ImportDataFunction();

  @Override
  public Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  @Override
  public InternalCache getCache() {
    return (InternalCache) CacheFactory.getAnyInstance();
  }

  @CliCommand(value = CliStrings.REBALANCE, help = CliStrings.REBALANCE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public Result rebalance(
      @CliOption(key = CliStrings.REBALANCE__INCLUDEREGION,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.REBALANCE__INCLUDEREGION__HELP) String[] includeRegions,
      @CliOption(key = CliStrings.REBALANCE__EXCLUDEREGION,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.REBALANCE__EXCLUDEREGION__HELP) String[] excludeRegions,
      @CliOption(key = CliStrings.REBALANCE__TIMEOUT, unspecifiedDefaultValue = "-1",
          help = CliStrings.REBALANCE__TIMEOUT__HELP) long timeout,
      @CliOption(key = CliStrings.REBALANCE__SIMULATE, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.REBALANCE__SIMULATE__HELP) boolean simulate) {

    ExecutorService commandExecutors = Executors.newSingleThreadExecutor();
    List<Future<Result>> commandResult = new ArrayList<>();
    Result result;
    try {
      commandResult.add(commandExecutors
          .submit(new ExecuteRebalanceWithTimeout(includeRegions, excludeRegions, simulate)));

      Future<Result> fs = commandResult.get(0);
      if (timeout > 0) {
        result = fs.get(timeout, TimeUnit.SECONDS);
      } else {
        result = fs.get();

      }
    } catch (TimeoutException timeoutException) {
      result = ResultBuilder.createInfoResult(CliStrings.REBALANCE__MSG__REBALANCE_WILL_CONTINUE);

    } catch (Exception ex) {
      result = ResultBuilder.createGemFireErrorResult(CliStrings.format(
          CliStrings.REBALANCE__MSG__EXCEPTION_OCCRED_WHILE_REBALANCING_0, ex.getMessage()));
    }
    LogWrapper.getInstance().info("Rebalance returning result >>>" + result);
    return result;
  }

  private class ExecuteRebalanceWithTimeout implements Callable<Result> {
    String[] includeRegions = null;
    String[] excludeRegions = null;
    boolean simulate;
    InternalCache cache = getCache();

    @Override
    public Result call() throws Exception {
      return executeRebalanceWithTimeout(includeRegions, excludeRegions, simulate);
    }

    ExecuteRebalanceWithTimeout(String[] includedRegions, String[] excludedRegions,
        boolean toSimulate) {
      includeRegions = includedRegions;
      excludeRegions = excludedRegions;
      simulate = toSimulate;
    }

    Result executeRebalanceWithTimeout(String[] includeRegions, String[] excludeRegions,
        boolean simulate) {

      Result result = null;
      try {
        RebalanceOperation op;

        if (ArrayUtils.isNotEmpty(includeRegions)) {
          CompositeResultData rebalanceResultData = ResultBuilder.createCompositeResultData();
          int index = 0;

          for (String regionName : includeRegions) {

            // To be removed after region Name specification with "/" is fixed
            regionName = regionName.startsWith("/") ? regionName : ("/" + regionName);
            Region region = cache.getRegion(regionName);

            if (region == null) {
              DistributedMember member = getAssociatedMembers(regionName, cache);

              if (member == null) {
                LogWrapper.getInstance().info(CliStrings.format(
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

              if (simulate) {
                List resultList;
                try {
                  resultList = (ArrayList) CliUtil
                      .executeFunction(rebalanceFunction, functionArgs, member).getResult();
                } catch (Exception ex) {
                  LogWrapper.getInstance()
                      .info(CliStrings.format(
                          CliStrings.REBALANCE__MSG__EXCEPTION_IN_REBALANCE_FOR_MEMBER_0_Exception_1,
                          member.getId(), ex.getMessage()), ex);
                  rebalanceResultData.addSection()
                      .addData(CliStrings.format(
                          CliStrings.REBALANCE__MSG__EXCEPTION_IN_REBALANCE_FOR_MEMBER_0_Exception,
                          member.getId()), ex.getMessage());
                  result = ResultBuilder.buildResult(rebalanceResultData);
                  continue;
                }

                if (checkResultList(rebalanceResultData, resultList, member)) {
                  result = ResultBuilder.buildResult(rebalanceResultData);
                  continue;
                }
                List<String> rstList = tokenize((String) resultList.get(0), ",");

                result = ResultBuilder.buildResult(toCompositeResultData(rebalanceResultData,
                    (ArrayList) rstList, index, true, cache));
              } else {
                List resultList;
                try {
                  resultList = (ArrayList) CliUtil
                      .executeFunction(rebalanceFunction, functionArgs, member).getResult();
                } catch (Exception ex) {
                  LogWrapper.getInstance()
                      .info(CliStrings.format(
                          CliStrings.REBALANCE__MSG__EXCEPTION_IN_REBALANCE_FOR_MEMBER_0_Exception_1,
                          member.getId(), ex.getMessage()), ex);
                  rebalanceResultData.addSection()
                      .addData(CliStrings.format(
                          CliStrings.REBALANCE__MSG__EXCEPTION_IN_REBALANCE_FOR_MEMBER_0_Exception,
                          member.getId()), ex.getMessage());
                  result = ResultBuilder.buildResult(rebalanceResultData);
                  continue;
                }

                if (checkResultList(rebalanceResultData, resultList, member)) {
                  result = ResultBuilder.buildResult(rebalanceResultData);
                  continue;
                }
                List<String> rstList = tokenize((String) resultList.get(0), ",");

                result = ResultBuilder.buildResult(toCompositeResultData(rebalanceResultData,
                    (ArrayList) rstList, index, false, cache));
              }

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
                result = ResultBuilder.buildResult(buildResultForRebalance(rebalanceResultData,
                    op.getResults(), index, true, cache));

              } else {
                op = manager.createRebalanceFactory().start();
                // Wait until the rebalance is complete and then get the results
                result = ResultBuilder.buildResult(buildResultForRebalance(rebalanceResultData,
                    op.getResults(), index, false, cache));
              }
            }
            index++;
          }
          LogWrapper.getInstance().info("Rebalance returning result " + result);
          return result;
        } else {
          result = executeRebalanceOnDS(cache, String.valueOf(simulate), excludeRegions);
          LogWrapper.getInstance().info("Starting Rebalance simulate false result >> " + result);
        }
      } catch (Exception e) {
        result = ResultBuilder.createGemFireErrorResult(e.getMessage());
      }
      LogWrapper.getInstance().info("Rebalance returning result >>>" + result);
      return result;
    }
  }

  private List<String> tokenize(String str, String separator) {
    StringTokenizer st = new StringTokenizer(str, separator);
    List<String> rstList = new ArrayList<>();

    while (st.hasMoreTokens()) {
      rstList.add(st.nextToken());

    }
    return rstList;
  }

  private boolean checkResultList(CompositeResultData rebalanceResultData, List resultList,
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

  private Result executeRebalanceOnDS(InternalCache cache, String simulate,
      String[] excludeRegionsList) {
    Result result = null;
    int index = 1;
    CompositeResultData rebalanceResultData = ResultBuilder.createCompositeResultData();
    List<String> listExcludedRegion = new ArrayList<>();
    if (excludeRegionsList != null) {
      Collections.addAll(listExcludedRegion, excludeRegionsList);
    }
    List<MemberPRInfo> listMemberRegion = getMemberRegionList(cache, listExcludedRegion);

    if (listMemberRegion.size() == 0) {
      return ResultBuilder
          .createInfoResult(CliStrings.REBALANCE__MSG__NO_REBALANCING_REGIONS_ON_DS);
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
      return ResultBuilder
          .createInfoResult(CliStrings.REBALANCE__MSG__NO_REBALANCING_REGIONS_ON_DS);
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
              if (checkMemberPresence(dsMember, cache)) {
                resultList = (ArrayList) CliUtil
                    .executeFunction(rebalanceFunction, functionArgs, dsMember).getResult();

                if (checkResultList(rebalanceResultData, resultList, dsMember)) {
                  result = ResultBuilder.buildResult(rebalanceResultData);
                  continue;
                }

                List<String> rstList = tokenize((String) resultList.get(0), ",");
                result = ResultBuilder.buildResult(toCompositeResultData(rebalanceResultData,
                    (ArrayList) rstList, index, simulate.equals("true"), cache));
                index++;

                // Rebalancing for region is done so break and continue with other region
                break;
              } else {
                if (i == memberPR.dsMemberList.size() - 1) {
                  rebalanceResultData.addSection().addData(
                      CliStrings.format(
                          CliStrings.REBALANCE__MSG__NO_EXECUTION_FOR_REGION_0_ON_MEMBERS_1,
                          memberPR.region, listOfAllMembers(memberPR.dsMemberList)),
                      CliStrings.REBALANCE__MSG__MEMBERS_MIGHT_BE_DEPARTED);
                  result = ResultBuilder.buildResult(rebalanceResultData);
                } else {
                  continue;
                }
              }
            } catch (Exception ex) {
              if (i == memberPR.dsMemberList.size() - 1) {
                rebalanceResultData.addSection().addData(
                    CliStrings.format(
                        CliStrings.REBALANCE__MSG__NO_EXECUTION_FOR_REGION_0_ON_MEMBERS_1,
                        memberPR.region, listOfAllMembers(memberPR.dsMemberList)),
                    CliStrings.REBALANCE__MSG__REASON + ex.getMessage());
                result = ResultBuilder.buildResult(rebalanceResultData);
              } else {
                continue;
              }
            }

            if (checkResultList(rebalanceResultData, resultList, dsMember)) {
              result = ResultBuilder.buildResult(rebalanceResultData);
              continue;
            }

            List<String> rstList = tokenize((String) resultList.get(0), ",");
            result = ResultBuilder.buildResult(toCompositeResultData(rebalanceResultData,
                (ArrayList) rstList, index, simulate.equals("true"), cache));
            index++;
          }
        }
      } catch (Exception e) {
        ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
            .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(e.getMessage());
        return (ResultBuilder.buildResult(errorResultData));
      }
    }
    return result;
  }

  private boolean checkMemberPresence(DistributedMember dsMember, InternalCache cache) {
    // check if member's presence just before executing function
    // this is to avoid running a function on departed members #47248
    Set<DistributedMember> dsMemberList = CliUtil.getAllNormalMembers(cache);
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

  private CompositeResultData toCompositeResultData(CompositeResultData rebalanceResulteData,
      ArrayList<String> rstlist, int index, boolean simulate, InternalCache cache) {

    // add only if there are any valid regions in results
    if (rstlist.size() > resultItemCount && StringUtils.isNotEmpty(rstlist.get(resultItemCount))) {
      TabularResultData table1 = rebalanceResulteData.addSection().addTable("Table" + index);
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
    return rebalanceResulteData;
  }

  private CompositeResultData buildResultForRebalance(CompositeResultData rebalanceResultData,
      RebalanceResults results, int index, boolean simulate, InternalCache cache) {
    Set<PartitionRebalanceInfo> regions = results.getPartitionRebalanceDetails();
    Iterator iterator = regions.iterator();

    // add only if there are valid number of regions
    if (regions.size() > 0
        && StringUtils.isNotEmpty(((PartitionRebalanceInfo) iterator.next()).getRegionPath())) {
      final TabularResultData resultData =
          rebalanceResultData.addSection().addTable("Table" + index);
      String newLine = System.getProperty("line.separator");
      StringBuilder resultStr = new StringBuilder();
      resultStr.append(newLine);

      resultData.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES);
      resultData.accumulate("Value", results.getTotalBucketCreateBytes());
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES).append(" = ")
          .append(results.getTotalBucketCreateBytes()).append(newLine);

      resultData.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM);
      resultData.accumulate("Value", results.getTotalBucketCreateTime());
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM).append(" = ")
          .append(results.getTotalBucketCreateTime()).append(newLine);

      resultData.accumulate("Rebalanced Stats",
          CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED);
      resultData.accumulate("Value", results.getTotalBucketCreatesCompleted());
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED).append(" = ")
          .append(results.getTotalBucketCreatesCompleted()).append(newLine);

      resultData.accumulate("Rebalanced Stats",
          CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES);
      resultData.accumulate("Value", results.getTotalBucketTransferBytes());
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES).append(" = ")
          .append(results.getTotalBucketTransferBytes()).append(newLine);

      resultData.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME);
      resultData.accumulate("Value", results.getTotalBucketTransferTime());
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME).append(" = ")
          .append(results.getTotalBucketTransferTime()).append(newLine);

      resultData.accumulate("Rebalanced Stats",
          CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED);
      resultData.accumulate("Value", results.getTotalBucketTransfersCompleted());
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED).append(" = ")
          .append(results.getTotalBucketTransfersCompleted()).append(newLine);

      resultData.accumulate("Rebalanced Stats",
          CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME);
      resultData.accumulate("Value", results.getTotalPrimaryTransferTime());
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME).append(" = ")
          .append(results.getTotalPrimaryTransferTime()).append(newLine);

      resultData.accumulate("Rebalanced Stats",
          CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED);
      resultData.accumulate("Value", results.getTotalPrimaryTransfersCompleted());
      resultStr.append(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED).append(" = ")
          .append(results.getTotalPrimaryTransfersCompleted()).append(newLine);

      resultData.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALTIME);
      resultData.accumulate("Value", results.getTotalTime());
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
    return rebalanceResultData;
  }

  private DistributedMember getAssociatedMembers(String region, final InternalCache cache) {
    DistributedRegionMXBean bean =
        ManagementService.getManagementService(cache).getDistributedRegionMXBean(region);

    DistributedMember member = null;

    if (bean == null) {
      return null;
    }

    String[] membersName = bean.getMembers();
    Set<DistributedMember> dsMembers = CliUtil.getAllMembers(cache);
    Iterator it = dsMembers.iterator();

    boolean matchFound = false;

    if (membersName.length > 1) {
      while (it.hasNext() && !matchFound) {
        DistributedMember dsmember = (DistributedMember) it.next();
        for (String memberName : membersName) {
          if (MBeanJMXAdapter.getMemberNameOrId(dsmember).equals(memberName)) {
            member = dsmember;
            matchFound = true;
            break;
          }
        }
      }
    }
    return member;
  }

  private List<MemberPRInfo> getMemberRegionList(InternalCache cache,
      List<String> listExcludedRegion) {
    List<MemberPRInfo> listMemberPRInfo = new ArrayList<>();
    String[] listDSRegions =
        ManagementService.getManagementService(cache).getDistributedSystemMXBean().listRegions();
    final Set<DistributedMember> dsMembers = CliUtil.getAllMembers(cache);

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
              if (MBeanJMXAdapter.getMemberNameOrId(dsmember).equals(memberName)) {
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

  @CliCommand(value = CliStrings.EXPORT_DATA, help = CliStrings.EXPORT_DATA__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  public Result exportData(
      @CliOption(key = CliStrings.EXPORT_DATA__REGION, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.EXPORT_DATA__REGION__HELP) String regionName,
      @CliOption(key = CliStrings.EXPORT_DATA__FILE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, mandatory = true,
          help = CliStrings.EXPORT_DATA__FILE__HELP) String filePath,
      @CliOption(key = CliStrings.EXPORT_DATA__MEMBER,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          optionContext = ConverterHint.MEMBERIDNAME, mandatory = true,
          help = CliStrings.EXPORT_DATA__MEMBER__HELP) String memberNameOrId) {

    getCache().getSecurityService().authorizeRegionRead(regionName);
    final DistributedMember targetMember = CliUtil.getDistributedMemberByNameOrId(memberNameOrId);
    Result result;

    if (!filePath.endsWith(CliStrings.GEODE_DATA_FILE_EXTENSION)) {
      return ResultBuilder.createUserErrorResult(CliStrings
          .format(CliStrings.INVALID_FILE_EXTENSION, CliStrings.GEODE_DATA_FILE_EXTENSION));
    }
    try {
      if (targetMember != null) {
        final String args[] = {regionName, filePath};

        ResultCollector<?, ?> rc = CliUtil.executeFunction(exportDataFunction, args, targetMember);
        List<Object> results = (List<Object>) rc.getResult();

        if (results != null) {
          Object resultObj = results.get(0);
          if (resultObj instanceof String) {
            result = ResultBuilder.createInfoResult((String) resultObj);
          } else if (resultObj instanceof Exception) {
            result = ResultBuilder.createGemFireErrorResult(((Exception) resultObj).getMessage());
          } else {
            result = ResultBuilder.createGemFireErrorResult(
                CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, CliStrings.EXPORT_DATA));
          }
        } else {
          result = ResultBuilder.createGemFireErrorResult(
              CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, CliStrings.EXPORT_DATA));
        }
      } else {
        result = ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.EXPORT_DATA__MEMBER__NOT__FOUND, memberNameOrId));
      }
    } catch (CacheClosedException e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, CliStrings.IMPORT_DATA));
    }

    return result;
  }

  @CliCommand(value = CliStrings.IMPORT_DATA, help = CliStrings.IMPORT_DATA__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  public Result importData(
      @CliOption(key = CliStrings.IMPORT_DATA__REGION, optionContext = ConverterHint.REGION_PATH,
          mandatory = true, help = CliStrings.IMPORT_DATA__REGION__HELP) String regionName,
      @CliOption(key = CliStrings.IMPORT_DATA__FILE, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.IMPORT_DATA__FILE__HELP) String filePath,
      @CliOption(key = CliStrings.IMPORT_DATA__MEMBER, mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.IMPORT_DATA__MEMBER__HELP) String memberNameOrId,
      @CliOption(key = CliStrings.IMPORT_DATA__INVOKE_CALLBACKS, unspecifiedDefaultValue = "false",
          help = CliStrings.IMPORT_DATA__INVOKE_CALLBACKS__HELP) boolean invokeCallbacks) {

    getCache().getSecurityService().authorizeRegionWrite(regionName);

    Result result;

    try {
      final DistributedMember targetMember = CliUtil.getDistributedMemberByNameOrId(memberNameOrId);

      if (!filePath.endsWith(CliStrings.GEODE_DATA_FILE_EXTENSION)) {
        return ResultBuilder.createUserErrorResult(CliStrings
            .format(CliStrings.INVALID_FILE_EXTENSION, CliStrings.GEODE_DATA_FILE_EXTENSION));
      }
      if (targetMember != null) {
        final Object args[] = {regionName, filePath, invokeCallbacks};
        ResultCollector<?, ?> rc = CliUtil.executeFunction(importDataFunction, args, targetMember);
        List<Object> results = (List<Object>) rc.getResult();

        if (results != null) {
          Object resultObj = results.get(0);

          if (resultObj instanceof String) {
            result = ResultBuilder.createInfoResult((String) resultObj);
          } else if (resultObj instanceof Exception) {
            result = ResultBuilder.createGemFireErrorResult(((Exception) resultObj).getMessage());
          } else {
            result = ResultBuilder.createGemFireErrorResult(
                CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, CliStrings.IMPORT_DATA));
          }
        } else {
          result = ResultBuilder.createGemFireErrorResult(
              CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, CliStrings.IMPORT_DATA));
        }
      } else {
        result = ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.IMPORT_DATA__MEMBER__NOT__FOUND, memberNameOrId));
      }
    } catch (CacheClosedException e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, CliStrings.IMPORT_DATA));
    }
    return result;
  }

  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @CliCommand(value = {CliStrings.PUT}, help = CliStrings.PUT__HELP)
  public Result put(
      @CliOption(key = {CliStrings.PUT__KEY}, mandatory = true,
          help = CliStrings.PUT__KEY__HELP) String key,
      @CliOption(key = {CliStrings.PUT__VALUE}, mandatory = true,
          help = CliStrings.PUT__VALUE__HELP) String value,
      @CliOption(key = {CliStrings.PUT__REGIONNAME}, mandatory = true,
          help = CliStrings.PUT__REGIONNAME__HELP,
          optionContext = ConverterHint.REGION_PATH) String regionPath,
      @CliOption(key = {CliStrings.PUT__KEYCLASS},
          help = CliStrings.PUT__KEYCLASS__HELP) String keyClass,
      @CliOption(key = {CliStrings.PUT__VALUEKLASS},
          help = CliStrings.PUT__VALUEKLASS__HELP) String valueClass,
      @CliOption(key = {CliStrings.PUT__PUTIFABSENT}, help = CliStrings.PUT__PUTIFABSENT__HELP,
          unspecifiedDefaultValue = "false") boolean putIfAbsent) {

    InternalCache cache = getCache();
    cache.getSecurityService().authorizeRegionWrite(regionPath);
    DataCommandResult dataResult;
    if (StringUtils.isEmpty(regionPath)) {
      return makePresentationResult(DataCommandResult.createPutResult(key, null, null,
          CliStrings.PUT__MSG__REGIONNAME_EMPTY, false));
    }

    if (StringUtils.isEmpty(key)) {
      return makePresentationResult(DataCommandResult.createPutResult(key, null, null,
          CliStrings.PUT__MSG__KEY_EMPTY, false));
    }

    if (StringUtils.isEmpty(value)) {
      return makePresentationResult(DataCommandResult.createPutResult(value, null, null,
          CliStrings.PUT__MSG__VALUE_EMPTY, false));
    }

    @SuppressWarnings("rawtypes")
    Region region = cache.getRegion(regionPath);
    DataCommandFunction putfn = new DataCommandFunction();
    if (region == null) {
      Set<DistributedMember> memberList = getRegionAssociatedMembers(regionPath, getCache(), false);
      if (CollectionUtils.isNotEmpty(memberList)) {
        DataCommandRequest request = new DataCommandRequest();
        request.setCommand(CliStrings.PUT);
        request.setValue(value);
        request.setKey(key);
        request.setKeyClass(keyClass);
        request.setRegionName(regionPath);
        request.setValueClass(valueClass);
        request.setPutIfAbsent(putIfAbsent);
        dataResult = callFunctionForRegion(request, putfn, memberList);
      } else {
        dataResult = DataCommandResult.createPutInfoResult(key, value, null,
            CliStrings.format(CliStrings.PUT__MSG__REGION_NOT_FOUND_ON_ALL_MEMBERS, regionPath),
            false);
      }
    } else {
      dataResult = putfn.put(key, value, putIfAbsent, keyClass, valueClass, regionPath);
    }
    dataResult.setKeyClass(keyClass);
    if (valueClass != null) {
      dataResult.setValueClass(valueClass);
    }
    return makePresentationResult(dataResult);
  }

  private Result makePresentationResult(DataCommandResult dataResult) {
    if (dataResult != null) {
      return dataResult.toCommandResult();
    } else {
      return ResultBuilder.createGemFireErrorResult("Error executing data command");
    }
  }

  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @CliCommand(value = {CliStrings.GET}, help = CliStrings.GET__HELP)
  public Result get(
      @CliOption(key = {CliStrings.GET__KEY}, mandatory = true,
          help = CliStrings.GET__KEY__HELP) String key,
      @CliOption(key = {CliStrings.GET__REGIONNAME}, mandatory = true,
          help = CliStrings.GET__REGIONNAME__HELP,
          optionContext = ConverterHint.REGION_PATH) String regionPath,
      @CliOption(key = {CliStrings.GET__KEYCLASS},
          help = CliStrings.GET__KEYCLASS__HELP) String keyClass,
      @CliOption(key = {CliStrings.GET__VALUEKLASS},
          help = CliStrings.GET__VALUEKLASS__HELP) String valueClass,
      @CliOption(key = CliStrings.GET__LOAD, unspecifiedDefaultValue = "true",
          specifiedDefaultValue = "true",
          help = CliStrings.GET__LOAD__HELP) Boolean loadOnCacheMiss) {

    InternalCache cache = getCache();
    cache.getSecurityService().authorizeRegionRead(regionPath, key);
    DataCommandResult dataResult;

    if (StringUtils.isEmpty(regionPath)) {
      return makePresentationResult(DataCommandResult.createGetResult(key, null, null,
          CliStrings.GET__MSG__REGIONNAME_EMPTY, false));
    }

    if (StringUtils.isEmpty(key)) {
      return makePresentationResult(DataCommandResult.createGetResult(key, null, null,
          CliStrings.GET__MSG__KEY_EMPTY, false));
    }

    @SuppressWarnings("rawtypes")
    Region region = cache.getRegion(regionPath);
    DataCommandFunction getfn = new DataCommandFunction();
    if (region == null) {
      Set<DistributedMember> memberList = getRegionAssociatedMembers(regionPath, getCache(), false);
      if (CollectionUtils.isNotEmpty(memberList)) {
        DataCommandRequest request = new DataCommandRequest();
        request.setCommand(CliStrings.GET);
        request.setKey(key);
        request.setKeyClass(keyClass);
        request.setRegionName(regionPath);
        request.setValueClass(valueClass);
        request.setLoadOnCacheMiss(loadOnCacheMiss);
        Subject subject = cache.getSecurityService().getSubject();
        if (subject != null) {
          request.setPrincipal(subject.getPrincipal());
        }
        dataResult = callFunctionForRegion(request, getfn, memberList);
      } else {
        dataResult = DataCommandResult.createGetInfoResult(key, null, null,
            CliStrings.format(CliStrings.GET__MSG__REGION_NOT_FOUND_ON_ALL_MEMBERS, regionPath),
            false);
      }
    } else {
      dataResult = getfn.get(null, key, keyClass, valueClass, regionPath, loadOnCacheMiss,
          cache.getSecurityService());
    }
    dataResult.setKeyClass(keyClass);
    if (valueClass != null) {
      dataResult.setValueClass(valueClass);
    }

    return makePresentationResult(dataResult);
  }

  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @CliCommand(value = {CliStrings.LOCATE_ENTRY}, help = CliStrings.LOCATE_ENTRY__HELP)
  public Result locateEntry(
      @CliOption(key = {CliStrings.LOCATE_ENTRY__KEY}, mandatory = true,
          help = CliStrings.LOCATE_ENTRY__KEY__HELP) String key,
      @CliOption(key = {CliStrings.LOCATE_ENTRY__REGIONNAME}, mandatory = true,
          help = CliStrings.LOCATE_ENTRY__REGIONNAME__HELP,
          optionContext = ConverterHint.REGION_PATH) String regionPath,
      @CliOption(key = {CliStrings.LOCATE_ENTRY__KEYCLASS},
          help = CliStrings.LOCATE_ENTRY__KEYCLASS__HELP) String keyClass,
      @CliOption(key = {CliStrings.LOCATE_ENTRY__VALUEKLASS},
          help = CliStrings.LOCATE_ENTRY__VALUEKLASS__HELP) String valueClass,
      @CliOption(key = {CliStrings.LOCATE_ENTRY__RECURSIVE},
          help = CliStrings.LOCATE_ENTRY__RECURSIVE__HELP,
          unspecifiedDefaultValue = "false") boolean recursive) {

    getCache().getSecurityService().authorizeRegionRead(regionPath, key);

    DataCommandResult dataResult;

    if (StringUtils.isEmpty(regionPath)) {
      return makePresentationResult(DataCommandResult.createLocateEntryResult(key, null, null,
          CliStrings.LOCATE_ENTRY__MSG__REGIONNAME_EMPTY, false));
    }

    if (StringUtils.isEmpty(key)) {
      return makePresentationResult(DataCommandResult.createLocateEntryResult(key, null, null,
          CliStrings.LOCATE_ENTRY__MSG__KEY_EMPTY, false));
    }

    DataCommandFunction locateEntry = new DataCommandFunction();
    Set<DistributedMember> memberList = getRegionAssociatedMembers(regionPath, getCache(), true);
    if (CollectionUtils.isNotEmpty(memberList)) {
      DataCommandRequest request = new DataCommandRequest();
      request.setCommand(CliStrings.LOCATE_ENTRY);
      request.setKey(key);
      request.setKeyClass(keyClass);
      request.setRegionName(regionPath);
      request.setValueClass(valueClass);
      request.setRecursive(recursive);
      dataResult = callFunctionForRegion(request, locateEntry, memberList);
    } else {
      dataResult = DataCommandResult.createLocateEntryInfoResult(key, null, null, CliStrings.format(
          CliStrings.LOCATE_ENTRY__MSG__REGION_NOT_FOUND_ON_ALL_MEMBERS, regionPath), false);
    }
    dataResult.setKeyClass(keyClass);
    if (valueClass != null) {
      dataResult.setValueClass(valueClass);
    }

    return makePresentationResult(dataResult);
  }

  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @CliCommand(value = {CliStrings.REMOVE}, help = CliStrings.REMOVE__HELP)
  public Result remove(
      @CliOption(key = {CliStrings.REMOVE__KEY}, help = CliStrings.REMOVE__KEY__HELP,
          specifiedDefaultValue = "") String key,
      @CliOption(key = {CliStrings.REMOVE__REGION}, mandatory = true,
          help = CliStrings.REMOVE__REGION__HELP,
          optionContext = ConverterHint.REGION_PATH) String regionPath,
      @CliOption(key = CliStrings.REMOVE__ALL, help = CliStrings.REMOVE__ALL__HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean removeAllKeys,
      @CliOption(key = {CliStrings.REMOVE__KEYCLASS},
          help = CliStrings.REMOVE__KEYCLASS__HELP) String keyClass) {
    InternalCache cache = getCache();
    DataCommandResult dataResult;

    if (StringUtils.isEmpty(regionPath)) {
      return makePresentationResult(DataCommandResult.createRemoveResult(key, null, null,
          CliStrings.REMOVE__MSG__REGIONNAME_EMPTY, false));
    }

    if (!removeAllKeys && (key == null)) {
      return makePresentationResult(DataCommandResult.createRemoveResult(null, null, null,
          CliStrings.REMOVE__MSG__KEY_EMPTY, false));
    }

    if (removeAllKeys) {
      cache.getSecurityService().authorizeRegionWrite(regionPath);
    } else {
      cache.getSecurityService().authorizeRegionWrite(regionPath, key);
    }

    @SuppressWarnings("rawtypes")
    Region region = cache.getRegion(regionPath);
    DataCommandFunction removefn = new DataCommandFunction();
    if (region == null) {
      Set<DistributedMember> memberList = getRegionAssociatedMembers(regionPath, getCache(), false);
      if (CollectionUtils.isNotEmpty(memberList)) {
        DataCommandRequest request = new DataCommandRequest();
        request.setCommand(CliStrings.REMOVE);
        request.setKey(key);
        request.setKeyClass(keyClass);
        request.setRemoveAllKeys(removeAllKeys ? "ALL" : null);
        request.setRegionName(regionPath);
        dataResult = callFunctionForRegion(request, removefn, memberList);
      } else {
        dataResult = DataCommandResult.createRemoveInfoResult(key, null, null,
            CliStrings.format(CliStrings.REMOVE__MSG__REGION_NOT_FOUND_ON_ALL_MEMBERS, regionPath),
            false);
      }

    } else {
      dataResult = removefn.remove(key, keyClass, regionPath, removeAllKeys ? "ALL" : null);
    }
    dataResult.setKeyClass(keyClass);

    return makePresentationResult(dataResult);
  }

  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @MultiStepCommand
  @CliCommand(value = {CliStrings.QUERY}, help = CliStrings.QUERY__HELP)
  public Object query(
      @CliOption(key = CliStrings.QUERY__QUERY, help = CliStrings.QUERY__QUERY__HELP,
          mandatory = true) final String query,
      @CliOption(key = CliStrings.QUERY__STEPNAME, help = "Step name",
          unspecifiedDefaultValue = CliStrings.QUERY__STEPNAME__DEFAULTVALUE) String stepName,
      @CliOption(key = CliStrings.QUERY__INTERACTIVE, help = CliStrings.QUERY__INTERACTIVE__HELP,
          unspecifiedDefaultValue = "true") final boolean interactive) {

    if (!CliUtil.isGfshVM() && stepName.equals(CliStrings.QUERY__STEPNAME__DEFAULTVALUE)) {
      return ResultBuilder.createInfoResult(CliStrings.QUERY__MSG__NOT_SUPPORTED_ON_MEMBERS);
    }

    Object[] arguments = new Object[] {query, stepName, interactive};
    CLIStep exec = new SelectExecStep(arguments);
    CLIStep display = new DataCommandFunction.SelectDisplayStep(arguments);
    CLIStep move = new DataCommandFunction.SelectMoveStep(arguments);
    CLIStep quit = new DataCommandFunction.SelectQuitStep(arguments);
    CLIStep[] steps = {exec, display, move, quit};
    return CLIMultiStepHelper.chooseStep(steps, stepName);
  }

  @CliAvailabilityIndicator({CliStrings.REBALANCE, CliStrings.GET, CliStrings.PUT,
      CliStrings.REMOVE, CliStrings.LOCATE_ENTRY, CliStrings.QUERY, CliStrings.IMPORT_DATA,
      CliStrings.EXPORT_DATA})
  public boolean dataCommandsAvailable() {
    boolean isAvailable = true; // always available on server
    if (CliUtil.isGfshVM()) { // in gfsh check if connected
      isAvailable = getGfsh() != null && getGfsh().isConnectedAndReady();
    }
    return isAvailable;
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
  }

  @SuppressWarnings("rawtypes")
  public static DataCommandResult callFunctionForRegion(DataCommandRequest request,
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

  @SuppressWarnings("rawtypes")
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

  // TODO:k revisit after adding support in Gfsh.java?
  public static Object[] replaceGfshEnvVar(String query, Map<String, String> gfshEnvVarMap) {
    boolean done = false;
    int startIndex = 0;
    int replacedVars = 0;
    while (!done) {
      int index1 = query.indexOf("${", startIndex);
      if (index1 == -1) {
        break;
      }
      int index2 = query.indexOf("}", index1);
      if (index2 == -1) {
        break;
      }
      String var = query.substring(index1 + 2, index2);
      String value = gfshEnvVarMap.get(var);
      if (value != null) {
        query = query.replaceAll("\\$\\{" + var + "\\}", value);
        replacedVars++;
      }
      startIndex = index2 + 1;
      if (startIndex >= query.length()) {
        done = true;
      }
    }
    return new Object[] {replacedVars, query};
  }

}
