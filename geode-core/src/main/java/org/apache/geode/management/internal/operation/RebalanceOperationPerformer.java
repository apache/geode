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
package org.apache.geode.management.internal.operation;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.exceptions.NoMembersException;
import org.apache.geode.management.internal.functions.RebalanceFunction;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceRegionResult;
import org.apache.geode.management.runtime.RebalanceResult;

@Experimental
public class RebalanceOperationPerformer {
  public static RebalanceResult perform(Cache cache, RebalanceOperation parameters) {

    List<String> includeRegions = parameters.getIncludeRegions();
    List<String> excludeRegions = parameters.getExcludeRegions();
    boolean simulate = parameters.isSimulate();

    RebalanceResultImpl result = new RebalanceResultImpl();
    result.setSuccess(false);

    if (includeRegions.size() != 0) {

      List<RebalanceRegionResult> rebalanceRegionResults = new ArrayList<>();

      NoMembersException latestNoMembersException = null;

      for (String regionName : includeRegions) {

        // Handle exclude / include regions
        RebalanceRegionResult rebalanceResult;
        try {
          rebalanceResult = performRebalance(cache, regionName, simulate);
        } catch (NoMembersException ex) {
          latestNoMembersException = ex;
          continue;
        } catch (Exception e) {
          result.setStatusMessage(e.getMessage());
          continue;
        }
        rebalanceRegionResults.add(rebalanceResult);
        result.setSuccess(true);
      }

      if (latestNoMembersException != null && !result.getSuccess()) {
        result.setStatusMessage(latestNoMembersException.getMessage());
      } else {
        result.setRebalanceSummary(rebalanceRegionResults);
      }

      return result;
    } else {
      result =
          (RebalanceResultImpl) executeRebalanceOnDS(ManagementService.getManagementService(cache),
              (InternalCache) cache,
              String.valueOf(simulate), excludeRegions, new FunctionExecutor());
    }

    return result;
  }

  private static RebalanceRegionResult performRebalance(Cache cache, String regionName,
      boolean simulate)
      throws InterruptedException {
    // To be removed after region Name specification with "/" is fixed
    regionName = regionName.startsWith("/") ? regionName : ("/" + regionName);
    Region region = cache.getRegion(regionName);

    if (region == null) {
      DistributedMember member = getAssociatedMembers(regionName, (InternalCache) cache);

      if (member == null) {
        throw new NoMembersException(MessageFormat.format(
            CliStrings.REBALANCE__MSG__NO_ASSOCIATED_DISTRIBUTED_MEMBER, regionName));
      }

      Function rebalanceFunction = new RebalanceFunction();
      Object[] functionArgs = new Object[3];
      functionArgs[0] = simulate ? "true" : "false";
      Set<String> setRegionName = new HashSet<>();
      setRegionName.add(regionName);
      functionArgs[1] = setRegionName;

      functionArgs[2] = null;

      List resultList = null;
      try {
        resultList = (ArrayList) ManagementUtils
            .executeFunction(rebalanceFunction, functionArgs, Collections.singleton(member))
            .getResult();
      } catch (Exception ex) {

      }

      RebalanceRegionResult result = new RebalanceRegionResultImpl();
      if (resultList != null && resultList.size() > 0) {
        List<String> rstList = Arrays.asList(((String) resultList.get(0)).split(","));

        result = (RebalanceRegionResultImpl) toRebalanceRegionResut(rstList);
      }

      return result;
    } else {

      ResourceManager manager = cache.getResourceManager();
      RebalanceFactory rbFactory = manager.createRebalanceFactory();
      Set<String> includeRegionSet = new HashSet<>();
      includeRegionSet.add(regionName);
      rbFactory.includeRegions(includeRegionSet);

      org.apache.geode.cache.control.RebalanceOperation op;
      if (simulate) {
        op = manager.createRebalanceFactory().simulate();
      } else {
        op = manager.createRebalanceFactory().start();
      }
      // Wait until the rebalance is complete and then get the results
      RebalanceResults results = op.getResults();

      // translate to the return type we want
      RebalanceRegionResultImpl result = new RebalanceRegionResultImpl();
      result.setRegionName(regionName.replace("/", ""));
      result.setBucketCreateBytes(results.getTotalBucketCreateBytes());
      result.setBucketCreateTimeInMilliseconds(results.getTotalBucketCreateTime());
      result.setBucketCreatesCompleted(results.getTotalBucketCreatesCompleted());
      result.setBucketTransferBytes(results.getTotalBucketTransferBytes());
      result.setBucketTransferTimeInMilliseconds(results.getTotalBucketTransferTime());
      result.setBucketTransfersCompleted(results.getTotalBucketTransfersCompleted());
      result.setPrimaryTransferTimeInMilliseconds(results.getTotalPrimaryTransferTime());
      result.setPrimaryTransfersCompleted(results.getTotalPrimaryTransfersCompleted());
      result.setTimeInMilliseconds(results.getTotalTime());

      return result;
    }
  }

  private static DistributedMember getAssociatedMembers(String region, final InternalCache cache) {
    DistributedRegionMXBean bean =
        ManagementService.getManagementService(cache).getDistributedRegionMXBean(region);

    DistributedMember member = null;

    if (bean == null) {
      return null;
    }

    String[] membersName = bean.getMembers();
    Set<DistributedMember> dsMembers = ManagementUtils.getAllMembers(cache);
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

  private static List<MemberPRInfo> getMemberRegionList(ManagementService managementService,
      InternalCache cache,
      List<String> listExcludedRegion) {
    List<MemberPRInfo> listMemberPRInfo = new ArrayList<>();
    String[] listDSRegions =
        managementService.getDistributedSystemMXBean().listRegions();
    final Set<DistributedMember> dsMembers = ManagementUtils.getAllMembers(cache);

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
      DistributedRegionMXBean bean = managementService.getDistributedRegionMXBean(regionName);

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

  private static boolean checkMemberPresence(InternalCache cache, DistributedMember dsMember) {
    // check if member's presence just before executing function
    // this is to avoid running a function on departed members #47248
    Set<DistributedMember> dsMemberList = ManagementUtils.getAllNormalMembers(cache);
    return dsMemberList.contains(dsMember);
  }

  private static String listOfAllMembers(ArrayList<DistributedMember> dsMemberList) {
    StringBuilder listMembersId = new StringBuilder();
    for (int j = 0; j < dsMemberList.size() - 1; j++) {
      listMembersId.append(dsMemberList.get(j).getId());
      listMembersId.append(" ; ");
    }
    return listMembersId.toString();
  }

  private static boolean checkResultList(List<String> errors, List<Object> resultList,
      DistributedMember member) {
    boolean toContinueForOtherMembers = false;
    if (CollectionUtils.isNotEmpty(resultList)) {
      for (Object object : resultList) {
        if (object instanceof Exception) {
          errors.add(
              MessageFormat.format(CliStrings.REBALANCE__MSG__NO_EXECUTION, member.getId()) + ": " +
                  ((Exception) object).getMessage());

          toContinueForOtherMembers = true;
          break;
        } else if (object instanceof Throwable) {
          errors.add(
              MessageFormat.format(CliStrings.REBALANCE__MSG__NO_EXECUTION, member.getId()) + ": " +
                  ((Throwable) object).getMessage());

          toContinueForOtherMembers = true;
          break;
        }
      }
    } else {
      toContinueForOtherMembers = true;
    }
    return toContinueForOtherMembers;
  }

  /**
   * This class was introduced so that it can be mocked
   * to all executeRebalanceOnDS to be unit tested
   */
  @VisibleForTesting
  static class FunctionExecutor {
    public List<Object> execute(Function rebalanceFunction, Object[] functionArgs,
        DistributedMember dsMember) {
      return (List<Object>) ManagementUtils.executeFunction(rebalanceFunction,
          functionArgs, Collections.singleton(dsMember)).getResult();
    }
  }

  @VisibleForTesting
  static RebalanceResult executeRebalanceOnDS(ManagementService managementService,
      InternalCache cache, String simulate,
      List<String> excludeRegionsList, FunctionExecutor functionExecutor) {
    RebalanceResultImpl rebalanceResult = new RebalanceResultImpl();
    rebalanceResult.setSuccess(false);
    List<String> errors = new ArrayList<>();

    List<MemberPRInfo> listMemberRegion =
        getMemberRegionList(managementService, cache, excludeRegionsList);

    if (listMemberRegion.size() == 0) {
      rebalanceResult.setStatusMessage(CliStrings.REBALANCE__MSG__NO_REBALANCING_REGIONS_ON_DS);
      rebalanceResult.setSuccess(true);
      return rebalanceResult;
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
      rebalanceResult.setStatusMessage(CliStrings.REBALANCE__MSG__NO_REBALANCING_REGIONS_ON_DS);
      rebalanceResult.setSuccess(true);
      return rebalanceResult;
    }

    List<RebalanceRegionResult> rebalanceRegionResults = new ArrayList<>();
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

            List<Object> resultList = new ArrayList<>();

            try {
              if (checkMemberPresence(cache, dsMember)) {
                resultList = functionExecutor.execute(rebalanceFunction, functionArgs, dsMember);
                if (checkResultList(errors, resultList, dsMember)) {
                  continue;
                }

                List<String> rstList = Arrays.asList(((String) resultList.get(0)).split(","));
                rebalanceRegionResults.add(toRebalanceRegionResut(rstList));
                rebalanceResult.setSuccess(true);

                // Rebalancing for region is done so break and continue with other region
                break;
              } else {
                if (i == memberPR.dsMemberList.size() - 1) {
                  // The last member hosting this region departed so no need to rebalance it.
                  // So act as if we never tried to rebalance this region.
                  // Break to get out of this inner loop and try the next region (if any).
                  break;
                } else {
                  continue;
                }
              }
            } catch (Exception ex) {
              if (i == memberPR.dsMemberList.size() - 1) {
                errors.add(
                    MessageFormat.format(
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
            rebalanceRegionResults.add(toRebalanceRegionResut(rstList));
            rebalanceResult.setSuccess(true);
          }
        }
      } catch (NoMembersException e) {
        rebalanceResult.setStatusMessage(e.getMessage());
        rebalanceResult.setRebalanceSummary(rebalanceRegionResults);
        return rebalanceResult;
      }
    }
    rebalanceResult.setRebalanceSummary(rebalanceRegionResults);
    if (0 == rebalanceRegionResults.size()) {
      rebalanceResult.setSuccess(false);
    }
    return rebalanceResult;
  }

  private static RebalanceRegionResult toRebalanceRegionResut(List<String> rstList) {
    RebalanceRegionResultImpl result = new RebalanceRegionResultImpl();
    result.setBucketCreateBytes(Long.parseLong(rstList.get(0)));
    result.setBucketCreateTimeInMilliseconds(Long.parseLong(rstList.get(1)));
    result.setBucketCreatesCompleted(Integer.parseInt(rstList.get(2)));
    result.setBucketTransferBytes(Long.parseLong(rstList.get(3)));
    result.setBucketTransferTimeInMilliseconds(Long.parseLong(rstList.get(4)));
    result.setBucketTransfersCompleted(Integer.parseInt(rstList.get(5)));
    result.setPrimaryTransferTimeInMilliseconds(Long.parseLong(rstList.get(6)));
    result.setPrimaryTransfersCompleted(Integer.parseInt(rstList.get(7)));
    result.setTimeInMilliseconds(Long.parseLong(rstList.get(8)));
    result.setRegionName(rstList.get(9).replace("/", ""));

    return result;
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
      if (o2 instanceof MemberPRInfo) {
        return this.region.equals(((MemberPRInfo) o2).region);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return this.region.hashCode();
    }
  }
}
