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

import static org.apache.geode.cache.Region.SEPARATOR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.exceptions.NoMembersException;
import org.apache.geode.management.internal.functions.RestoreRedundancyFunction;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.management.operation.RestoreRedundancyRequest;
import org.apache.geode.management.runtime.RestoreRedundancyResponse;

@Experimental
public class RestoreRedundancyOperationPerformer {
  private static final Logger logger = LogService.getLogger();

  public static RestoreRedundancyResponse perform(Cache cache,
                                                  RestoreRedundancyRequest parameters) {
    RestoreRedundancyResponse result;

    result = executeRestoreRedundancyOnDS(
        ManagementService.getManagementService(cache),
        (InternalCache) cache,
        parameters,
        new FunctionExecutor());
    return result;
  }


  public static List<RestoreRedundancyOperationPerformer.MemberPRInfo> getMemberRegionList(
      ManagementService managementService,
      InternalCache cache,
      List<String> listExcludedRegion) {
    List<RestoreRedundancyOperationPerformer.MemberPRInfo> listMemberPRInfo = new ArrayList<>();
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
        if (regionName.startsWith(SEPARATOR)) {
          if (!excludedRegion.startsWith(SEPARATOR)) {
            excludedRegion = SEPARATOR + excludedRegion;
          }
        }
        if (excludedRegion.startsWith(SEPARATOR)) {
          if (!regionName.startsWith(SEPARATOR)) {
            regionName = SEPARATOR + regionName;
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

      if (!regionName.startsWith(SEPARATOR)) {
        regionName = SEPARATOR + regionName;
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
                RestoreRedundancyOperationPerformer.MemberPRInfo
                    memberAndItsPRRegions = new RestoreRedundancyOperationPerformer.MemberPRInfo();
                memberAndItsPRRegions.region = regionName;
                memberAndItsPRRegions.dsMemberList.add(dsmember);
                if (listMemberPRInfo.contains(memberAndItsPRRegions)) {
                  // add member for appropriate region
                  int index = listMemberPRInfo.indexOf(memberAndItsPRRegions);
                  RestoreRedundancyOperationPerformer.MemberPRInfo
                      listMember =
                      listMemberPRInfo.get(index);
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

  /**
   * This class was introduced so that it can be mocked to all executeRebalanceOnDS to be unit
   * tested
   */
  @VisibleForTesting
  static class FunctionExecutor {
    public List<RestoreRedundancyResponse> execute(
        Function<RestoreRedundancyRequest> restoreRedundancyFunction,
        RestoreRedundancyRequest functionArgs,
        DistributedMember dsMember) {
      return (List<RestoreRedundancyResponse>) ManagementUtils
          .executeFunction(restoreRedundancyFunction,
              functionArgs, Collections.singleton(dsMember)).getResult();
    }
  }

  @VisibleForTesting
  static RestoreRedundancyResponse executeRestoreRedundancyOnDS(ManagementService managementService,
                                                                InternalCache cache,
                                                                RestoreRedundancyRequest restoreRedundancyRequest,
                                                                FunctionExecutor functionExecutor) {

    logger.info("MLH executeRestoreRedundancyOnDS entered");

    RestoreRedundancyResponseImpl restoreRedundancyResponseImpl = new RestoreRedundancyResponseImpl();
    RestoreRedundancyResponse restoreRedundancyResponse = restoreRedundancyResponseImpl;

    List<RestoreRedundancyOperationPerformer.MemberPRInfo> listMemberRegion =
        getMemberRegionList(managementService, cache, restoreRedundancyRequest.getExcludeRegions());

    logger.info(
        "MLH executeRestoreRedundancyOnDS 1 listMemberRegion.size()= " + listMemberRegion.size());

    if (listMemberRegion.size() == 0) {
      restoreRedundancyResponseImpl.setStatusMessage(CliStrings.REDUNDANCY__MSG__NO_RESTORE_REDUNDANCY_REGIONS_ON_DS);
      restoreRedundancyResponseImpl.setSuccess(true);
      return restoreRedundancyResponse;
    }

    logger.info("MLH executeRestoreRedundancyOnDS 4");
    RestoreRedundancyOperationPerformer.MemberPRInfo memberPR = listMemberRegion.get(0);
    try {
      logger.info("MLH executeRestoreRedundancyOnDS 5 looking at PR " + memberPR.region);
      // check if there are more than one members associated with region for rebalancing
      if (memberPR.dsMemberList.size() >= 1) {
        DistributedMember dsMember = memberPR.dsMemberList.get(0);
        Function<RestoreRedundancyRequest> restoreRedundancyFunction =
            new RestoreRedundancyFunction();
        List<RestoreRedundancyResponse> resultList;

        try {
          if (checkMemberPresence(cache, dsMember)) {
            logger.info("MLH executeRestoreRedundancyOnDS 7");
            resultList =
                functionExecutor
                    .execute(restoreRedundancyFunction, restoreRedundancyRequest, dsMember);
            restoreRedundancyResponse = resultList.get(0);
            logger.info("MLH executeRestoreRedundancyOnDS 8 restoreRedundancyResponse = "
                + restoreRedundancyResponse);

          }
          logger.info("MLH executeRestoreRedundancyOnDS 10");
        } catch (Exception ex) {
          logger.info("MLH executeRestoreRedundancyOnDS 11 Exception ", ex);
          return restoreRedundancyResponse;
        }

      }
    } catch (NoMembersException e) {
      logger.info("MLH executeRestoreRedundancyOnDS 13 NoMembersException ", e);
      return restoreRedundancyResponse;
    }

    logger.info("MLH executeRestoreRedundancyOnDS 15 results = " + restoreRedundancyResponse);
    return restoreRedundancyResponse;
  }


  public static class MemberPRInfo {
    public List<DistributedMember> dsMemberList;
    public String region;

    public MemberPRInfo() {
      region = "";
      dsMemberList = new ArrayList<>();
    }

    @Override
    public boolean equals(Object o2) {
      if (o2 instanceof RestoreRedundancyOperationPerformer.MemberPRInfo) {
        return this.region.equals(((RestoreRedundancyOperationPerformer.MemberPRInfo) o2).region);
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
