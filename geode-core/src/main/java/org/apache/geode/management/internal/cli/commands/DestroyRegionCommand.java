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

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.RegionAttributesData;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.RegionDestroyFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyRegionCommand implements GfshCommand {
  @CliCommand(value = {CliStrings.DESTROY_REGION}, help = CliStrings.DESTROY_REGION__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public Result destroyRegion(
      @CliOption(key = CliStrings.DESTROY_REGION__REGION, optionContext = ConverterHint.REGION_PATH,
          mandatory = true, help = CliStrings.DESTROY_REGION__REGION__HELP) String regionPath) {

    if (regionPath == null) {
      return ResultBuilder
          .createInfoResult(CliStrings.DESTROY_REGION__MSG__SPECIFY_REGIONPATH_TO_DESTROY);
    }

    if (StringUtils.isBlank(regionPath) || regionPath.equals(Region.SEPARATOR)) {
      return ResultBuilder.createInfoResult(CliStrings.format(
          CliStrings.DESTROY_REGION__MSG__REGIONPATH_0_NOT_VALID, new Object[] {regionPath}));
    }

    Result result;
    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();
    try {
      InternalCache cache = getCache();
      ManagementService managementService = ManagementService.getExistingManagementService(cache);
      String regionPathToUse = regionPath;

      if (!regionPathToUse.startsWith(Region.SEPARATOR)) {
        regionPathToUse = Region.SEPARATOR + regionPathToUse;
      }

      Set<DistributedMember> regionMembersList =
          findMembersForRegion(cache, managementService, regionPathToUse);

      if (regionMembersList.size() == 0) {
        return ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.DESTROY_REGION__MSG__COULD_NOT_FIND_REGIONPATH_0_IN_GEODE,
                regionPath, "jmx-manager-update-rate milliseconds"));
      }

      CliFunctionResult destroyRegionResult;

      ResultCollector<?, ?> resultCollector =
          CliUtil.executeFunction(RegionDestroyFunction.INSTANCE, regionPath, regionMembersList);
      List<CliFunctionResult> resultsList = (List<CliFunctionResult>) resultCollector.getResult();
      String message =
          CliStrings.format(CliStrings.DESTROY_REGION__MSG__REGION_0_1_DESTROYED, regionPath, "");

      // Only if there is an error is this set to false
      boolean isRegionDestroyed = true;
      for (CliFunctionResult aResultsList : resultsList) {
        destroyRegionResult = aResultsList;
        if (destroyRegionResult.isSuccessful()) {
          xmlEntity.set(destroyRegionResult.getXmlEntity());
        } else if (destroyRegionResult.getThrowable() != null) {
          Throwable t = destroyRegionResult.getThrowable();
          LogWrapper.getInstance().info(t.getMessage(), t);
          message = CliStrings.format(
              CliStrings.DESTROY_REGION__MSG__ERROR_OCCURRED_WHILE_DESTROYING_0_REASON_1,
              regionPath, t.getMessage());
          isRegionDestroyed = false;
        } else {
          message = CliStrings.format(
              CliStrings.DESTROY_REGION__MSG__UNKNOWN_RESULT_WHILE_DESTROYING_REGION_0_REASON_1,
              regionPath, destroyRegionResult.getMessage());
          isRegionDestroyed = false;
        }
      }
      if (isRegionDestroyed) {
        result = ResultBuilder.createInfoResult(message);
      } else {
        result = ResultBuilder.createUserErrorResult(message);
      }
    } catch (IllegalStateException e) {
      result = ResultBuilder.createUserErrorResult(CliStrings.format(
          CliStrings.DESTROY_REGION__MSG__ERROR_WHILE_DESTROYING_REGION_0_REASON_1, regionPath,
          e.getMessage()));
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(CliStrings.format(
          CliStrings.DESTROY_REGION__MSG__ERROR_WHILE_DESTROYING_REGION_0_REASON_1, regionPath,
          e.getMessage()));
    }

    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().deleteXmlEntity(xmlEntity.get(), null));
    }

    return result;
  }

  private Set<DistributedMember> findMembersForRegion(InternalCache cache,
      ManagementService managementService, String regionPath) {
    Set<DistributedMember> membersList = new HashSet<>();
    Set<String> regionMemberIds = new HashSet<>();
    MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;

    // needs to be escaped with quotes if it contains a hyphen
    if (regionPath.contains("-")) {
      regionPath = "\"" + regionPath + "\"";
    }

    String queryExp =
        MessageFormat.format(MBeanJMXAdapter.OBJECTNAME__REGION_MXBEAN, regionPath, "*");

    try {
      ObjectName queryExpON = new ObjectName(queryExp);
      Set<ObjectName> queryNames = mbeanServer.queryNames(null, queryExpON);
      if (queryNames == null || queryNames.isEmpty()) {
        return membersList; // protects against null pointer exception below
      }

      boolean addedOneRemote = false;
      for (ObjectName regionMBeanObjectName : queryNames) {
        try {
          RegionMXBean regionMXBean =
              managementService.getMBeanInstance(regionMBeanObjectName, RegionMXBean.class);
          if (regionMXBean != null) {
            RegionAttributesData regionAttributes = regionMXBean.listRegionAttributes();
            String scope = regionAttributes.getScope();
            // For Scope.LOCAL regions we need to identify each hosting member, but for
            // other scopes we just need a single member as the region destroy will be
            // propagated.
            if (Scope.LOCAL.equals(Scope.fromString(scope))) {
              regionMemberIds.add(regionMXBean.getMember());
            } else {
              if (!addedOneRemote) {
                regionMemberIds.add(regionMXBean.getMember());
                addedOneRemote = true;
              }
            }
          }
        } catch (ClassCastException e) {
          LogWriter logger = cache.getLogger();
          if (logger.finerEnabled()) {
            logger.finer(regionMBeanObjectName + " is not a " + RegionMXBean.class.getSimpleName(),
                e);
          }
        }
      }

      if (!regionMemberIds.isEmpty()) {
        membersList = getMembersByIds(cache, regionMemberIds);
      }
    } catch (MalformedObjectNameException | NullPointerException e) {
      LogWrapper.getInstance().info(e.getMessage(), e);
    }

    return membersList;
  }

  private Set<DistributedMember> getMembersByIds(InternalCache cache, Set<String> memberIds) {
    Set<DistributedMember> foundMembers = Collections.emptySet();
    if (memberIds != null && !memberIds.isEmpty()) {
      foundMembers = new HashSet<>();
      Set<DistributedMember> allNormalMembers = CliUtil.getAllNormalMembers(cache);

      for (String memberId : memberIds) {
        for (DistributedMember distributedMember : allNormalMembers) {
          if (memberId.equals(distributedMember.getId())
              || memberId.equals(distributedMember.getName())) {
            foundMembers.add(distributedMember);
          }
        }
      }
    }
    return foundMembers;
  }
}
