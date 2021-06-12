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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.partitioned.ColocatedRegionDetails;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.PersistentMemberDetails;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ShowMissingDiskStoreCommand extends GfshCommand {
  public static final String MISSING_DISK_STORES_SECTION = "missing-disk-stores";
  public static final String MISSING_COLOCATED_REGIONS_SECTION = "missing-colocated-regions";

  @CliCommand(value = CliStrings.SHOW_MISSING_DISK_STORE,
      help = CliStrings.SHOW_MISSING_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel showMissingDiskStore() {

    Set<DistributedMember> dataMembers =
        DiskStoreCommandsUtils.getNormalMembers((InternalCache) getCache());

    List<ColocatedRegionDetails> missingRegions = null;

    if (!dataMembers.isEmpty()) {
      missingRegions = getMissingColocatedRegionList(dataMembers);
    }

    DistributedSystemMXBean dsMXBean =
        ManagementService.getManagementService(getCache()).getDistributedSystemMXBean();
    PersistentMemberDetails[] missingDiskStores = dsMXBean.listMissingDiskStores();

    return toMissingDiskStoresTabularResult(missingDiskStores, missingRegions);
  }

  private List<ColocatedRegionDetails> getMissingColocatedRegionList(
      Set<DistributedMember> members) {
    final Execution membersFunctionExecutor = getMembersFunctionExecutor(members);
    if (membersFunctionExecutor instanceof AbstractExecution) {
      ((AbstractExecution) membersFunctionExecutor).setIgnoreDepartedMembers(true);
    }

    final ResultCollector<?, ?> resultCollector =
        membersFunctionExecutor.execute(new ShowMissingDiskStoresFunction());

    final List<?> results = (List<?>) resultCollector.getResult();

    // Clean up the data. For backwards compatibility, the ShowMissingDiskStoresFunction
    // sends a List of Sets. Some of the sets are Set<PersistentMemberIds>, some are
    // Set<ColocatedRegionDetails>. We want to return a List of all of the ColocatedRegionDetails,
    // and ignore the PersistentMemberIds
    return (List<ColocatedRegionDetails>) results.stream().filter(Set.class::isInstance)
        .map(Set.class::cast)
        .flatMap(Set::stream)
        .filter(ColocatedRegionDetails.class::isInstance)
        .map(ColocatedRegionDetails.class::cast)
        .collect(Collectors.toList());
  }

  private ResultModel toMissingDiskStoresTabularResult(
      PersistentMemberDetails[] missingDiskStores,
      final List<ColocatedRegionDetails> missingColocatedRegions) {
    ResultModel result = new ResultModel();

    TabularResultModel missingDiskStoreSection = result.addTable(MISSING_DISK_STORES_SECTION);

    if (missingDiskStores.length != 0) {
      missingDiskStoreSection.setHeader("Missing Disk Stores");

      for (PersistentMemberDetails persistentMemberDetails : missingDiskStores) {
        missingDiskStoreSection.accumulate("Disk Store ID",
            persistentMemberDetails.getDiskStoreId());
        missingDiskStoreSection.accumulate("Host", persistentMemberDetails.getHost());
        missingDiskStoreSection.accumulate("Directory", persistentMemberDetails.getDirectory());
      }
    } else {
      missingDiskStoreSection.setHeader("No missing disk store found");
    }

    TabularResultModel missingRegionsSection = result.addTable(MISSING_COLOCATED_REGIONS_SECTION);
    if (missingColocatedRegions == null) {
      missingRegionsSection.setHeader("No caching members found.");
    } else if (!missingColocatedRegions.isEmpty()) {
      missingRegionsSection.setHeader("Missing Colocated Regions");

      for (ColocatedRegionDetails colocatedRegionDetails : missingColocatedRegions) {
        missingRegionsSection.accumulate("Host", colocatedRegionDetails.getHost());
        missingRegionsSection.accumulate("Distributed Member", colocatedRegionDetails.getMember());
        missingRegionsSection.accumulate("Parent Region", colocatedRegionDetails.getParent());
        missingRegionsSection.accumulate("Missing Colocated Region",
            colocatedRegionDetails.getChild());
      }
    } else {
      missingRegionsSection.setHeader("No missing colocated region found");
    }

    return result;
  }
}
