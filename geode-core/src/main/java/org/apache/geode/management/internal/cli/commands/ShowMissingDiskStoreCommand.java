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
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.partitioned.ColocatedRegionDetails;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultDataException;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ShowMissingDiskStoreCommand extends InternalGfshCommand {
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

    if (dataMembers.isEmpty()) {
      return ResultModel.createInfo(CliStrings.NO_CACHING_MEMBERS_FOUND_MESSAGE);
    }
    List<?> results = getMissingDiskStoresList(dataMembers);

    return toMissingDiskStoresTabularResult(results);
  }

  private List<?> getMissingDiskStoresList(Set<DistributedMember> members) {
    final Execution membersFunctionExecutor = getMembersFunctionExecutor(members);
    if (membersFunctionExecutor instanceof AbstractExecution) {
      ((AbstractExecution) membersFunctionExecutor).setIgnoreDepartedMembers(true);
    }

    final ResultCollector<?, ?> resultCollector =
        membersFunctionExecutor.execute(new ShowMissingDiskStoresFunction());

    final List<?> results = (List<?>) resultCollector.getResult();
    final List<?> distributedPersistentRecoveryDetails = new ArrayList<>(results.size());
    for (final Object result : results) {
      if (result instanceof Set) {
        distributedPersistentRecoveryDetails.addAll((Set) result);
      }
    }
    return distributedPersistentRecoveryDetails;
  }

  private ResultModel toMissingDiskStoresTabularResult(final List<?> resultDetails)
      throws ResultDataException {
    ResultModel result = new ResultModel();
    List<PersistentMemberPattern> missingDiskStores = new ArrayList<>();
    List<ColocatedRegionDetails> missingColocatedRegions = new ArrayList<>();

    for (Object detail : resultDetails) {
      if (detail instanceof PersistentMemberPattern) {
        missingDiskStores.add((PersistentMemberPattern) detail);
      } else if (detail instanceof ColocatedRegionDetails) {
        missingColocatedRegions.add((ColocatedRegionDetails) detail);
      } else {
        throw new ResultDataException("Unknown type of PersistentRecoveryFailures result");
      }
    }

    boolean hasMissingDiskStores = !missingDiskStores.isEmpty();
    boolean hasMissingColocatedRegions = !missingColocatedRegions.isEmpty();
    TabularResultModel missingDiskStoreSection = result.addTable(MISSING_DISK_STORES_SECTION);

    if (hasMissingDiskStores) {
      missingDiskStoreSection.setHeader("Missing Disk Stores");

      for (PersistentMemberPattern persistentMemberDetails : missingDiskStores) {
        missingDiskStoreSection.accumulate("Disk Store ID",
            persistentMemberDetails.getUUID().toString());
        missingDiskStoreSection.accumulate("Host", persistentMemberDetails.getHost().toString());
        missingDiskStoreSection.accumulate("Directory", persistentMemberDetails.getDirectory());
      }
    } else {
      missingDiskStoreSection.setHeader("No missing disk store found");
    }

    TabularResultModel missingRegionsSection = result.addTable(MISSING_COLOCATED_REGIONS_SECTION);
    if (hasMissingColocatedRegions) {
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
