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

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.partitioned.ColocatedRegionDetails;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.ResultDataException;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ShowMissingDiskStoreCommand extends GfshCommand {
  @CliCommand(value = CliStrings.SHOW_MISSING_DISK_STORE,
      help = CliStrings.SHOW_MISSING_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result showMissingDiskStore() {

    try {
      Set<DistributedMember> dataMembers = DiskStoreCommandsUtils.getNormalMembers(getCache());

      if (dataMembers.isEmpty()) {
        return ResultBuilder.createInfoResult(CliStrings.NO_CACHING_MEMBERS_FOUND_MESSAGE);
      }
      List<Object> results = getMissingDiskStoresList(dataMembers);
      return toMissingDiskStoresTabularResult(results);
    } catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(
          CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, CliStrings.SHOW_MISSING_DISK_STORE));
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      if (t.getMessage() == null) {
        return ResultBuilder.createGemFireErrorResult(
            String.format(CliStrings.SHOW_MISSING_DISK_STORE__ERROR_MESSAGE, t));
      }
      return ResultBuilder.createGemFireErrorResult(
          String.format(CliStrings.SHOW_MISSING_DISK_STORE__ERROR_MESSAGE, t.getMessage()));
    }
  }

  private List<Object> getMissingDiskStoresList(Set<DistributedMember> members) {
    final Execution membersFunctionExecutor = getMembersFunctionExecutor(members);
    if (membersFunctionExecutor instanceof AbstractExecution) {
      ((AbstractExecution) membersFunctionExecutor).setIgnoreDepartedMembers(true);
    }

    final ResultCollector<?, ?> resultCollector =
        membersFunctionExecutor.execute(new ShowMissingDiskStoresFunction());

    final List<?> results = (List<?>) resultCollector.getResult();
    final List<Object> distributedPersistentRecoveryDetails = new ArrayList<>(results.size());
    for (final Object result : results) {
      if (result instanceof Set) {
        distributedPersistentRecoveryDetails.addAll((Set<Object>) result);
      }
    }
    return distributedPersistentRecoveryDetails;
  }

  private Result toMissingDiskStoresTabularResult(final List<Object> resultDetails)
      throws ResultDataException {
    CompositeResultData crd = ResultBuilder.createCompositeResultData();
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
    if (hasMissingDiskStores) {
      CompositeResultData.SectionResultData missingDiskStoresSection = crd.addSection();
      missingDiskStoresSection.setHeader("Missing Disk Stores");
      TabularResultData missingDiskStoreData = missingDiskStoresSection.addTable();

      for (PersistentMemberPattern persistentMemberDetails : missingDiskStores) {
        missingDiskStoreData.accumulate("Disk Store ID", persistentMemberDetails.getUUID());
        missingDiskStoreData.accumulate("Host", persistentMemberDetails.getHost());
        missingDiskStoreData.accumulate("Directory", persistentMemberDetails.getDirectory());
      }
    } else {
      CompositeResultData.SectionResultData noMissingDiskStores = crd.addSection();
      noMissingDiskStores.setHeader("No missing disk store found");
    }
    if (hasMissingDiskStores || hasMissingColocatedRegions) {
      // For clarity, separate disk store and colocated region information
      crd.addSection().setHeader("\n");
    }

    if (hasMissingColocatedRegions) {
      CompositeResultData.SectionResultData missingRegionsSection = crd.addSection();
      missingRegionsSection.setHeader("Missing Colocated Regions");
      TabularResultData missingRegionData = missingRegionsSection.addTable();

      for (ColocatedRegionDetails colocatedRegionDetails : missingColocatedRegions) {
        missingRegionData.accumulate("Host", colocatedRegionDetails.getHost());
        missingRegionData.accumulate("Distributed Member", colocatedRegionDetails.getMember());
        missingRegionData.accumulate("Parent Region", colocatedRegionDetails.getParent());
        missingRegionData.accumulate("Missing Colocated Region", colocatedRegionDetails.getChild());
      }
    } else {
      CompositeResultData.SectionResultData noMissingColocatedRegions = crd.addSection();
      noMissingColocatedRegions.setHeader("No missing colocated region found");
    }
    return ResultBuilder.buildResult(crd);
  }
}
