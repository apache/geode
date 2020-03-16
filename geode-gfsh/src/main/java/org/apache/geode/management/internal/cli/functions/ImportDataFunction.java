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
package org.apache.geode.management.internal.cli.functions;

import java.io.File;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.cache.snapshot.SnapshotOptions;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

/****
 * Function which carries out the import of a region to a file on a member. Uses the
 * RegionSnapshotService to import the data
 *
 */
public class ImportDataFunction implements InternalFunction<Object[]> {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext<Object[]> context) {
    final Object[] args = context.getArguments();
    if (args.length < 4) {
      throw new IllegalStateException(
          "Arguments length does not match required length. Import command may have been sent from incompatible older version");
    }
    final String regionName = (String) args[0];
    final String importFileName = (String) args[1];
    final boolean invokeCallbacks = (boolean) args[2];
    final boolean parallel = (boolean) args[3];

    CliFunctionResult result;
    try {
      final Cache cache =
          ((InternalCache) context.getCache()).getCacheForProcessingClientRequests();
      final Region<Object, Object> region = cache.getRegion(regionName);
      final String hostName = cache.getDistributedSystem().getDistributedMember().getHost();
      if (region != null) {
        RegionSnapshotService<Object, Object> snapshotService = region.getSnapshotService();
        SnapshotOptions<Object, Object> options = snapshotService.createOptions();
        options.invokeCallbacks(invokeCallbacks);
        options.setParallelMode(parallel);
        File importFile = new File(importFileName);
        snapshotService.load(new File(importFileName), SnapshotFormat.GEMFIRE, options);
        String successMessage = CliStrings.format(CliStrings.IMPORT_DATA__SUCCESS__MESSAGE,
            importFile.getCanonicalPath(), hostName, regionName);
        result = new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
            successMessage);
      } else {
        result = new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
            CliStrings.format(CliStrings.REGION_NOT_FOUND, regionName));
      }

    } catch (Exception e) {
      result = new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          e.getMessage());
    }

    context.getResultSender().lastResult(result);
  }

  @Override
  public String getId() {
    return ImportDataFunction.class.getName();
  }

}
