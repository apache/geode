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
import org.apache.geode.internal.cache.snapshot.SnapshotOptionsImpl;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

/***
 * Function which carries out the export of a region to a file on a member. Uses the
 * RegionSnapshotService to export the data
 *
 *
 */
public class ExportDataFunction extends CliFunction<String[]> {
  private static final long serialVersionUID = 1L;

  @Override
  public CliFunctionResult executeFunction(FunctionContext<String[]> context) throws Exception {
    final String[] args = context.getArguments();
    if (args.length < 3) {
      throw new IllegalStateException(
          "Arguments length does not match required length. Export command may have been sent from incompatible older version");
    }
    final String regionName = args[0];
    final String fileName = args[1];
    final boolean parallel = Boolean.parseBoolean(args[2]);
    CliFunctionResult result;

    Cache cache = ((InternalCache) context.getCache()).getCacheForProcessingClientRequests();
    Region<Object, Object> region = cache.getRegion(regionName);
    String hostName = cache.getDistributedSystem().getDistributedMember().getHost();
    if (region != null) {
      RegionSnapshotService<Object, Object> snapshotService = region.getSnapshotService();
      final File exportFile = new File(fileName);
      if (parallel) {
        SnapshotOptions<Object, Object> options = new SnapshotOptionsImpl<>().setParallelMode(true);
        snapshotService.save(exportFile, SnapshotFormat.GEMFIRE, options);
      } else {
        snapshotService.save(exportFile, SnapshotFormat.GEMFIRE);
      }

      String successMessage = CliStrings.format(CliStrings.EXPORT_DATA__SUCCESS__MESSAGE,
          regionName, exportFile.getCanonicalPath(), hostName);
      result = new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
          successMessage);
    } else {
      result = new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(CliStrings.REGION_NOT_FOUND, regionName));
    }

    return result;
  }

  @Override
  public String getId() {
    return ExportDataFunction.class.getName();
  }

}
