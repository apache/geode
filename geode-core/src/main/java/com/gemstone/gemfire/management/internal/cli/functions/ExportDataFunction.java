/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import java.io.File;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.snapshot.RegionSnapshotService;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

/***
 * Function which carries out the export of a region to a file on a member.
 * Uses the RegionSnapshotService to export the data
 * 
 *
 */
public class ExportDataFunction extends FunctionAdapter implements
    InternalEntity {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public void execute(FunctionContext context) {
    final String [] args = (String [])context.getArguments();
    final String regionName = args[0];
    final String fileName = args[1];
    
    try {
      Cache cache = CacheFactory.getAnyInstance();
      Region<?,?> region = cache.getRegion(regionName);
      String hostName = cache.getDistributedSystem().getDistributedMember().getHost();
      if (region != null) {
        RegionSnapshotService<?, ?> snapshotService = region.getSnapshotService();
        final File exportFile = new File(fileName);
        snapshotService.save(exportFile, SnapshotFormat.GEMFIRE);
        String successMessage = CliStrings.format(CliStrings.EXPORT_DATA__SUCCESS__MESSAGE, regionName, exportFile.getCanonicalPath(), hostName);
        context.getResultSender().lastResult(successMessage);
      } else {
        throw new IllegalArgumentException(CliStrings.format(CliStrings.REGION_NOT_FOUND, regionName));
      }
      
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    }
  }

  public String getId() {
    return ExportDataFunction.class.getName();
  }

}
