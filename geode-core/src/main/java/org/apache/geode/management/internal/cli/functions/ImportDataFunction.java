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
package org.apache.geode.management.internal.cli.functions;

import java.io.File;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/****
 * Function which carries out the import of a region to a file on a member.
 * Uses the RegionSnapshotService to import the data
 *
 */
public class ImportDataFunction extends FunctionAdapter implements
    InternalEntity {

  private static final long serialVersionUID = 1L;

  public void execute(FunctionContext context) {
    final String [] args = (String [])context.getArguments();
    final String regionName = args[0];
    final String importFileName = args[1];
    
    try {
      final Cache cache = CacheFactory.getAnyInstance();
      final Region<?,?> region = cache.getRegion(regionName);
      final String hostName = cache.getDistributedSystem().getDistributedMember().getHost();
      if (region != null) {
        RegionSnapshotService<?, ?> snapshotService = region.getSnapshotService();
        File importFile = new File(importFileName);
        snapshotService.load(new File(importFileName), SnapshotFormat.GEMFIRE);
        String successMessage = CliStrings.format(CliStrings.IMPORT_DATA__SUCCESS__MESSAGE, importFile.getCanonicalPath(), hostName, regionName);
        context.getResultSender().lastResult(successMessage);
      } else {
        throw new IllegalArgumentException(CliStrings.format(CliStrings.REGION_NOT_FOUND, regionName));
      }
      
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    }
  }

  public String getId() {
    return ImportDataFunction.class.getName();
  }

}
