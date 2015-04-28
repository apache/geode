/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author Sourabh Bansod
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
