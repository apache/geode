/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import java.io.File;
import java.io.FilenameFilter;

import com.gemstone.gemfire.internal.cache.Oplog;

public class DiskStoreFilter implements FilenameFilter {
  
  private final String filterCondn;
  private final boolean includeKRF;
  public DiskStoreFilter(OplogType type, boolean includeKRF, String name) {
    this.includeKRF = includeKRF;
    this.filterCondn = new StringBuffer(type.getPrefix())
      .append(name)
      .toString();
  }
  private boolean selected(String fileName) {
    if (this.includeKRF) {
      return (fileName.endsWith(Oplog.CRF_FILE_EXT)
          || fileName.endsWith(Oplog.KRF_FILE_EXT)
          || fileName.endsWith(Oplog.DRF_FILE_EXT));

    } else {
      return (fileName.endsWith(Oplog.CRF_FILE_EXT)
          || fileName.endsWith(Oplog.DRF_FILE_EXT));
    }
  }
  public boolean accept(File f, String fileName) {
    if (selected(fileName)) {
      int positionOfLastDot = fileName.lastIndexOf('_');
      String filePathWithoutNumberAndExtension
        = fileName.substring(0, positionOfLastDot);
      boolean result = this.filterCondn.equals(filePathWithoutNumberAndExtension);
      return result;
    }
    else {
      return false;
    }
  }
}