/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.

 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.util;


import java.io.File;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

/**
 * @author Ajay Pande
 * @since 7.0
 */


public class DiskStoreValidater {
  /**
   * @param args
   */
  public static void main(String[] args) {
    if (args.length < 2 || args.length > 2) {
      throw new IllegalArgumentException(
          "Requires only 2  arguments : <DiskStore> <Dirs>");
    }
    validate((String) args[0], (String) args[1]);    
  }

  static void validate(String diskStoreName, String diskDirs) {
    try {
      File[] dirs = null;      
      String []dirList = null;
      dirList = diskDirs.split(";");
      if (dirList != null && dirList.length  > 0) {
        dirs = new File[dirList.length];
        for (int i = 0; i < dirList.length; ++i) {
          dirs[i] = new File(dirList[i]);
        }
      } else {
        System.out.println(CliStrings.VALIDATE_DISK_STORE__MSG__NO_DIRS);        
      }
      DiskStoreImpl.validate(diskStoreName, dirs);      
    } catch (Exception e) {
      System.out.println(CliStrings.format(CliStrings.VALIDATE_DISK_STORE__MSG__ERROR,   diskStoreName, e.getMessage()));      
    }
  }
}
