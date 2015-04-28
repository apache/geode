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
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.regex.Pattern;

import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class DiskStoreCompacter {
  public static String STACKTRACE_START = "--------------------------";
  
  public static void main(String[] args) {
    String errorString      = null;
    String stackTraceString = null;
    
    String   diskStoreName = null;
    String   diskDirsStr   = null;;
    String[] diskDirs      = null;;
    String   maxOpLogSize  = null;;
    long     maxOplogSize  = -1;
    try {
      if (args.length < 3) {
        throw new IllegalArgumentException("Requires 3 arguments : <diskStoreName> <diskDirs> <maxOplogSize>") ;
      }

      Properties prop = new Properties();
      try {
        prop.load(new StringReader(args[0]+GfshParser.LINE_SEPARATOR+args[1]+GfshParser.LINE_SEPARATOR+args[2]));
      } catch (IOException e) {
        throw new IllegalArgumentException("Requires 3 arguments : <diskStoreName> <diskDirs> <maxOplogSize>");
      }

      diskStoreName = prop.getProperty(CliStrings.COMPACT_OFFLINE_DISK_STORE__NAME);
      diskDirsStr   = prop.getProperty(CliStrings.COMPACT_OFFLINE_DISK_STORE__DISKDIRS);
      diskDirs      = diskDirsStr.split(",");
      maxOpLogSize  = prop.getProperty(CliStrings.COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE);
      maxOplogSize  = Long.valueOf(maxOpLogSize);

      compact(diskStoreName, diskDirs, maxOplogSize);
    } catch (GemFireIOException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IllegalStateException) {
        String message = cause.getMessage();
        if (stringMatches(LocalizedStrings.DiskInitFile_THE_INIT_FILE_0_DOES_NOT_EXIST.toLocalizedString("(.*)"), message)) {
          errorString = CliStrings.format(CliStrings.COMPACT_OFFLINE_DISK_STORE__MSG__VERIFY_WHETHER_DISKSTORE_EXISTS_IN_0, CliUtil.arrayToString(diskDirs));
        } else {
          errorString = message;
        }
      } else if (cause instanceof DiskAccessException) {
        boolean isKnownCause = false;
        Throwable nestedCause = cause.getCause();
        if (nestedCause instanceof IOException) {
          String message = nestedCause.getMessage();
          if (stringMatches(LocalizedStrings.Oplog_THE_FILE_0_IS_BEING_USED_BY_ANOTHER_PROCESS.toLocalizedString("(.*)"), message)) {
            errorString = CliStrings.COMPACT_OFFLINE_DISK_STORE__MSG__DISKSTORE_IN_USE_COMPACT_DISKSTORE_CAN_BE_USED;
            isKnownCause = true;
          }
        }
        if (!isKnownCause) {
          errorString = CliStrings.format(CliStrings.COMPACT_OFFLINE_DISK_STORE__MSG__CANNOT_ACCESS_DISKSTORE_0_FROM_1_CHECK_GFSH_LOGS, 
                                          new Object[] {diskStoreName, CliUtil.arrayToString(diskDirs)});
        }
      } else {
        errorString = e.getMessage(); // which are other known exceptions?
      }
      stackTraceString = CliUtil.stackTraceAsString(e);
    } catch (IllegalArgumentException e) {
      errorString = e.getMessage();
      stackTraceString = CliUtil.stackTraceAsString(e);
    } finally {
      if (errorString != null) {
        System.err.println(errorString);
      }
      if (stackTraceString != null) {
        System.err.println(STACKTRACE_START);
        System.err.println(stackTraceString);
      }
    }
  }

  private static void compact(String diskStoreName, String[] diskDirs, long maxOplogSize) {
    File[] dirs = null;
    if (diskDirs != null) {
      dirs = new File[diskDirs.length];
      for (int i = 0; i < dirs.length; i++) {
        dirs[i] = new File(diskDirs[i]);
      }
    }
    try {
      DiskStoreImpl.offlineCompact(diskStoreName, dirs, false/*upgrade*/, maxOplogSize);
    } catch (Exception ex) {
      String fieldsMessage = (maxOplogSize != -1 ? CliStrings.COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE + "=" +maxOplogSize + "," : "");
      fieldsMessage += CliUtil.arrayToString(dirs);
      throw new GemFireIOException(
          CliStrings.format(
              CliStrings.COMPACT_OFFLINE_DISK_STORE__MSG__ERROR_WHILE_COMPACTING_DISKSTORE_0_WITH_1_REASON_2,
              new Object[] { diskStoreName, fieldsMessage, ex.getMessage() }), ex);
    }
  }
  
  private static boolean stringMatches(String str1, String str2) {
    return Pattern.matches(str1, str2);
  }
}
