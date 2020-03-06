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
package org.apache.geode.management.internal.cli.util;

import static org.apache.commons.lang3.StringUtils.join;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.geode.GemFireIOException;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.i18n.CliStrings;

/**
 *
 *
 * @since GemFire 7.0
 */
public class DiskStoreCompacter {
  public static final String STACKTRACE_START = "--------------------------";
  private static final String DIR_ARRAY_SEPARATOR = ", ";

  public static void main(String[] args) {
    String errorString = null;
    String stackTraceString = null;

    String diskStoreName = null;
    String[] diskDirs = null;
    boolean errored = false;
    try {
      if (args.length < 3) {
        throw new IllegalArgumentException(
            "Requires 3 arguments : <diskStoreName> <diskDirs> <maxOplogSize>");
      }

      Properties prop = new Properties();
      try {
        prop.load(new StringReader(
            args[0] + GfshParser.LINE_SEPARATOR + args[1] + GfshParser.LINE_SEPARATOR + args[2]));
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "Requires 3 arguments : <diskStoreName> <diskDirs> <maxOplogSize>");
      }

      diskStoreName = prop.getProperty(CliStrings.COMPACT_OFFLINE_DISK_STORE__NAME);
      String diskDirsStr = prop.getProperty(CliStrings.COMPACT_OFFLINE_DISK_STORE__DISKDIRS);
      diskDirs = diskDirsStr.split(",");
      String maxOpLogSize = prop.getProperty(CliStrings.COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE);
      long maxOplogSize = Long.valueOf(maxOpLogSize);

      compact(diskStoreName, diskDirs, maxOplogSize);
    } catch (GemFireIOException e) {
      errored = true;
      Throwable cause = e.getCause();
      if (cause instanceof IllegalStateException) {
        String message = cause.getMessage();
        if (stringMatches(
            String.format("The init file %s does not exist.", "(.*)"),
            message)) {
          errorString = CliStrings.format(
              CliStrings.COMPACT_OFFLINE_DISK_STORE__MSG__VERIFY_WHETHER_DISKSTORE_EXISTS_IN_0,
              join(diskDirs, DIR_ARRAY_SEPARATOR));
        } else {
          errorString = message;
        }
      } else if (cause instanceof DiskAccessException) {
        boolean isKnownCause = false;
        Throwable nestedCause = cause.getCause();
        if (nestedCause instanceof IOException) {
          String message = nestedCause.getMessage();
          if (stringMatches(String.format("The file %s is being used by another process.",
              "(.*)"), message)) {
            errorString =
                CliStrings.COMPACT_OFFLINE_DISK_STORE__MSG__DISKSTORE_IN_USE_COMPACT_DISKSTORE_CAN_BE_USED;
            isKnownCause = true;
          }
        }
        if (!isKnownCause) {
          errorString = CliStrings.format(
              CliStrings.COMPACT_OFFLINE_DISK_STORE__MSG__CANNOT_ACCESS_DISKSTORE_0_FROM_1_CHECK_GFSH_LOGS,
              diskStoreName, join(diskDirs, DIR_ARRAY_SEPARATOR));
        }
      } else {
        errorString = e.getMessage(); // which are other known exceptions?
      }
      stackTraceString = ExceptionUtils.getStackTrace(e);
    } catch (IllegalArgumentException e) {
      errored = true;
      errorString = e.getMessage();
      stackTraceString = ExceptionUtils.getStackTrace(e);
    } finally {
      if (errorString != null) {
        System.err.println(errorString);
      }
      if (stackTraceString != null) {
        System.err.println(STACKTRACE_START);
        System.err.println(stackTraceString);
      }

      if (errored) {
        System.exit(1);
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
      DiskStoreImpl.offlineCompact(diskStoreName, dirs, false/* upgrade */, maxOplogSize);
    } catch (Exception ex) {
      String fieldsMessage = (maxOplogSize != -1
          ? CliStrings.COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE + "=" + maxOplogSize + "," : "");
      fieldsMessage += join(dirs, DIR_ARRAY_SEPARATOR);
      throw new GemFireIOException(CliStrings.format(
          CliStrings.COMPACT_OFFLINE_DISK_STORE__MSG__ERROR_WHILE_COMPACTING_DISKSTORE_0_WITH_1_REASON_2,
          diskStoreName, fieldsMessage, ex.getMessage()), ex);
    }
  }

  private static boolean stringMatches(String str1, String str2) {
    return Pattern.matches(str1, str2);
  }
}
