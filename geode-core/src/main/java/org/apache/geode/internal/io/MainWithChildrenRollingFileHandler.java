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
package org.apache.geode.internal.io;

import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.FileUtil;
import org.apache.geode.internal.i18n.LocalizedStrings;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Pattern;

/**
 * Extracted from ManagerLogWriter. MainWithChildrenRollingFileHandler is used for both rolling of
 * log files and stat archive files.
 */
public class MainWithChildrenRollingFileHandler implements RollingFileHandler {

  private static final Pattern MAIN_ID_PATTERN = Pattern.compile(".+-\\d+-\\d+\\..+");
  private static final Pattern META_ID_PATTERN = Pattern.compile("meta-.+-\\d+\\..+");
  private static final Pattern CHILD_ID_PATTERN = Pattern.compile(".+-\\d+-\\d+\\..+");

  public MainWithChildrenRollingFileHandler() {}

  @Override
  public int calcNextChildId(final File file, final int mainId) {
    int result = 0;
    File dir = getParentFile(file.getAbsoluteFile());
    int endIdx1 = file.getName().indexOf('-');
    int endIdx2 = file.getName().lastIndexOf('.');
    String baseName = file.getName();
    if (endIdx1 != -1) {
      baseName = file.getName().substring(0, endIdx1);
    } else {
      baseName = file.getName().substring(0, endIdx2);
    }
    File[] children = findChildren(dir, CHILD_ID_PATTERN);

    /* Search child logs */

    for (File child : children) {
      String name = child.getName();
      // only compare the child id among the same set of files.
      if (!name.startsWith(baseName)) {
        continue;
      }
      int endIdIdx = name.lastIndexOf('-');
      int startIdIdx = name.lastIndexOf('-', endIdIdx - 1);
      String id = name.substring(startIdIdx + 1, endIdIdx);

      int startChild = name.lastIndexOf("-");
      int endChild = name.lastIndexOf(".");
      if (startChild > 0 && endChild > 0) {
        String childId = name.substring(startChild + 1, endChild);

        try {
          int mainLogId = Integer.parseInt(id);
          int childLogId = Integer.parseInt(childId);
          if (mainLogId == mainId && childLogId > result) {
            result = childLogId;
          }
        } catch (NumberFormatException ignore) {
        }
      }
    }
    result++;
    return result;
  }

  @Override
  public int calcNextMainId(final File dir, final boolean toCreateNew) {
    int result = 0;
    File[] children = findChildren(dir, MAIN_ID_PATTERN);

    /* Search child logs */
    for (File child : children) {
      String name = child.getName();
      int endIdIdx = name.lastIndexOf('-');
      int startIdIdx = name.lastIndexOf('-', endIdIdx - 1);
      String id = name.substring(startIdIdx + 1, endIdIdx);
      try {
        int mid = Integer.parseInt(id);
        if (mid > result) {
          result = mid;
        }
      } catch (NumberFormatException ignore) {
      }
    }

    /* And search meta logs */
    if (toCreateNew) {
      File[] metaFiles = findChildren(dir, META_ID_PATTERN);
      for (File metaFile : metaFiles) {
        String name = metaFile.getName();
        int endIdIdx = name.lastIndexOf('.');
        int startIdIdx = name.lastIndexOf('-', endIdIdx - 1);
        String id = name.substring(startIdIdx + 1, endIdIdx);
        try {
          int mid = Integer.parseInt(id);
          if (mid > result) {
            result = mid;
          }
        } catch (NumberFormatException ignore) {
        }
      }
      result++;
    }

    return result;
  }

  @Override
  public void checkDiskSpace(final String type, final File newFile, final long spaceLimit,
      final File dir, final LogWriterI18n logger) {
    checkDiskSpace(type, newFile, spaceLimit, dir, getFilePattern(newFile.getName()), logger);
  }

  private void checkDiskSpace(final String type, final File newFile, final long spaceLimit,
      final File dir, final Pattern pattern, final LogWriterI18n logger) {
    if (spaceLimit == 0 || pattern == null) {
      return;
    }
    File[] children = findChildrenExcept(dir, pattern, newFile);
    if (children == null) {
      if (dir.isDirectory()) {
        logger.warning(
            LocalizedStrings.ManagerLogWriter_COULD_NOT_CHECK_DISK_SPACE_ON_0_BECAUSE_JAVAIOFILELISTFILES_RETURNED_NULL_THIS_COULD_BE_CAUSED_BY_A_LACK_OF_FILE_DESCRIPTORS,
            dir);
      }
      return;
    }
    Arrays.sort(children, new Comparator() {
      @Override
      public int compare(Object o1, Object o2) {
        File f1 = (File) o1;
        File f2 = (File) o2;
        long diff = f1.lastModified() - f2.lastModified();
        if (diff < 0) {
          return -1;
        } else if (diff > 0) {
          return 1;
        } else {
          return 0;
        }
      }
    });
    long spaceUsed = 0;
    for (File child : children) {
      spaceUsed += child.length();
    }
    int idx = 0;
    while (spaceUsed >= spaceLimit && idx < children.length) { // check array index to 37388
      long childSize = children[idx].length();
      if (delete(children[idx])) {
        spaceUsed -= childSize;
        logger.info(LocalizedStrings.ManagerLogWriter_DELETED_INACTIVE__0___1_,
            new Object[] {type, children[idx]});
      } else {
        logger.warning(LocalizedStrings.ManagerLogWriter_COULD_NOT_DELETE_INACTIVE__0___1_,
            new Object[] {type, children[idx]});
      }
      idx++;
    }
    if (spaceUsed > spaceLimit) {
      logger.warning(
          LocalizedStrings.ManagerLogWriter_COULD_NOT_FREE_SPACE_IN_0_DIRECTORY_THE_SPACE_USED_IS_1_WHICH_EXCEEDS_THE_CONFIGURED_LIMIT_OF_2,
          new Object[] {type, Long.valueOf(spaceUsed), Long.valueOf(spaceLimit)});
    }
  }

  protected boolean delete(final File file) {
    return file.delete();
  }

  @Override
  public String formatId(final int id) {
    StringBuffer result = new StringBuffer(10);
    result.append('-');
    if (id < 10) {
      result.append('0');
    }
    result.append(id);
    return result.toString();
  }

  @Override
  public File getParentFile(final File file) {
    File tmp = file.getAbsoluteFile().getParentFile();
    if (tmp == null) {
      tmp = new File("."); // as a fix for bug #41474 we use "." if getParentFile returns null
    }
    return tmp;
  }

  private Pattern getFilePattern(String name) {
    int extIdx = name.lastIndexOf('.');
    String ext = "";
    if (extIdx != -1) {
      ext = "\\Q" + name.substring(extIdx) + "\\E";
      name = name.substring(0, extIdx);
    }
    name = "\\Q" + name + "\\E" + "-\\d+-\\d+" + ext;
    return Pattern.compile(name);
  }

  private File[] findChildren(final File dir, final Pattern pattern) {
    return FileUtil.listFiles(dir, new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return pattern.matcher(name).matches();
      }
    });
  }

  private File[] findChildrenExcept(final File dir, final Pattern pattern, final File exception) {
    final String exceptionName = (exception == null) ? null : exception.getName();
    return FileUtil.listFiles(dir, new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.equals(exceptionName)) {
          return false;
        } else {
          return pattern.matcher(name).matches();
        }
      }
    });
  }

}
