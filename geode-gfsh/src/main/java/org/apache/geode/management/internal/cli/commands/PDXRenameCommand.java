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

package org.apache.geode.management.internal.cli.commands;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Collection;

import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.PdxType;

public class PDXRenameCommand extends GfshCommand {
  @ShellMethod(value = CliStrings.PDX_RENAME__HELP, key = CliStrings.PDX_RENAME)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  public ResultModel pdxRename(@ShellOption(value = CliStrings.PDX_RENAME_OLD,
      help = CliStrings.PDX_RENAME_OLD__HELP) String oldClassName,

      @ShellOption(value = CliStrings.PDX_RENAME_NEW,
          help = CliStrings.PDX_RENAME_NEW__HELP) String newClassName,

      @ShellOption(value = CliStrings.PDX_DISKSTORE,
          help = CliStrings.PDX_DISKSTORE__HELP) String diskStore,

      @ShellOption(value = CliStrings.PDX_DISKDIR,
          help = CliStrings.PDX_DISKDIR__HELP) String[] diskDirs)
      throws Exception {


    final File[] dirs = new File[diskDirs.length];
    for (int i = 0; i < diskDirs.length; i++) {
      dirs[i] = new File((diskDirs[i]));
    }

    Collection<Object> results =
        DiskStoreImpl.pdxRename(diskStore, dirs, oldClassName, newClassName);

    if (results.isEmpty()) {
      return ResultModel.createError(CliStrings.format(CliStrings.PDX_RENAME__EMPTY));
    }

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);
    for (Object p : results) {
      if (p instanceof PdxType) {
        ((PdxType) p).toStream(printStream, false);
      } else {
        ((EnumInfo) p).toStream(printStream);
      }
    }
    String resultString =
        CliStrings.format(CliStrings.PDX_RENAME__SUCCESS, outputStream.toString());
    return ResultModel.createInfo(resultString);
  }
}
