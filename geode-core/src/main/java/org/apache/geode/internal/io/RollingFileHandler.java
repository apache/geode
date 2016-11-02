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

import java.io.File;

/**
 * Defines the constants and methods for rolling files (logs and stat archives).
 */
public interface RollingFileHandler {

  int calcNextMainId(final File dir, final boolean toCreateNew);

  int calcNextChildId(final File file, final int mainId);

  File getParentFile(final File file);

  String formatId(final int id);

  void checkDiskSpace(final String type, final File newFile, final long spaceLimit, final File dir,
      final LogWriterI18n logger);
}
