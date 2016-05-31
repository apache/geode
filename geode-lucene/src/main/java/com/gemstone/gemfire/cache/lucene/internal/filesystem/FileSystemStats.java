/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.gemstone.gemfire.cache.lucene.internal.filesystem;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;

public class FileSystemStats {
  private static final StatisticsType statsType;
  private static final String statsTypeName = "FileSystemStats";
  private static final String statsTypeDescription = "Statistics about in memory file system implementation";

  private final Statistics stats;

  private static final int readBytesId;
  private static final int writtenBytesId;
  private static final int fileCreatesId;
  private static final int temporaryFileCreatesId;
  private static final int fileDeletesId;
  private static final int fileRenamesId;

  static {
    final StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    statsType = f.createType(
      statsTypeName,
      statsTypeDescription,
      new StatisticDescriptor[] {
        f.createLongCounter("readBytes", "Number of bytes written", "bytes"),
        f.createLongCounter("writtenBytes", "Number of bytes read", "bytes"),
        f.createIntCounter("fileCreates", "Number files created", "files"),
        f.createIntCounter("temporaryFileCreates", "Number temporary files created", "files"),
        f.createIntCounter("fileDeletes", "Number files deleted", "files"),
        f.createIntCounter("fileRenames", "Number files renamed", "files"),
      }
    );

    readBytesId = statsType.nameToId("readBytes");
    writtenBytesId = statsType.nameToId("writtenBytes");
    fileCreatesId = statsType.nameToId("fileCreates");
    temporaryFileCreatesId = statsType.nameToId("temporaryFileCreates");
    fileDeletesId = statsType.nameToId("fileDeletes");
    fileRenamesId = statsType.nameToId("fileRenames");
  }

  public FileSystemStats(StatisticsFactory f, String name) {
    this.stats = f.createAtomicStatistics(statsType, name);
  }

  public void incReadBytes(int delta) {
    stats.incLong(readBytesId, delta);
  }

  public void incWrittenBytes(int delta) {
    stats.incLong(writtenBytesId, delta);
  }

  public void incFileCreates(final int delta) {
    stats.incInt(fileCreatesId,delta);
  }

  public void incTemporaryFileCreates(final int delta) {
    stats.incInt(temporaryFileCreatesId, delta);
  }

  public void incFileDeletes(final int delta) {
    stats.incInt(fileDeletesId,delta);
  }

  public void incFileRenames(final int delta) {
    stats.incInt(fileRenamesId,delta);
  }
}
