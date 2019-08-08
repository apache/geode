/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.statistics;

import java.io.File;

import org.apache.geode.distributed.ConfigurationProperties;

/**
 * Configuration for statistics.
 */
public interface StatisticsConfig {

  /**
   * Returns the value of the {@link ConfigurationProperties#STATISTIC_ARCHIVE_FILE} property.
   *
   * @return <code>null</code> if no file was specified
   */
  File getStatisticArchiveFile();

  /**
   * Returns the value of the {@link ConfigurationProperties#ARCHIVE_FILE_SIZE_LIMIT} property
   */
  int getArchiveFileSizeLimit();

  /**
   * Returns the value of the {@link ConfigurationProperties#ARCHIVE_DISK_SPACE_LIMIT} property
   */
  int getArchiveDiskSpaceLimit();

  /**
   * Returns the value of the {@link ConfigurationProperties#STATISTIC_SAMPLE_RATE} property
   */
  int getStatisticSampleRate();

  /**
   * Returns the value of the {@link ConfigurationProperties#STATISTIC_SAMPLING_ENABLED} property
   */
  boolean getStatisticSamplingEnabled();
}
