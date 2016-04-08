/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.statistics;

import java.io.File;

/**
 * Defines the contract enabling the {@link StatArchiveHandler} to retrieve
 * configuration details (some of which may change at runtime).
 * <p/>
 * Implemented by {@link com.gemstone.gemfire.internal.HostStatSampler}.

 * @since 7.0
 * @see com.gemstone.gemfire.distributed.internal.RuntimeDistributionConfigImpl
 */
public interface StatArchiveHandlerConfig {

  /**
   * Gets the name of the archive file.
   */
  public File getArchiveFileName();
  
  /**
   * Gets the archive size limit in bytes.
   */
  public long getArchiveFileSizeLimit();
  
  /**
   * Gets the archive disk space limit in bytes.
   */
  public long getArchiveDiskSpaceLimit();

  /**
   * Returns a unique id for the sampler's system.
   */
  public long getSystemId();

  /**
   * Returns the time this sampler's system was started.
   */
  public long getSystemStartTime();

  /**
   * Returns the path to this sampler's system directory; if it has one.
   */
  public String getSystemDirectoryPath();
  
  /**
   * Returns a description of the product that the stats are on
   */
  public String getProductDescription();
}
