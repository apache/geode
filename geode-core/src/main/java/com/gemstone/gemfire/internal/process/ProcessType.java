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
package com.gemstone.gemfire.internal.process;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;

/**
 * Enumeration of GemFire {@link ControllableProcess} types and the file names
 * associated with controlling its lifecycle.
 * 
 * @since GemFire 8.0
 */
public enum ProcessType {
  LOCATOR ("LOCATOR", "vf.gf.locator"),
  SERVER ("SERVER", "vf.gf.server");

  public static final String TEST_PREFIX_PROPERTY = DistributionConfig.GEMFIRE_PREFIX + "test.ProcessType.TEST_PREFIX";

  private static final String SUFFIX_PID = "pid";
  private static final String SUFFIX_STOP_REQUEST = "stop.cmd";
  private static final String SUFFIX_STATUS_REQUEST = "status.cmd";
  private static final String SUFFIX_STATUS = "status";
  
  private final String name;
  private final String fileName;
  
  private ProcessType(final String name, final String fileName) {
    this.name = name;
    this.fileName = fileName;
  }
  
  public String getPidFileName() {
    return new StringBuilder(System.getProperty(TEST_PREFIX_PROPERTY, ""))
        .append(this.fileName).append(".").append(SUFFIX_PID).toString();
  }
  
  public String getStopRequestFileName() {
    return new StringBuilder(System.getProperty(TEST_PREFIX_PROPERTY, ""))
        .append(this.fileName).append(".").append(SUFFIX_STOP_REQUEST).toString();
  }
  
  public String getStatusRequestFileName() {
    return new StringBuilder(System.getProperty(TEST_PREFIX_PROPERTY, ""))
        .append(this.fileName).append(".").append(SUFFIX_STATUS_REQUEST).toString();
  }
  
  public String getStatusFileName() {
    return new StringBuilder(System.getProperty(TEST_PREFIX_PROPERTY, ""))
        .append(this.fileName).append(".").append(SUFFIX_STATUS).toString();
  }
  
  @Override
  public String toString() {
    return this.name;
  }
}
