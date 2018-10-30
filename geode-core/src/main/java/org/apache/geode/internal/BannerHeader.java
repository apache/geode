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
package org.apache.geode.internal;

/**
 * The headers of the log {@link Banner}.
 */
public enum BannerHeader {

  LICENSE_START("Licensed to the Apache Software Foundation (ASF) under one or more"),
  BUILD_DATE(VersionDescription.BUILD_DATE),
  BUILD_ID(VersionDescription.BUILD_ID),
  BUILD_JAVA_VERSION(VersionDescription.BUILD_JAVA_VERSION),
  BUILD_PLATFORM(VersionDescription.BUILD_PLATFORM),
  PRODUCT_NAME(VersionDescription.PRODUCT_NAME),
  PRODUCT_VERSION(VersionDescription.PRODUCT_VERSION),
  SOURCE_DATE(VersionDescription.SOURCE_DATE),
  SOURCE_REPOSITORY(VersionDescription.SOURCE_REPOSITORY),
  SOURCE_REVISION(VersionDescription.SOURCE_REVISION),
  NATIVE_VERSION(VersionDescription.NATIVE_VERSION),
  RUNNING_ON(VersionDescription.RUNNING_ON),
  COMMUNICATIONS_VERSION("Communications version"),
  PROCESS_ID("Process ID"),
  USER("User"),
  CURRENT_DIR("Current dir"),
  HOME_DIR("Home dir"),
  COMMAND_LINE_PARAMETERS("Command Line Parameters"),
  CLASS_PATH("Class Path"),
  LIBRARY_PATH("Library Path"),
  SYSTEM_PROPERTIES("System Properties"),
  LOG4J2_CONFIGURATION("Log4J 2 Configuration");

  private final String displayValue;

  BannerHeader(String displayValue) {
    this.displayValue = displayValue;
  }

  public String displayValue() {
    return displayValue;
  }

  public static String[] displayValues() {
    String[] headerValues = new String[values().length];
    int i = 0;
    for (BannerHeader bannerHeader : values()) {
      headerValues[i] = bannerHeader.displayValue();
      i++;
    }
    return headerValues;
  }
}
