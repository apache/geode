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
package org.apache.geode.distributed.internal;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Used by build to generate a default configuration properties file for use by applications
 */
public class DefaultPropertiesGenerator {

  DefaultPropertiesGenerator() {}

  public static void main(final String[] args) throws IOException {
    String targetFileName = null;
    if (ArrayUtils.isNotEmpty(args)) {
      targetFileName = args[0];
    }
    DefaultPropertiesGenerator generator = new DefaultPropertiesGenerator();
    generator.generateDefaultPropertiesFile(targetFileName);
  }

  static String getDefaultFileName() {
    return GeodeGlossary.GEMFIRE_PREFIX + "properties";
  }

  void generateDefaultPropertiesFile(final String targetFileName) throws IOException {
    String fileName = StringUtils.trimToNull(targetFileName);
    if (fileName == null) {
      fileName = getDefaultFileName();
    }

    DistributionConfig config = DistributionConfigImpl.createDefaultInstance();
    config.toFile(new File(fileName));
  }
}
