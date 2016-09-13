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
package com.gemstone.gemfire.internal.logging.log4j.custom;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Level;

public class CustomConfiguration {

  public static final String CONFIG_FILE_NAME = "log4j2-custom.xml";
  public static final String CONFIG_LAYOUT_PREFIX = "CUSTOM";

  protected CustomConfiguration() {
  }

  public static URL openConfigResource() {
    return new CustomConfiguration().getClass().getResource(CONFIG_FILE_NAME);
  }

  public static File createConfigFileIn(final File targetFolder) throws IOException, URISyntaxException {
    URL resource = openConfigResource();
    File targetFile = new File(targetFolder, CONFIG_FILE_NAME);
    IOUtils.copy(resource.openStream(), new FileOutputStream(targetFile));
    assertThat(targetFile).hasSameContentAs(new File(resource.toURI()));
    return targetFile;
  }

  private static final String DATE = "((?:19|20)\\\\d\\\\d)/(0?[1-9]|1[012])/([12][0-9]|3[01]|0?[1-9])";

  private static final String TIME = "\\b(?<!')([xXzZ])(?!')\\b";

  public static String defineLogStatementRegex(final Level level, final String message) {
    // CUSTOM: level=%level time=%date{yyyy/MM/dd HH:mm:ss.SSS z} message=%message%nthrowable=%throwable%n
    return CONFIG_LAYOUT_PREFIX + ": level=" + level.toString() + " time=" + ".*" + " message=" + message + "\nthrowable=\n";
  }

  public static String defineLogStatementRegex(final Level level, final String message, final String throwable) {
    // CUSTOM: level=%level time=%date{yyyy/MM/dd HH:mm:ss.SSS z} message=%message%nthrowable=%throwable%n
    return CONFIG_LAYOUT_PREFIX + ": level=" + level.toString() + " time=" + ".*" + " message=" + message + "\nthrowable=" + throwable + "\n";
  }

}
