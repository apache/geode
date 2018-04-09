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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.logging.log4j.LogWriterAppender;
import org.apache.geode.internal.logging.log4j.LogWriterAppenders;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class InternalLocatorIntegrationTest {

  private Locator locator;
  private LogWriterAppender appender;

  @Rule
  public TemporaryFolder temporaryFolder =
      new TemporaryFolder(StringUtils.isBlank(System.getProperty("java.io.tmpdir")) ? null
          : new File(System.getProperty("java.io.tmpdir")));

  @Test
  public void testLogWriterAppenderShouldBeRemovedForALocatorWithNoDS() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(NAME, "testVM");
    properties.setProperty(LOG_FILE, temporaryFolder.newFile("testVM.log").getAbsolutePath());

    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    locator = InternalLocator.startLocator(port, null, null, null, null, false, properties, null);

    appender = LogWriterAppenders.getAppender(LogWriterAppenders.Identifier.MAIN);
    assertThat(appender).isNotNull();

    locator.stop();

    appender = LogWriterAppenders.getAppender(LogWriterAppenders.Identifier.MAIN);
    assertThat(appender).isNull();
  }

  @Test
  public void testLogWriterAppenderShouldBeRemovedForALocatorWithDS() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(NAME, "testVM");
    properties.setProperty(LOG_FILE, temporaryFolder.newFile("testVM.log").getAbsolutePath());

    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    locator = InternalLocator.startLocatorAndDS(port, null, properties);

    appender = LogWriterAppenders.getAppender(LogWriterAppenders.Identifier.MAIN);
    assertThat(appender).isNotNull();

    locator.stop();

    appender = LogWriterAppenders.getAppender(LogWriterAppenders.Identifier.MAIN);
    assertThat(appender).isNull();
  }
}
