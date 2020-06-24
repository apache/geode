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
package org.apache.geode.management.internal.cli.shell;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;


public class GfshHistoryJUnitTest {

  private File gfshHistoryFile;

  private GfshConfig gfshConfig;

  @Rule
  public TemporaryFolder tempDirectory = new TemporaryFolder();
  private Gfsh gfsh;

  @Before
  public void setUp() throws Exception {
    teardown();
    gfshHistoryFile = tempDirectory.newFile("historyFile");
    gfshConfig = new GfshConfig(gfshHistoryFile.getAbsolutePath(), "", // defaultPrompt
        0, // historySize
        tempDirectory.getRoot().getAbsolutePath(), // logDir
        null, // logLevel
        null, // logLimit
        null, // logCount
        null // initFileName
    );
    gfsh = Gfsh.getInstance(false, new String[] {}, gfshConfig, new ServiceLoaderModuleService(
        LogService.getLogger()));
  }

  @After
  public void teardown() throws Exception {
    // Null out static instance so Gfsh can be reinitialised
    Field gfshInstance = Gfsh.class.getDeclaredField("instance");
    gfshInstance.setAccessible(true);
    gfshInstance.set(null, null);
  }

  @Test
  public void testHistoryFileIsCreated() throws Exception {
    gfsh.executeScriptLine("connect");

    List<String> lines = Files.readAllLines(gfshHistoryFile.toPath());
    assertEquals(2, lines.size());
    assertEquals(lines.get(1), "connect");
  }

  @Test
  public void testHistoryFileDoesNotContainPasswords() throws Exception {
    gfsh.executeScriptLine("connect --password=foo");

    List<String> lines = Files.readAllLines(gfshHistoryFile.toPath());
    assertEquals("connect --password=********", lines.get(1));
  }

  @Test
  public void testClearHistory() throws Exception {
    gfsh.executeScriptLine("connect");
    List<String> lines = Files.readAllLines(gfshHistoryFile.toPath());
    assertEquals(2, lines.size());

    // clear the history
    gfsh.clearHistory();
    assertEquals(gfsh.getGfshHistory().size(), 0);
  }
}
