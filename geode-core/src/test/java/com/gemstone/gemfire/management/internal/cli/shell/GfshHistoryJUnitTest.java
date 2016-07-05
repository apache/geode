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
package com.gemstone.gemfire.management.internal.cli.shell;

import static org.junit.Assert.*;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class GfshHistoryJUnitTest {

  private File gfshHistoryFile;

  private GfshConfig gfshConfig;

  @Rule
  public TemporaryFolder tempDirectory = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    teardown();
    gfshHistoryFile = tempDirectory.newFile("historyFile");
    gfshConfig = new GfshConfig(gfshHistoryFile.getAbsolutePath(),
        "",                                         // defaultPrompt
        0,                                          // historySize
        tempDirectory.getRoot().getAbsolutePath(),  // logDir
        null,                                       // logLevel
        null,                                       // logLimit
        null,                                       // logCount
        null                                        // initFileName
    );
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
    Gfsh gfsh = Gfsh.getInstance(false, new String[] {}, gfshConfig);
    gfsh.executeScriptLine("connect --fake-param=foo");

    List<String> lines = Files.readAllLines(gfshHistoryFile.toPath());
    assertEquals(2, lines.size());
    assertEquals(lines.get(1), "connect --fake-param=foo");
  }

  @Test
  public void testHistoryFileDoesNotContainPasswords() throws Exception {
    Gfsh gfsh = Gfsh.getInstance(false, new String[] {}, gfshConfig);
    gfsh.executeScriptLine("connect --password=foo --password = foo --password= goo --password =goo --password-param=blah --other-password-param=    gah");

    List<String> lines = Files.readAllLines(gfshHistoryFile.toPath());
    assertEquals("connect --password=***** --password = ***** --password= ***** --password =***** --password-param=***** --other-password-param= *****", lines.get(1));
  }

  @Test
  public void testClearHistory() throws Exception{
    Gfsh gfsh = Gfsh.getInstance(false, new String[] {}, gfshConfig);
    gfsh.executeScriptLine("connect --fake-param=foo");
    List<String> lines = Files.readAllLines(gfshHistoryFile.toPath());
    assertEquals(2, lines.size());

    // clear the history
    gfsh.clearHistory();
    assertEquals(gfsh.getGfshHistory().size(), 0);
    assertFalse(gfshHistoryFile.exists());
  }
}
