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
 *
 */

package org.apache.geode.test.dunit.rules;

import org.apache.commons.io.FileUtils;
import org.apache.geode.test.dunit.VM;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * A server or locator inside a DUnit {@link VM}.
 */
public abstract class MemberStarterRule extends ExternalResource implements Member {
  protected TemporaryFolder temporaryFolder;
  protected String oldUserDir;

  protected File workingDir;
  protected int memberPort = -1;
  protected int jmxPort = -1;
  protected String name;

  @Override
  public void before() throws Exception {
    oldUserDir = System.getProperty("user.dir");
    if (workingDir == null) {
      temporaryFolder = new TemporaryFolder();
      temporaryFolder.create();
      workingDir = temporaryFolder.newFolder("locator").getAbsoluteFile();
    }
    System.setProperty("user.dir", workingDir.toString());
  }

  @Override
  public void after() {
    stopMember();
    FileUtils.deleteQuietly(workingDir);
    if (oldUserDir == null) {
      System.clearProperty("user.dir");
    } else {
      System.setProperty("user.dir", oldUserDir);
    }
    if (temporaryFolder != null) {
      temporaryFolder.delete();
    }
  }

  abstract void stopMember();

  @Override
  public File getWorkingDir() {
    return workingDir;
  }

  @Override
  public int getPort() {
    return memberPort;
  }

  @Override
  public int getJmxPort() {
    return jmxPort;
  }

  @Override
  public String getName() {
    return name;
  }
}
