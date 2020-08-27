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
package org.apache.geode.management.internal.cli.commands;

import java.io.File;

import org.junit.Test;

import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.GfshConfig;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;

public class StartServerCommandModularTest {

  @Test
  public void startServer() throws Exception {
    String workingDirectory = new File(System.getProperty("user.dir")).getAbsolutePath();

    Gfsh.getInstance(false, null, new GfshConfig(), new ServiceLoaderModuleService(null));
    StartServerCommand command = new StartServerCommand();
    command.startServer("serv1", false, null, null, null, 0.0f, 0.0f, workingDirectory, false,
        false, false, 0.0f, 0.0f, false, null, null, null, false, null, null, null, 1, false,
        null, 1, null, 1, 1, null, 0, 0, null, null, 0, null, null, 10, null, null, false, null,
        null, 40404, 100, null, null, false, false, null, null, null, null, false, false);
  }
}
