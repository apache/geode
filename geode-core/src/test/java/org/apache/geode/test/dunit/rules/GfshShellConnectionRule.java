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
package org.apache.geode.test.dunit.rules;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.awaitility.Awaitility;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.HeadlessGfsh;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.rules.DescribedExternalResource;
import org.junit.runner.Description;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class which eases the connection to the jmxManager {@link ConnectionConfiguration} it allows for
 * the creation of per-test connections with different user/password combinations, or no username
 * and password
 */
public class GfshShellConnectionRule extends DescribedExternalResource {

  private int port = 0;
  private PortType portType = null;
  private HeadlessGfsh gfsh;
  private boolean connected;

  public GfshShellConnectionRule(Locator locator) {
    this(locator.getPort(), PortType.locator);
  }

  public GfshShellConnectionRule(int port, PortType portType) {
    this.portType = portType;
    this.port = port;
    try {
      this.gfsh = new HeadlessGfsh(getClass().getName(), 30, "gfsh_files");
    } catch (Exception e) {
      e.printStackTrace();
    }
    this.connected = false;
  }

  protected void before(Description description) throws Throwable {
    ConnectionConfiguration config = description.getAnnotation(ConnectionConfiguration.class);
    if (config != null) {
      connect(CliStrings.CONNECT__USERNAME, config.user(), CliStrings.CONNECT__PASSWORD,
          config.password());
    } else {
      connect();
    }
  }

  public void connect(String... options) throws Exception {
    CliUtil.isGfshVM = true;
    final CommandStringBuilder connectCommand = new CommandStringBuilder(CliStrings.CONNECT);
    String endpoint;
    if (portType == PortType.locator) {
      // port is the locator port
      endpoint = "localhost[" + port + "]";
      connectCommand.addOption(CliStrings.CONNECT__LOCATOR, endpoint);
    } else if (portType == PortType.http) {
      endpoint = "http://localhost:" + port + "/gemfire/v1";
      connectCommand.addOption(CliStrings.CONNECT__USE_HTTP, Boolean.TRUE.toString());
      connectCommand.addOption(CliStrings.CONNECT__URL, endpoint);
    } else {
      endpoint = "localhost[" + port + "]";
      connectCommand.addOption(CliStrings.CONNECT__JMX_MANAGER, endpoint);
    }

    // add the extra options
    if (options != null) {
      for (int i = 0; i < options.length; i += 2) {
        connectCommand.addOption(options[i], options[i + 1]);
      }
    }

    // when we connect too soon, we would get "Failed to retrieve RMIServer stub:
    // javax.naming.CommunicationException [Root exception is java.rmi.NoSuchObjectException: no
    // such object in table]" Exception.
    // Tried to wait on jmx connector server being ready, but it doesn't work.
    AtomicReference<CommandResult> result = new AtomicReference<>();
    Awaitility.await().atMost(2, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(() -> {
      gfsh.executeCommand(connectCommand.toString());
      result.set((CommandResult) gfsh.getResult());
      System.out.println("connect result: " + result.get().getContent().toString());
      return !gfsh.outputString.contains("no such object in table");
    });

    connected = (result.get().getStatus() == Result.Status.OK);
  }


  /**
   * Override to tear down your specific external resource.
   */
  protected void after(Description description) throws Throwable {
    close();
  }

  public void close() throws Exception {
    if (gfsh != null) {
      gfsh.clear();
      gfsh.executeCommand("disconnect");
      gfsh.executeCommand("exit");
      gfsh.terminate();
      gfsh = null;
    }
    CliUtil.isGfshVM = false;
  }

  public HeadlessGfsh getGfsh() {
    return gfsh;
  }

  public CommandResult executeCommand(String command) throws Exception {
    gfsh.executeCommand(command);
    CommandResult result = (CommandResult) gfsh.getResult();
    System.out.println("command result: " + result.getContent().toString());
    return result;
  }

  public CommandResult executeAndVerifyCommand(String command) throws Exception {
    CommandResult result = executeCommand(command);
    assertThat(result.getStatus()).describedAs(result.getContent().toString())
        .isEqualTo(Result.Status.OK);
    return result;
  }

  public boolean isConnected() {
    return connected;
  }

  public enum PortType {
    locator, jmxManger, http
  }
}
