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

import org.awaitility.Awaitility;
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
 *
 * you can use this as Rule
 * 
 * @Rule GfshShellConnectionRule rule = new GfshSheelConnectionRule(); then after you connect to a
 *       locator, you don't have to call disconnect() or close() at all, since the rule's after
 *       takes care of it for you.
 *
 *       Or as a ClassRule
 * @ClassRule GfshShellConnectionRule rule = new GfshSheelConnectionRule(); When using as a
 *            ClassRule, if you call connect in a test, you will need to call disconnect after the
 *            test as well. See NetstatDUnitTest for example.
 *
 */
public class GfshShellConnectionRule extends DescribedExternalResource {

  private int port = -1;
  private PortType portType = PortType.jmxManger;
  private HeadlessGfsh gfsh = null;
  private boolean connected = false;

  public GfshShellConnectionRule() {}

  public GfshShellConnectionRule(int port, PortType portType) {
    this.portType = portType;
    this.port = port;
  }

  @Override
  protected void before(Description description) throws Throwable {
    this.gfsh = new HeadlessGfsh(getClass().getName(), 30, "gfsh_files");
    // do not connect if no port initialized
    if (port < 0) {
      return;
    }

    ConnectionConfiguration config = description.getAnnotation(ConnectionConfiguration.class);
    if (config == null) {
      connect(port, portType);
      return;
    }

    connect(port, portType, CliStrings.CONNECT__USERNAME, config.user(),
        CliStrings.CONNECT__PASSWORD, config.password());

  }

  public void connect(Locator locator, String... options) throws Exception {
    connect(locator.getPort(), PortType.locator, options);
  }

  public void connect(int port, PortType type, String... options) throws Exception {
    CliUtil.isGfshVM = true;
    final CommandStringBuilder connectCommand = new CommandStringBuilder(CliStrings.CONNECT);
    String endpoint;
    if (type == PortType.locator) {
      // port is the locator port
      endpoint = "localhost[" + port + "]";
      connectCommand.addOption(CliStrings.CONNECT__LOCATOR, endpoint);
    } else if (type == PortType.http) {
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
      CommandResult cResult = executeCommand(connectCommand.toString());
      result.set(cResult);
      return !gfsh.outputString.contains("no such object in table");
    });

    connected = (result.get().getStatus() == Result.Status.OK);
  }

  @Override
  protected void after(Description description) throws Throwable {
    if (connected) {
      disconnect();
    }
    close();
  }

  public void disconnect() throws Exception {
    gfsh.clear();
    executeCommand("disconnect");
    connected = false;
  }

  public void close() throws Exception {
    gfsh.executeCommand("exit");
    gfsh.terminate();
    gfsh = null;
    CliUtil.isGfshVM = false;
  }

  public HeadlessGfsh getGfsh() {
    return gfsh;
  }

  public CommandResult executeCommand(String command) throws Exception {
    gfsh.executeCommand(command);
    CommandResult result = (CommandResult) gfsh.getResult();
    System.out.println("Command Result: \n" + gfsh.outputString);
    return result;
  }


  public CommandResult executeAndVerifyCommand(String command) throws Exception {
    CommandResult result = executeCommand(command);
    assertThat(result.getStatus()).describedAs(result.getContent().toString())
        .isEqualTo(Result.Status.OK);
    return result;
  }

  public String execute(String command) throws Exception {
    executeCommand(command);
    return gfsh.outputString;
  }

  public boolean isConnected() {
    return connected;
  }

  public enum PortType {
    locator, jmxManger, http
  }
}
