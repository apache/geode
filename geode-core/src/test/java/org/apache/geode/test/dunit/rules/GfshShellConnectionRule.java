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

import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.lang.StringUtils;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.HeadlessGfsh;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.rules.DescribedExternalResource;
import org.json.JSONArray;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;

import java.util.function.Supplier;

/**
 * Class which eases the connection to the locator/jmxManager in Gfsh shell and execute gfsh
 * commands.
 *
 * if used with {@link ConnectionConfiguration}, you will need to specify a port number when
 * constructing this rule. Then this rule will do auto connect for you before running your test.
 *
 * otherwise, you can call connect with the specific port number yourself in your test. This rules
 * handles closing your connection and gfsh instance.
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

  private Supplier<Integer> portSupplier;
  private PortType portType = PortType.jmxManger;
  private HeadlessGfsh gfsh = null;
  private boolean connected = false;
  private IgnoredException ignoredException;
  private TemporaryFolder temporaryFolder = new TemporaryFolder();

  public GfshShellConnectionRule() {
    try {
      temporaryFolder.create();
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public GfshShellConnectionRule(Supplier<Integer> portSupplier, PortType portType) {
    this();
    this.portType = portType;
    this.portSupplier = portSupplier;
  }

  @Override
  protected void before(Description description) throws Throwable {
    this.gfsh = new HeadlessGfsh(getClass().getName(), 30,
        temporaryFolder.newFolder("gfsh_files").getAbsolutePath());
    ignoredException =
        addIgnoredException("java.rmi.NoSuchObjectException: no such object in table");

    // do not auto connect if no port initialized
    if (portSupplier == null) {
      return;
    }

    // do not auto connect if it's not used with ConnectionConfiguration
    ConnectionConfiguration config = description.getAnnotation(ConnectionConfiguration.class);
    if (config == null) {
      return;
    }

    connect(portSupplier.get(), portType, CliStrings.CONNECT__USERNAME, config.user(),
        CliStrings.CONNECT__PASSWORD, config.password());

  }

  public void connect(Member locator, String... options) throws Exception {
    connect(locator.getPort(), PortType.locator, options);
  }

  public void connectAndVerify(Member locator, String... options) throws Exception {
    connect(locator.getPort(), PortType.locator, options);
    assertThat(this.connected).isTrue();
  }

  public void connectAndVerify(int port, PortType type, String... options) throws Exception {
    connect(port, type, options);
    assertThat(this.connected).isTrue();
  }

  public void secureConnect(int port, PortType type, String username, String password)
      throws Exception {
    connect(port, type, CliStrings.CONNECT__USERNAME, username, CliStrings.CONNECT__PASSWORD,
        password);
  }

  public void secureConnectAndVerify(int port, PortType type, String username, String password)
      throws Exception {
    connect(port, type, CliStrings.CONNECT__USERNAME, username, CliStrings.CONNECT__PASSWORD,
        password);
    assertThat(this.connected).isTrue();
  }

  public void connect(int port, PortType type, String... options) throws Exception {
    if (gfsh == null) {
      this.gfsh = new HeadlessGfsh(getClass().getName(), 30,
          temporaryFolder.newFolder("gfsh_files").getAbsolutePath());
    }
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
    // can not use Awaitility here because it starts another thead, but the Gfsh instance is in a
    // threadLocal variable, See Gfsh.getExistingInstance()
    CommandResult result = null;
    for (int i = 0; i < 50; i++) {
      result = executeCommand(connectCommand.toString());
      if (!gfsh.outputString.contains("no such object in table")) {
        break;
      }
      Thread.currentThread().sleep(2000);
    }
    connected = (result.getStatus() == Result.Status.OK);
  }

  @Override
  protected void after(Description description) throws Throwable {
    close();

    if (ignoredException != null) {
      ignoredException.remove();
    }
  }

  public void disconnect() throws Exception {
    gfsh.clear();
    executeCommand("disconnect");
    connected = false;
  }

  public void close() throws Exception {
    temporaryFolder.delete();
    if (connected) {
      disconnect();
    }
    gfsh.executeCommand("exit");
    gfsh.terminate();
    gfsh = null;
  }

  public HeadlessGfsh getGfsh() {
    return gfsh;
  }

  public CommandResult executeCommand(String command) throws Exception {
    gfsh.executeCommand(command);
    CommandResult result = (CommandResult) gfsh.getResult();
    if (StringUtils.isBlank(gfsh.outputString) && result != null && result.getContent() != null) {
      if (result.getStatus() == Result.Status.ERROR) {
        gfsh.outputString = result.toString();
      } else {
        // print out the message body as the command result
        JSONArray messages = ((JSONArray) result.getContent().get("message"));
        if (messages != null) {
          for (int i = 0; i < messages.length(); i++) {
            gfsh.outputString += messages.getString(i) + "\n";
          }
        }
      }
    }
    System.out.println("Command result for <" + command + ">: \n" + gfsh.outputString);
    return result;
  }

  public String getGfshOutput() {
    return gfsh.outputString;
  }


  public CommandResult executeAndVerifyCommand(String command) throws Exception {
    CommandResult result = executeCommand(command);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
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
