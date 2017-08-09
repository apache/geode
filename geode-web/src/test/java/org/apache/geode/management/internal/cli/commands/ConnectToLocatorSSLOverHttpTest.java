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

package org.apache.geode.management.internal.cli.commands;

import javax.net.ssl.HttpsURLConnection;

import org.apache.geode.management.ConnectToLocatorSSLDUnitTest;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;

public class ConnectToLocatorSSLOverHttpTest extends ConnectToLocatorSSLDUnitTest {

  protected void connect() throws Exception {
    final int httpPort = locator.getHttpPort();
    final String securityPropsFilePath = securityPropsFile.getCanonicalPath();
    Host.getHost(0).getVM(1).invoke(() -> {
      // Our SSL certificate used for tests does not match the hostname "localhost"
      HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);

      GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();
      gfshConnector.connectAndVerify(httpPort, GfshShellConnectionRule.PortType.http,
          CliStrings.CONNECT__SECURITY_PROPERTIES, securityPropsFilePath,
          CliStrings.CONNECT__USE_SSL, "true");
      gfshConnector.executeAndVerifyCommand("list members");
      gfshConnector.close();
    });
  }
}

