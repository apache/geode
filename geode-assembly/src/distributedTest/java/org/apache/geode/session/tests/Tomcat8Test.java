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
package org.apache.geode.session.tests;

import static org.apache.geode.session.tests.ContainerInstall.ConnectionType.PEER_TO_PEER;
import static org.apache.geode.session.tests.TomcatInstall.TomcatVersion.TOMCAT8_RECENT;

import java.util.function.IntSupplier;

public class Tomcat8Test extends CargoTestBase {
  @Override
  public ContainerInstall getInstall(IntSupplier portSupplier) throws Exception {
    return new TomcatInstall(getClass().getSimpleName(), TOMCAT8_RECENT, PEER_TO_PEER, portSupplier,
        TomcatInstall.CommitValve.DEFAULT);
  }
}
