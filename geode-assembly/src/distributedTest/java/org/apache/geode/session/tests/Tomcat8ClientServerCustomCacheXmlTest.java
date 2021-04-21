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

// package org.apache.geode.session.tests;
//
//
// public class Tomcat8ClientServerCustomCacheXmlTest extends Tomcat8ClientServerTest {
//
// @Override
// public void customizeContainers() throws Exception {
// for (int i = 0; i < manager.numContainers(); i++) {
// ServerContainer container = manager.getContainer(i);
//
// HashMap<String, String> regionAttributes = new HashMap<>();
// regionAttributes.put("refid", "PROXY");
// regionAttributes.put("name", "gemfire_modules_sessions");
//
// ContainerInstall.editXMLFile(
// container.cacheXMLFile.getAbsolutePath(),
// null,
// "region",
// "client-cache",
// regionAttributes);
// }
// }
//
// @Override
// public void afterStartServers() throws Exception {
// gfsh.connect(locatorVM);
// gfsh.executeAndAssertThat("create region --name=gemfire_modules_sessions --type=PARTITION")
// .statusIsSuccess();
// }
//
// }
