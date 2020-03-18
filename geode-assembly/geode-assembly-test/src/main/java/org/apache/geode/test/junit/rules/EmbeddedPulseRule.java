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
package org.apache.geode.test.junit.rules;

import org.junit.rules.ExternalResource;

import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * This is used to test embedded pulse. If your test needs to check pulse's repository object for
 * assertions, use this rules to properly initialize and cleanup the repository
 */
public class EmbeddedPulseRule extends ExternalResource {

  private Repository repository;

  @Override
  protected void before() throws Throwable {
    repository = new Repository();
    cleanup();
    repository.setHost("localhost");
  }

  @Override
  protected void after() {
    cleanup();
  }

  public Repository getRepository() {
    return repository;
  }

  public void useJmxManager(String jmxHost, int jmxPort) {
    repository.setJmxUseLocator(false);
    repository.setHost(jmxHost + "");
    repository.setPort(jmxPort + "");
  }

  public void useJmxPort(int jmxPort) {
    repository.setJmxUseLocator(false);
    repository.setPort(jmxPort + "");
  }

  public void useLocatorPort(int locatorPort) {
    repository.setJmxUseLocator(true);
    repository.setPort(locatorPort + "");
  }

  public void setLocatorSSL(boolean locatorSSL) {
    repository.setUseSSLLocator(locatorSSL);
  }

  public void setJmxSSL(boolean jmxSSL) {
    repository.setUseSSLManager(jmxSSL);
  }

  private void cleanup() {
    if (repository != null) {
      repository.setPort("-1");
      repository.setHost("");
      repository.setJmxUseLocator(false);
      repository.setUseSSLManager(false);
      repository.setUseSSLManager(false);
      repository.removeAllClusters();
    }
  }
}
