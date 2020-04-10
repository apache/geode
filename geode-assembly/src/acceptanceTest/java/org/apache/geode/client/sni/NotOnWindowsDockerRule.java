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
package org.apache.geode.client.sni;

import java.util.function.Supplier;

import com.palantir.docker.compose.DockerComposeRule;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Assume;
import org.junit.rules.ExternalResource;

/**
 * A rule that wraps {@link DockerComposeRule} in such a way that docker
 * tests will be ignored on the windows platform.
 *
 * Provide the code to build a DockerComposeRule in the constructor to this rule,
 * and access the rule later in your test with the {@link #get()} method.
 */
public class NotOnWindowsDockerRule extends ExternalResource {

  private final Supplier<DockerComposeRule> dockerRuleSupplier;
  private DockerComposeRule docker;

  public NotOnWindowsDockerRule(Supplier<DockerComposeRule> dockerRuleSupplier) {
    this.dockerRuleSupplier = dockerRuleSupplier;
  }

  @Override
  protected void before() throws Throwable {
    Assume.assumeFalse(SystemUtils.IS_OS_WINDOWS);
    this.docker = dockerRuleSupplier.get();
    docker.before();
  }

  @Override
  protected void after() {
    if (docker != null) {
      docker.after();
    }
  }

  public DockerComposeRule get() {
    return docker;
  }
}
