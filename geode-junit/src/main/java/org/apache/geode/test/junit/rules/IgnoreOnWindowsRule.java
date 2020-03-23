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

import java.io.Serializable;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Assume;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;

/**
 * A rule that causes a test to be ignored if it is run on windows.
 *
 * This is best used a as a ClassRule to make sure it runs first. If you have
 * other class rules, us a {@link RuleChain} to make sure this runs first.
 */
public class IgnoreOnWindowsRule extends ExternalResource implements Serializable {
  @Override
  protected void before() throws Throwable {
    Assume.assumeFalse(SystemUtils.IS_OS_WINDOWS);
  }
}
