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
package org.apache.geode.test.junit.support;

import org.junit.runner.Description;
import org.apache.geode.test.junit.IgnoreCondition;

/**
 * The DefaultIgnoreCondition class...
 *
 * @see org.junit.runner.Description
 * @see org.apache.geode.test.junit.ConditionalIgnore
 * @see org.apache.geode.test.junit.IgnoreCondition
 */
@SuppressWarnings("unused")
public class DefaultIgnoreCondition implements IgnoreCondition {

  public static final boolean DEFAULT_IGNORE = false;

  public static final DefaultIgnoreCondition DO_NOT_IGNORE = new DefaultIgnoreCondition(false);
  public static final DefaultIgnoreCondition IGNORE = new DefaultIgnoreCondition(true);

  private final boolean ignore;

  public DefaultIgnoreCondition() {
    this(DEFAULT_IGNORE);
  }

  public DefaultIgnoreCondition(final boolean ignore) {
    this.ignore = ignore;
  }

  public boolean isIgnore() {
    return this.ignore;
  }

  @Override
  public boolean evaluate(final Description testCaseDescription) {
    return isIgnore();
  }

}
