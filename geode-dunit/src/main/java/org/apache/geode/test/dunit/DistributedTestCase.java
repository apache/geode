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
package org.apache.geode.test.dunit;

import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * This class is the superclass of all distributed unit tests.
 *
 * @deprecated this class used to be the base for distributed test cases, but due to its complexity
 *             and the complexity of inheritance in test classes, it should not be used.
 *             Please use {@link DistributedRule} and Geode User APIs or
 *             {@link ClusterStartupRule} instead.
 */
@Deprecated
@SuppressWarnings("serial")
public abstract class DistributedTestCase extends JUnit4DistributedTestCase {
}
