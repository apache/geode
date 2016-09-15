/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.test.junit.categories;

/**
 * JUnit Test Category that specifies a test with very narrow and well defined
 * scope. Any complex dependencies and interactions are stubbed or mocked.
 *
 * <p><ul>A {@code UnitTest} should <bold>not<bold> do any of the following:
 * <li>communicate with a database
 * <li>communicate across the network
 * <li>access the file system
 * <li>prevent the running of other unit tests in parallel
 * <li>require anything special in the environment (such as editing config files or running an external process)
 * </ul>
 *
 * @see <a href="http://www.artima.com/weblogs/viewpost.jsp?thread=126923">A Set of Unit Testing Rules by Michael Feathers</a>
 */
public interface UnitTest {
}
