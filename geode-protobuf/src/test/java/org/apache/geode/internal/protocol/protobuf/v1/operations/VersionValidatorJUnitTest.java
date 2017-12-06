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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class VersionValidatorJUnitTest {
  private static final int MAJOR_VERSION = 3;
  private static final int MINOR_VERSION = 3;
  private static final VersionValidator validator =
      new VersionValidator(MAJOR_VERSION, MINOR_VERSION);

  @Test
  public void testInvalidVersions() throws Exception {
    assertFalse(
        validator.isValid(MAJOR_VERSION, ConnectionAPI.MinorVersions.INVALID_MINOR_VERSION_VALUE));
    assertFalse(
        validator.isValid(ConnectionAPI.MajorVersions.INVALID_MAJOR_VERSION_VALUE, MINOR_VERSION));
    assertFalse(validator.isValid(ConnectionAPI.MajorVersions.INVALID_MAJOR_VERSION_VALUE,
        ConnectionAPI.MinorVersions.INVALID_MINOR_VERSION_VALUE));
  }

  @Test
  public void testCurrentVersions() throws Exception {
    assertTrue(validator.isValid(MAJOR_VERSION, MINOR_VERSION));
  }

  @Test
  public void testPreviousMajorVersions() throws Exception {
    assertFalse(validator.isValid(MAJOR_VERSION - 1, MINOR_VERSION));
    assertFalse(validator.isValid(MAJOR_VERSION - 2, MINOR_VERSION));
  }

  @Test
  public void testPreviousMinorVersions() throws Exception {
    assertTrue(validator.isValid(MAJOR_VERSION, MINOR_VERSION - 1));
    assertTrue(validator.isValid(MAJOR_VERSION, MINOR_VERSION - 2));
  }
}
