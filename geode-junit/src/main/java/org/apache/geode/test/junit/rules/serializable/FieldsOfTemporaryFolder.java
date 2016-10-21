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
package org.apache.geode.test.junit.rules.serializable;

/**
 * Names of member fields in {@link org.junit.rules.TemporaryFolder}.
 */
interface FieldsOfTemporaryFolder {
  static final String FIELD_PARENT_FOLDER = "parentFolder"; // java.io.File
  static final String FIELD_ASSURE_DELETION = "assureDeletion"; // boolean (since JUnit 4.13)
  static final String FIELD_FOLDER = "folder"; // java.io.File
}
