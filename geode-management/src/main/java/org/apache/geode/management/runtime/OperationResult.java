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

package org.apache.geode.management.runtime;

import java.io.Serializable;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.api.JsonSerializable;

/**
 * Describes the result of a starting or checking the status of a
 * {@link ClusterManagementOperation}.
 */
@Experimental
public interface OperationResult extends JsonSerializable, Serializable {
  /**
   * Returns true if the operation was successful; false if it failed.
   */
  boolean getSuccess();

  /**
   * Returns details about what caused the operation to fail or succeed.
   * If the operation failed then a description of what was wrong will
   * be in this status message. If the operation was successful then
   * the status message may contain additional information.
   */
  String getStatusMessage();
}
