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
 * marker interface for the final result of a {@link ClusterManagementOperation}
 */
@Experimental
public interface OperationResult extends JsonSerializable, Serializable {
}
