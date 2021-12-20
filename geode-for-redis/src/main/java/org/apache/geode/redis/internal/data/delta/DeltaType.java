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
 *
 */

package org.apache.geode.redis.internal.data.delta;

public enum DeltaType {
  ADD_BYTE_ARRAYS,
  ADD_BYTE_ARRAY_PAIRS,
  ADD_BYTE_ARRAY_DOUBLE_PAIRS,
  APPEND_BYTE_ARRAY,
  REMOVE_BYTE_ARRAYS,
  REPLACE_BYTE_ARRAYS,
  REPLACE_BYTE_ARRAY_AT_OFFSET,
  REPLACE_BYTE_ARRAY_DOUBLE_PAIRS,
  REPLACE_BYTE_AT_OFFSET,
  SET_BYTE_ARRAY,
  SET_BYTE_ARRAY_AND_TIMESTAMP,
  SET_TIMESTAMP
}
