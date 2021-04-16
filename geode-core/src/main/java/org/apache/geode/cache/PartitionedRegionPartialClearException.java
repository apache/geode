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
package org.apache.geode.cache;

/**
 * Indicates a failure to perform a distributed clear operation on a Partitioned Region
 * after multiple attempts. The clear may not have been successfully applied on some of
 * the members hosting the region.
 */
public class PartitionedRegionPartialClearException extends CacheRuntimeException {

  private static final long serialVersionUID = -3420558263697703892L;

  public PartitionedRegionPartialClearException() {
    // nothing
  }

  public PartitionedRegionPartialClearException(String message) {
    super(message);
  }

  public PartitionedRegionPartialClearException(String message, Throwable cause) {
    super(message, cause);
  }

  public PartitionedRegionPartialClearException(Throwable cause) {
    super(cause);
  }
}
