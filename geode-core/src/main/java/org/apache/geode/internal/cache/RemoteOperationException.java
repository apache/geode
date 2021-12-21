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

package org.apache.geode.internal.cache;

/**
 * Indicates that the current non-partitioned remote operation failed. Note that even though this
 * exception extends DataLocationException it should not have since that exception has to do with
 * data partitioning. Note: this exception should be in org.apache.geode.internal.cache.tx but for
 * backwards compatibility needs to stay in org.apache.geode.internal.cache.
 *
 * @since GemFire 6.5
 */
public class RemoteOperationException extends DataLocationException {
  private static final long serialVersionUID = -595988965679204903L;

  @SuppressWarnings("unused")
  private final boolean hasHash = false; // kept for serialization backwards compatibility
  @SuppressWarnings("unused")
  private final int keyHash = 0; // kept for serialization backwards compatibility

  public RemoteOperationException(String message, Throwable cause) {
    super(message, cause);
  }

  public RemoteOperationException(String message) {
    super(message);
  }
}
