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
package org.apache.geode.cache.persistence;

import org.apache.geode.GemFireException;

/**
 * Thrown when a member tries to revoke a persistent ID, but the member
 * with that persistent ID is currently running. You can only revoke
 * members which is not running.
 * 
 * @since GemFire 6.6.2
 */
public class RevokeFailedException extends GemFireException {

  private static final long serialVersionUID = -2629287782021455875L;

  public RevokeFailedException() {
    super();
  }

  public RevokeFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public RevokeFailedException(String message) {
    super(message);
  }

  public RevokeFailedException(Throwable cause) {
    super(cause);
  }

  

}
