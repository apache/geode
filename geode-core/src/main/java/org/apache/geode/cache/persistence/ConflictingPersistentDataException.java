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
import org.apache.geode.admin.AdminDistributedSystem;

/**
 * Thrown when a member with persistence is recovering, and it discovers that
 * the data it has on disk was never part of the same distributed system as the
 * members that are currently online.
 * 
 * This exception can occur when two members both have persistent files for the
 * same region, but they were online at different times, so the contents of their
 * persistent files are completely different. In that case, gemfire throws this
 * exception rather than discarding one of the sets of persistent files.
 * 
 * @since GemFire 6.5
 */
public class ConflictingPersistentDataException extends GemFireException {

  private static final long serialVersionUID = -2629287782021455875L;

  public ConflictingPersistentDataException() {
    super();
  }

  public ConflictingPersistentDataException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConflictingPersistentDataException(String message) {
    super(message);
  }

  public ConflictingPersistentDataException(Throwable cause) {
    super(cause);
  }

  

}
