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
package com.gemstone.gemfire.cache;

/**
 * An <code>UnsupportedVersionException</code> indicates an unsupported version.
 *
 * @since GemFire 5.7
 */
public class UnsupportedVersionException extends VersionException {
private static final long serialVersionUID = 1152280300663399399L;

  /**
   * Constructs a new <code>UnsupportedVersionException</code>.
   * 
   * @param versionOrdinal The ordinal of the requested <code>Version</code>
   */
  public UnsupportedVersionException(short versionOrdinal) {
    super(String.valueOf(versionOrdinal));
  }
  
  /**
   * Constructs a new <code>UnsupportedVersionException</code>.
   * 
   * @param message The exception message
   */
  public UnsupportedVersionException(String message) {
    super(message);
  }
}
