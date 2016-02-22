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
/**
 * 
 */
package com.gemstone.gemfire.cache.hdfs;

import com.gemstone.gemfire.GemFireIOException;

/**
 * Thrown when an error has occurred while attempted to use
 * the HDFS file system. This error may indicate a failure of the HDFS
 * system.
 * 
 * 
 * @since 7.5
 * 
 */
public class HDFSIOException extends GemFireIOException {

  /**
   * @param message
   * @param cause
   */
  public HDFSIOException(String message, Throwable cause) {
    super(message, cause);
    // TODO Auto-generated constructor stub
  }

  /**
   * @param message
   */
  public HDFSIOException(String message) {
    super(message);
    // TODO Auto-generated constructor stub
  }

}
