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
 * Thrown when a problem is encountered while parsing a <A
 * href="package-summary.html#declarative">declarative caching XML
 * file</A>.  Examples of such problems are a malformed XML file or
 * the inability to load a {@link Declarable} class.
 *
 * @see CacheFactory#create
 *
 *
 * @since GemFire 3.0
 */
public class CacheXmlException extends CacheRuntimeException {
private static final long serialVersionUID = -4343870964883131754L;

  /**
   * Creates a new <code>CacheXmlException</code> with the given
   * description and cause.
   */
  public CacheXmlException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new <code>CacheXmlException</code> with the given
   * description.
   */
  public CacheXmlException(String message) {
    super(message);
  }

}
