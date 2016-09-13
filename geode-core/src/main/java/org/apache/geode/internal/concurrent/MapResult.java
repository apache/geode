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

package org.apache.geode.internal.concurrent;

/**
 * Any additional result state needed to be passed to {@link MapCallback} which
 * returns values by reference.
 * 
 * @since GemFire Helios
 */
public interface MapResult {

  /**
   * Set whether the result of {@link MapCallback#newValue} created a new value
   * or not. If not, then the result value of newValue is not inserted into the
   * map though it is still returned by the create methods. Default for
   * MapResult is assumed to be true if this method was not invoked by
   * {@link MapCallback} explicitly.
   */
  public void setNewValueCreated(boolean created);

  /**
   * Result set by {@link #setNewValueCreated(boolean)}. Default is required to
   * be true.
   */
  public boolean isNewValueCreated();
}
