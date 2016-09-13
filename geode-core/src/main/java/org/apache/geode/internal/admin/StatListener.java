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
   
package org.apache.geode.internal.admin;

/**
 * Interface for those who want to be alerted of a change in value of
 * a statistic
 */
public interface StatListener {
  /**
   * Invoked when the value of a statistic has changed
   *
   * @param value
   *        The new value of the statistic
   * @param time
   *        The time at which the statistic's value change was
   *        detected 
   */
  public void statValueChanged( double value, long time );

  /**
   * Invoked when the value of a statistic has not changed
   *
   * @param time
   *        The time of the latest statistic sample
   */
  public void statValueUnchanged( long time );
}
