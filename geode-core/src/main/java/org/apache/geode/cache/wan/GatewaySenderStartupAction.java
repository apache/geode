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

package org.apache.geode.cache.wan;

/*
 * GatewaySenderStartupAction is persisted in cluster configuration when start, stop, pause or
 * resume gateway-sender command is successfully executed. At startup member will read persisted
 * startup-action parameter and act accordingly.
 */
public enum GatewaySenderStartupAction {
  /*
   * This action ("start") is persisted in cluster configuration after
   * start or resume gateway-sender command is successfully executed.
   * At startup member will start gateway sender. When set then
   * this state has advantage over manual-start parameter.
   */
  START("start"),
  /*
   * This action ("stop") is persisted in cluster configuration after
   * stop gateway-sender command is successfully executed. At startup
   * member will not start gateway-sender. When set then
   * this state has advantage over manual-start parameter.
   */
  STOP("stop"),
  /*
   * This action ("pause") is persisted in cluster configuration after
   * pause gateway-sender command is successfully executed. At startup
   * member will start gateway-sender in paused state. When set then
   * this state has advantage over manual-start parameter.
   */
  PAUSE("pause"),
  /*
   * Used when startup-action parameter is not available in cluster configuration.
   */
  NONE("none");

  private String action;

  GatewaySenderStartupAction(String action) {
    this.action = action;
  }

  public String getAction() {
    return action;
  }

  @Override
  public String toString() {
    return "GatewaySenderStartupAction {" +
        "action='" + action + '\'' +
        '}';
  }
}
