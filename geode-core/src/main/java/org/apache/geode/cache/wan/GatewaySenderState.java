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
 * GatewaySenderState doesn't reflect current state, but the desired state of GatewaySender
 * at the startup of the member. This state is only persisted within cluster configuration when
 * start, stop, pause or resume gateway-sender command is successfully executed. When member starts
 * again, it will read the state from cluster configuration and bring gateway-sender
 * that desired state.
 */
public enum GatewaySenderState {
  /*
   * This state ("running") is persisted in cluster configuration after
   * start or resume gateway-sender command is successfully executed.
   * At startup member will start gateway sender. When set then
   * this state has advantage over manual-start parameter.
   */
  RUNNING("running"),
  /*
   * This state ("stopped") is persisted in cluster configuration after
   * stop gateway-sender command is successfully executed. At startup
   * member will not start gateway-sender. When set then
   * this state has advantage over manual-start parameter.
   */
  STOPPED("stopped"),
  /*
   * This state ("paused") is persisted in cluster configuration after
   * pause gateway-sender command is successfully executed. At startup
   * member will start gateway-sender in paused state. When set then
   * this state has advantage over manual-start parameter.
   */
  PAUSED("paused");

  private String state;

  GatewaySenderState(String state) {
    this.state = state;
  }

  public String getState() {
    return state;
  }

  @Override
  public String toString() {
    return "GatewaySenderState{" +
        "state='" + state + '\'' +
        '}';
  }
}
