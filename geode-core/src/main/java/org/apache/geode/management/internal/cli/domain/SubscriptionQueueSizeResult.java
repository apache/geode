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
package org.apache.geode.management.internal.cli.domain;

/***
 *Data class used for sending back subscription-queue-size for a client or a cq
 *
 */
public class SubscriptionQueueSizeResult extends MemberResult {

  private static final long serialVersionUID = 1L;
  private long subscriptionQueueSize;

  public SubscriptionQueueSizeResult(String memberNameOrId) {
    super(memberNameOrId);
  }

  public long getSubscriptionQueueSize() {
    return subscriptionQueueSize;
  }

  public void setSubscriptionQueueSize(long queueSize) {
    this.subscriptionQueueSize = queueSize;
    super.isSuccessful = true;
    super.opPossible = true;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(super.toString());
    sb.append("\nsubscription-queue-size : ");
    sb.append(this.subscriptionQueueSize);
    return sb.toString();
  }

}
