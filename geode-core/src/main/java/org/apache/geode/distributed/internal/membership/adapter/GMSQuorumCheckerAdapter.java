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
package org.apache.geode.distributed.internal.membership.adapter;

import org.apache.geode.distributed.internal.membership.QuorumChecker;
import org.apache.geode.distributed.internal.membership.gms.messenger.GMSQuorumChecker;
import org.apache.geode.distributed.internal.membership.gms.messenger.MembershipInformation;

public class GMSQuorumCheckerAdapter implements QuorumChecker {
  private final GMSQuorumChecker quorumChecker;

  public GMSQuorumCheckerAdapter(GMSQuorumChecker delegate) {

    quorumChecker = delegate;
  }

  @Override
  public boolean checkForQuorum(long timeoutMS) throws InterruptedException {
    return quorumChecker.checkForQuorum(timeoutMS);
  }

  @Override
  public void suspend() {
    quorumChecker.suspend();
  }

  @Override
  public void resume() {
    quorumChecker.resume();
  }

  @Override
  public void close() {
    quorumChecker.close();
  }

  @Override
  public MembershipInformation getMembershipInfo() {
    return quorumChecker.getMembershipInfo();
  }
}
