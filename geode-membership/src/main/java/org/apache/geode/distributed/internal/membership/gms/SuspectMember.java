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
package org.apache.geode.distributed.internal.membership.gms;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;

/** represents a suspicion raised about a member */
public class SuspectMember<ID extends MemberIdentifier> {
  /** the source of suspicion */
  public ID whoSuspected;

  /** suspected member */
  public ID suspectedMember;

  /** the reason */
  public String reason;

  /** create a new SuspectMember */
  public SuspectMember(ID whoSuspected,
      ID suspectedMember, String reason) {
    this.whoSuspected = whoSuspected;
    this.suspectedMember = suspectedMember;
    this.reason = reason;
  }

  @Override
  public String toString() {
    return "{source=" + whoSuspected + "; suspect=" + suspectedMember + "}";
  }

  @Override
  public int hashCode() {
    return suspectedMember.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SuspectMember)) {
      return false;
    }
    return suspectedMember.equals(((SuspectMember<ID>) other).suspectedMember);
  }
}
