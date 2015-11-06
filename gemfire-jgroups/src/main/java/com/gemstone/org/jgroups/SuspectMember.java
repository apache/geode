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
package com.gemstone.org.jgroups;

/**
 * GemStoneAddition. This class is used in SUSPECT_WITH_ORIGIN events to
 * hold both the suspected members and the origin of suspicion
 *
 * @author bruce
 */
public class SuspectMember
{
  /** the source of suspicion */
  public Address whoSuspected;
  
  /** suspected member */
  public Address suspectedMember;
  
  /** create a new SuspectMember */
  public SuspectMember(Address whoSuspected, Address suspectedMember) {
    this.whoSuspected = whoSuspected;
    this.suspectedMember = suspectedMember;
  }
  
  @Override // GemStoneAddition
  public String toString() {
    return "{source="+whoSuspected+"; suspect="+suspectedMember+"}";
  }
  
  @Override // GemStoneAddition
  public int hashCode() {
    return this.suspectedMember.hashCode();
  }
  
  @Override // GemStoneAddition
  public boolean equals(Object other) {
    if ( !(other instanceof SuspectMember) ) {
      return false;
    }
    return this.suspectedMember.equals(((SuspectMember)other).suspectedMember);
  }
}
