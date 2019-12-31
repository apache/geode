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
package org.apache.geode.distributed.internal.membership.api;

import java.util.Properties;

public interface Authenticator<ID extends MemberIdentifier> {

  /**
   * Authenticate peer member
   *
   * @param member the member to be authenticated
   * @param credentials the credentials used in authentication
   * @return null if authentication succeed (including no authenticator case), otherwise, return
   *         failure message
   */
  String authenticate(ID member, Properties credentials);

  /**
   * Get credential object for the given GemFire distributed member.
   *
   * @param member the target distributed member
   * @return the credentials
   */
  Properties getCredentials(ID member);
}
