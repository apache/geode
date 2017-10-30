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

package org.apache.geode.internal.protocol.security;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.security.exception.IncompatibleAuthenticationMechanismsException;
import org.apache.geode.security.AuthenticationFailedException;

public class InvalidConfigAuthenticator implements Authenticator<Object, Object> {
  private static final Logger logger = LogService.getLogger(InvalidConfigAuthenticator.class);

  @Override
  public Object authenticate(Object object) throws AuthenticationFailedException {
    logger.warn(
        "Attempting to authenticate incoming protobuf message using legacy security implementation. This is not supported. Failing authentication.");
    throw new IncompatibleAuthenticationMechanismsException(
        "Attempting to authenticate incoming protobuf message using legacy security implementation. This is not supported. Failing authentication.");
  }
}
