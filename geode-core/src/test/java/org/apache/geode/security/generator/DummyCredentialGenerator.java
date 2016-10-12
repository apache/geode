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
package org.apache.geode.security.generator;

import java.security.Principal;
import java.util.Properties;

import org.apache.geode.security.templates.DummyAuthenticator;
import org.apache.geode.security.templates.UserPasswordAuthInit;

public class DummyCredentialGenerator extends CredentialGenerator {

  @Override
  protected Properties initialize() throws IllegalArgumentException {
    return null;
  }

  @Override
  public ClassCode classCode() {
    return ClassCode.DUMMY;
  }

  @Override
  public String getAuthInit() {
    return UserPasswordAuthInit.class.getName() + ".create";
  }

  @Override
  public String getAuthenticator() {
    return DummyAuthenticator.class.getName() + ".create";
  }

  @Override
  public Properties getValidCredentials(final int index) {
    final String[] validGroups = new String[] {"admin", "user", "reader", "writer"};
    final String[] admins = new String[] {"root", "admin", "administrator"};

    final Properties props = new Properties();
    final int groupNum = index % validGroups.length;

    String userName;
    if (groupNum == 0) {
      userName = admins[index % admins.length];
    } else {
      userName = validGroups[groupNum] + (index / validGroups.length);
    }

    props.setProperty(UserPasswordAuthInit.USER_NAME, userName);
    props.setProperty(UserPasswordAuthInit.PASSWORD, userName);
    return props;
  }

  @Override
  public Properties getValidCredentials(final Principal principal) {
    final String userName = principal.getName();

    if (DummyAuthenticator.checkValidName(userName)) {
      Properties props = new Properties();
      props.setProperty(UserPasswordAuthInit.USER_NAME, userName);
      props.setProperty(UserPasswordAuthInit.PASSWORD, userName);
      return props;

    } else {
      throw new IllegalArgumentException("Dummy: [" + userName + "] is not a valid user");
    }
  }

  @Override
  public Properties getInvalidCredentials(int index) {
    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, "invalid" + index);
    props.setProperty(UserPasswordAuthInit.PASSWORD, "none");
    return props;
  }
}
