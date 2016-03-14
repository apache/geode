/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package security;

import templates.security.DummyAuthenticator;
import templates.security.UserPasswordAuthInit;

import java.security.Principal;
import java.util.Properties;

public class DummyCredentialGenerator extends CredentialGenerator {

  public DummyCredentialGenerator() {
  }

  protected Properties initialize() throws IllegalArgumentException {
    return null;
  }

  public ClassCode classCode() {
    return ClassCode.DUMMY;
  }

  public String getAuthInit() {
    return templates.security.UserPasswordAuthInit.class.getName() + ".create";
  }

  public String getAuthenticator() {
    return templates.security.DummyAuthenticator.class.getName() + ".create";
  }

  public Properties getValidCredentials(int index) {

    String[] validGroups = new String[] { "admin", "user", "reader", "writer" };
    String[] admins = new String[] { "root", "admin", "administrator" };

    Properties props = new Properties();
    int groupNum = (index % validGroups.length);
    String userName;
    if (groupNum == 0) {
      userName = admins[index % admins.length];
    }
    else {
      userName = validGroups[groupNum] + (index / validGroups.length);
    }
    props.setProperty(UserPasswordAuthInit.USER_NAME, userName);
    props.setProperty(UserPasswordAuthInit.PASSWORD, userName);
    return props;
  }

  public Properties getValidCredentials(Principal principal) {

    String userName = principal.getName();
    if (DummyAuthenticator.testValidName(userName)) {
      Properties props = new Properties();
      props.setProperty(UserPasswordAuthInit.USER_NAME, userName);
      props.setProperty(UserPasswordAuthInit.PASSWORD, userName);
      return props;
    }
    else {
      throw new IllegalArgumentException("Dummy: [" + userName
          + "] is not a valid user");
    }
  }

  public Properties getInvalidCredentials(int index) {

    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, "invalid" + index);
    props.setProperty(UserPasswordAuthInit.PASSWORD, "none");
    return props;
  }

}
