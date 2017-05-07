/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package templates.security;

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
    return "templates.security.UserPasswordAuthInit.create";
  }

  public String getAuthenticator() {
    return "templates.security.DummyAuthenticator.create";
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
