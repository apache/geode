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
package javaobject;

import java.util.Properties;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;

/**
 * An {@link AuthInitialize} implementation that obtains the user name and
 * password as the credentials from the given set of properties.
 * 
 * To use this class the <c>security-client-auth-init</c> property should be
 * set to the fully qualified name the static <code>create</code> function
 * viz. <code>templates.security.UserPasswordAuthInit.create</code>
 * 
 * 
 * @since 5.5
 */
public class UserPasswordAuthInit implements AuthInitialize {

  public static final String USER_NAME = "security-username";

  public static final String PASSWORD = "security-password";

  protected LogWriter securitylog;

  protected LogWriter systemlog;

  public static AuthInitialize create() {
    return new UserPasswordAuthInit();
  }

  public void init(LogWriter systemLogger, LogWriter securityLogger)
      throws AuthenticationFailedException {
    this.systemlog = systemLogger;
    this.securitylog = securityLogger;
  }

  public UserPasswordAuthInit() {
  }

  public Properties getCredentials(Properties props, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {

    Properties newProps = new Properties();
    String userName = props.getProperty(USER_NAME);
    if (userName == null) {
      throw new AuthenticationFailedException(
          "UserPasswordAuthInit: user name property [" + USER_NAME
              + "] not set.");
    }
    newProps.setProperty(USER_NAME, userName);
    String passwd = props.getProperty(PASSWORD);
    // If password is not provided then use empty string as the password.
    if (passwd == null) {
      passwd = "";
    }
    newProps.setProperty(PASSWORD, passwd);
    return newProps;
  }

  public void close() {
  }

}
