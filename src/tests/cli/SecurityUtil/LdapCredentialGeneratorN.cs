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

using System;

namespace Apache.Geode.Client.Tests
{
  using Apache.Geode.Templates.Cache.Security;
  using Apache.Geode.Client;

  public class LDAPCredentialGenerator : CredentialGenerator
  {
    private const string UserPrefix = "geode";

    public LDAPCredentialGenerator()
    {
    }

    protected override Properties<string, string> Init()
    {
      Properties<string, string> extraProps = new Properties<string, string>();
      string ldapServer = Environment.GetEnvironmentVariable("LDAP_SERVER");
      string ldapBaseDN = Environment.GetEnvironmentVariable("LDAP_BASEDN");
      string ldapUseSSL = Environment.GetEnvironmentVariable("LDAP_USESSL");
      if (ldapServer == null || ldapServer.Length == 0)
      {
        ldapServer = "ldap";
      }
      if (ldapBaseDN == null || ldapBaseDN.Length == 0)
      {
        ldapBaseDN = "ou=ldapTesting,dc=ldap,dc=gemstone,dc=com";
      }
      if (ldapUseSSL == null || ldapUseSSL.Length == 0)
      {
        ldapUseSSL = "false";
      }
      extraProps.Insert("security-ldap-server", ldapServer);
      extraProps.Insert("security-ldap-basedn", ldapBaseDN);
      extraProps.Insert("security-ldap-usessl", ldapUseSSL);
      return extraProps;
    }

    public override ClassCode GetClassCode()
    {
      return ClassCode.LDAP;
    }

    public override string AuthInit
    {
      get
      {
        return "Apache.Geode.Templates.Cache.Security.UserPasswordAuthInit.Create";
      }
    }

    public override string Authenticator
    {
      get
      {
        return "templates.security.LdapUserAuthenticator.create";
      }
    }

    public override Properties<string, string> GetValidCredentials(int index)
    {
      Properties<string, string> props = new Properties<string, string>();
      props.Insert(UserPasswordAuthInit.UserNameProp, UserPrefix
        + ((index % 10) + 1));
      props.Insert(UserPasswordAuthInit.PasswordProp, UserPrefix
        + ((index % 10) + 1));
      return props;
    }

    public override Properties<string, string> GetValidCredentials(Properties<string, string> principal)
    {
      Properties<string, string> props = null;
      string userName = (string)principal.Find(UserPasswordAuthInit.UserNameProp);
      if (userName != null && userName.StartsWith(UserPrefix))
      {
        bool isValid;
        try
        {
          int suffix = Int32.Parse(userName.Substring(UserPrefix.Length));
          isValid = (suffix >= 1 && suffix <= 10);
        }
        catch (Exception)
        {
          isValid = false;
        }
        if (isValid)
        {
          props = new Properties<string, string>();
          props.Insert(UserPasswordAuthInit.UserNameProp, userName);
          props.Insert(UserPasswordAuthInit.PasswordProp, userName);
        }
      }
      if (props == null)
      {
        throw new IllegalArgumentException("LDAP: [" + userName +
          "] not a valid user");
      }
      return props;
    }

    public override Properties<string, string> GetInvalidCredentials(int index)
    {
      Properties<string, string> props = new Properties<string, string>();
      props.Insert(UserPasswordAuthInit.UserNameProp, "invalid" + index);
      props.Insert(UserPasswordAuthInit.PasswordProp, "none");
      return props;
    }
  }
}
