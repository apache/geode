//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.Templates.Cache.Security;
  using GemStone.GemFire.Cache.Generic;

  public class LDAPCredentialGenerator : CredentialGenerator
  {
    private const string UserPrefix = "gemfire";

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
        return "GemStone.GemFire.Templates.Cache.Security.UserPasswordAuthInit.Create";
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
