//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.Tests
{
  using GemStone.GemFire.Templates.Cache.Security;

  public class LDAPCredentialGenerator : CredentialGenerator
  {
    private const string UserPrefix = "gemfire";

    public LDAPCredentialGenerator()
    {
    }

    protected override Properties Init()
    {
      Properties extraProps = new Properties();
      string ldapServer = Environment.GetEnvironmentVariable("LDAP_SERVER");
      string ldapBaseDN = Environment.GetEnvironmentVariable("LDAP_BASEDN");
      string ldapUseSSL = Environment.GetEnvironmentVariable("LDAP_USESSL");
      if (ldapServer == null || ldapServer.Length == 0)
      {
        ldapServer = "ldap";
      }
      if (ldapBaseDN == null || ldapBaseDN.Length == 0)
      {
        ldapBaseDN = "ou=ldapTesting,dc=pune,dc=gemstone,dc=com";
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

    public override Properties GetValidCredentials(int index)
    {
      Properties props = new Properties();
      props.Insert(UserPasswordAuthInit.UserNameProp, UserPrefix
        + ((index % 10) + 1));
      props.Insert(UserPasswordAuthInit.PasswordProp, UserPrefix
        + ((index % 10) + 1));
      return props;
    }

    public override Properties GetValidCredentials(Properties principal)
    {
      Properties props = null;
      string userName = principal.Find(UserPasswordAuthInit.UserNameProp);
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
          props = new Properties();
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

    public override Properties GetInvalidCredentials(int index)
    {
      Properties props = new Properties();
      props.Insert(UserPasswordAuthInit.UserNameProp, "invalid" + index);
      props.Insert(UserPasswordAuthInit.PasswordProp, "none");
      return props;
    }
  }
}
