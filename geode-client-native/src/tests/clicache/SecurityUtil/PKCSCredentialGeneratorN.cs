//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;

  public class PKCSCredentialGenerator : CredentialGenerator
  {
    public const string PublicKeyFileProp = "security-publickey-filepath";
    public const string PublicKeyPassProp = "security-publickey-pass";
    public const string KeyStoreFileProp = "security-keystorepath";
    public const string KeyStoreAliasProp = "security-alias";
    public const string KeyStorePasswordProp = "security-keystorepass";
    private const string UserPrefix = "gemfire";
    private bool IsMultiUserMode = false;
    GemStone.GemFire.Cache.Tests.NewAPI.PkcsAuthInit Pkcs = null;

    public PKCSCredentialGenerator(bool isMultiUser)
    {
      IsMultiUserMode = isMultiUser;
      if (IsMultiUserMode)
      {
        Pkcs = new PkcsAuthInit();
      }
    }

    private string GetKeyStoreDir(string dataDir)
    {
      string keystoreDir = dataDir;
      if (keystoreDir != null && keystoreDir.Length > 0)
      {
        keystoreDir += "/keystore/";
      }
      return keystoreDir;
    }

    protected override Properties<string, string> Init()
    {
      Properties<string, string> props = new Properties<string, string>();
      props.Insert(PublicKeyFileProp, GetKeyStoreDir(m_serverDataDir) +
        "publickeyfile");
      props.Insert(PublicKeyPassProp, "gemfire");
      return props;
    }

    public override ClassCode GetClassCode()
    {
      return ClassCode.PKCS;
    }

    public override string AuthInit
    {
      get
      {
        return "securityImpl::createPKCSAuthInitInstance";
      }
    }

    public override string Authenticator
    {
      get
      {
        return "templates.security.PKCSAuthenticator.create";
      }
    }

    public override Properties<string, string> GetInvalidCredentials(int index)
    {
      Properties<string, string> props = new Properties<string, string>();
      int aliasnum = (index % 10) + 1;
      props.Insert(KeyStoreFileProp, GetKeyStoreDir(m_clientDataDir) +
        "gemfire11.keystore");
      props.Insert(KeyStoreAliasProp, "gemfire11");
      props.Insert(KeyStorePasswordProp, "gemfire");
      if (!IsMultiUserMode){
        return props;
      }
      else{
        Properties<string, object> pkcsprops = Pkcs.GetCredentials/*<string, string>*/(props, "0:0");
        Properties<string, string> stringprops = new Properties<string, string>();
        PropertyVisitorGeneric<string, object> visitor =
          delegate(string key, object value)
          {
            stringprops.Insert(key, value.ToString());
          };
        pkcsprops.ForEach(visitor);
        return stringprops;
      }
    }

    public override Properties<string, string> GetValidCredentials(int index)
    {
      Properties<string, string> props = new Properties<string, string>();
      int aliasnum = (index % 10) + 1;
      props.Insert(KeyStoreFileProp, GetKeyStoreDir(m_clientDataDir) +
        UserPrefix + aliasnum + ".keystore");
      props.Insert(KeyStoreAliasProp, UserPrefix + aliasnum);
      props.Insert(KeyStorePasswordProp, "gemfire");
      if (!IsMultiUserMode){
        return props;
      }
      else{
        Properties<string, object> pkcsprops = Pkcs.GetCredentials/*<string, string>*/(props, "0:0");
        Properties<string, string> stringprops = new Properties<string, string>();
        PropertyVisitorGeneric<string, object> visitor =
          delegate(string key, object value)
          {
            stringprops.Insert(key, value.ToString());
          };
        pkcsprops.ForEach(visitor);
        return stringprops;
      }
    }

    public override Properties<string, string> GetValidCredentials(Properties<string, string> principal)
    {
      string userName = (string)principal.Find(KeyStoreAliasProp);
      Properties<string, string> props = new Properties<string, string>();
      props.Insert(KeyStoreFileProp, GetKeyStoreDir(m_clientDataDir) +
        userName + ".keystore");
      props.Insert(KeyStoreAliasProp, userName);
      props.Insert(KeyStorePasswordProp, "gemfire");
      if (!IsMultiUserMode)
      {
        return props;
      }
      else{
        Properties<string, object> pkcsprops = Pkcs.GetCredentials/*<string, string>*/(props, "0:0");
        Properties<string, string> stringprops = new Properties<string, string>();
        PropertyVisitorGeneric<string, object> visitor =
          delegate(string key, object value)
          {
            stringprops.Insert(key, value.ToString());
          };
        pkcsprops.ForEach(visitor);
        return stringprops;
      }
    }
  }
}
