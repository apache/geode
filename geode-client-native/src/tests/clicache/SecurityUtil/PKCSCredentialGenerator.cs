//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

namespace GemStone.GemFire.Cache.Tests
{
  using GemStone.GemFire.DUnitFramework;

  public class PKCSCredentialGenerator : CredentialGenerator
  {
    public const string PublicKeyFileProp = "security-publickey-filepath";
    public const string PublicKeyPassProp = "security-publickey-pass";
    public const string KeyStoreFileProp = "security-keystorepath";
    public const string KeyStoreAliasProp = "security-alias";
    public const string KeyStorePasswordProp = "security-keystorepass";
    private const string UserPrefix = "gemfire";
    private bool IsMultiUserMode = false;
    PkcsAuthInit Pkcs = null;

    public PKCSCredentialGenerator(bool isMultiUser)
    {
      IsMultiUserMode = isMultiUser;
      if (IsMultiUserMode) {
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

    protected override Properties Init()
    {
      Properties props = new Properties();
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

    public override Properties GetInvalidCredentials(int index)
    {
      Properties props = new Properties();
      int aliasnum = (index % 10) + 1;
      props.Insert(KeyStoreFileProp, GetKeyStoreDir(m_clientDataDir) +
        "gemfire11.keystore");
      props.Insert(KeyStoreAliasProp, "gemfire11");
      props.Insert(KeyStorePasswordProp, "gemfire");
      if (!IsMultiUserMode) {
        return props;
      }
      else {
        return Pkcs.GetCredentials(props, "0:0");
      }
    }

    public override Properties GetValidCredentials(int index)
    {
      Properties props = new Properties();
      int aliasnum = (index % 10) + 1;
      props.Insert(KeyStoreFileProp, GetKeyStoreDir(m_clientDataDir) +
        UserPrefix + aliasnum + ".keystore");
      props.Insert(KeyStoreAliasProp, UserPrefix + aliasnum);
      props.Insert(KeyStorePasswordProp, "gemfire");
      if (!IsMultiUserMode) {
        return props;
      }
      else {
        return Pkcs.GetCredentials(props, "0:0");
      }
    }

    public override Properties GetValidCredentials(Properties principal)
    {
      string userName = principal.Find(KeyStoreAliasProp);
      Properties props = new Properties();
      props.Insert(KeyStoreFileProp, GetKeyStoreDir(m_clientDataDir) +
        userName + ".keystore");
      props.Insert(KeyStoreAliasProp, userName);
      props.Insert(KeyStorePasswordProp, "gemfire");
      if (!IsMultiUserMode) {
        return props;
      }
      else {
        return Pkcs.GetCredentials(props, "0:0");
      }
    }
  }
}
