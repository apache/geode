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

namespace Apache.Geode.Client.Tests
{
  using Apache.Geode.DUnitFramework;
  using Apache.Geode.Client;

  public class PKCSCredentialGenerator : CredentialGenerator
  {
    public const string PublicKeyFileProp = "security-publickey-filepath";
    public const string PublicKeyPassProp = "security-publickey-pass";
    public const string KeyStoreFileProp = "security-keystorepath";
    public const string KeyStoreAliasProp = "security-alias";
    public const string KeyStorePasswordProp = "security-keystorepass";
    private const string UserPrefix = "geode";
    private bool IsMultiUserMode = false;
    Apache.Geode.Client.Tests.PkcsAuthInit Pkcs = null;

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
      props.Insert(PublicKeyPassProp, "geode");
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
        "geode11.keystore");
      props.Insert(KeyStoreAliasProp, "geode11");
      props.Insert(KeyStorePasswordProp, "geode");
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
      props.Insert(KeyStorePasswordProp, "geode");
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
      props.Insert(KeyStorePasswordProp, "geode");
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
