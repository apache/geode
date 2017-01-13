//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.Cache.Generic;
  /// <summary>
  /// Encapsulates obtaining valid and invalid credentials. Implementations will be
  /// for different kinds of authentication schemes.
  /// </summary>
  public abstract class CredentialGenerator
  {

    /// <summary>
    /// Enumeration for various <c>CredentialGenerator</c> implementations.
    /// </summary>
    /// <remarks>
    /// The following schemes are supported as of now:
    /// <c>LdapUserAuthenticator</c>, <c>PKCSAuthenticator</c>.
    /// In addition SSL socket mode with mutual authentication is also provided.
    ///
    /// To add a new authentication scheme the following needs to be done:
    /// <list type="number">
    /// <item><description>
    /// Add implementation for <see cref="GemStone.GemFire.Cache.IAuthInitialize"/>
    /// or the C++ <c>gemfire::AuthInitialize</c> interface.
    /// </description></item>
    /// <item><description>
    /// On the java server side add implementation for
    /// <c>org.apache.geode.security.Authenticator</c> interface.
    /// </description></item>
    /// <item><description>
    /// Add a new enumeration value for the scheme in this class.
    /// </description></item>
    /// <item><description>
    /// Add an implementation for <see cref="CredentialGenerator"/>.
    /// </description></item>
    /// <item><description>
    /// Modify the <see cref="CredentialGenerator.Factory.Create"/> method to
    /// add creation of an instance of the new implementation for the
    /// <c>ClassCode</c> enumeration value.
    /// </description></item>
    /// </list>
    /// All security unit tests will automagically start testing the new
    /// implementation after this.
    /// </remarks>
    public enum ClassCode
    {
      None,
      Dummy,
      LDAP,
      PKCS,
      SSL
    }

    /// <summary>
    /// A set of properties that should be added to the Gemfire system properties
    /// before using the authentication module.
    /// </summary>
    private Properties<string, string> m_sysProps = null;

    /// <summary>
    /// A set of properties that should be added to the java system properties
    /// before using the authentication module.
    /// </summary>
    protected Properties<string, string> m_javaProps = null;

    /// <summary>
    /// The directory containing data files for server (e.g. XML, keystore etc.)
    /// </summary>
    protected string m_serverDataDir = null;

    /// <summary>
    /// The directory containing data files for client (e.g. XML, keystore etc.)
    /// </summary>
    protected string m_clientDataDir = null;

    /// <summary>
    /// A factory method to create a new instance of a
    /// <c>CredentialGenerator</c> for the given <c>ClassCode</c>. Caller is
    /// supposed to invoke <see cref="CredentialGenerator.init"/> immediately
    /// after obtaining the instance.
    /// </summary>
    /// <param name="classCode">
    /// the <c>ClassCode</c> of the <c>CredentialGenerator</c> implementation
    /// </param>
    /// <returns>
    /// an instance of <c>CredentialGenerator</c> for the given class code
    /// </returns>
    public static CredentialGenerator Create(ClassCode classCode, bool isMultiUser)
    {
      switch (classCode)
      {
        case ClassCode.None:
          return null;
        case ClassCode.Dummy:
          // return new DummyCredentialGenerator();
          return null;
        case ClassCode.LDAP:
          return new LDAPCredentialGenerator();
        case ClassCode.PKCS:
          return new PKCSCredentialGenerator(isMultiUser);
        case ClassCode.SSL:
          // return new SSLCredentialGenerator();
          return null;
      }
      return null;
    }

    /// <summary>
    /// Initialize the credential generator.
    /// </summary>
    /// <exception cref="IllegalArgumentException">
    /// when there is a problem during initialization
    /// </exception>
    public void Init(string serverDataDir, string clientDataDir)
    {
      m_serverDataDir = serverDataDir;
      m_clientDataDir = clientDataDir;
      m_sysProps = Init();
    }

    /// <summary>
    /// A set of extra properties that should be added to Gemfire system
    /// properties when not null.
    /// </summary>
    public Properties<string, string> SystemProperties
    {
      get
      {
        return this.m_sysProps;
      }
    }

    /// <summary>
    /// A set of extra properties that should be added to Gemfire system
    /// properties when not null.
    /// </summary>
    public Properties<string, string> JavaProperties
    {
      get
      {
        return this.m_javaProps;
      }
    }

    /// <summary>
    /// The directory containing data files for server (e.g. XML, keystore etc.)
    /// </summary>
    public string ServerDataDir
    {
      get
      {
        return this.m_serverDataDir;
      }
    }

    /// <summary>
    /// The directory containing data files for client (e.g. XML, keystore etc.)
    /// </summary>
    public string ClientDataDir
    {
      get
      {
        return this.m_clientDataDir;
      }
    }

    /// <summary>
    /// The <see cref="ClassCode"/> of this particular implementation.
    /// </summary>
    /// <returns>the <c>ClassCode</c></returns>
    public abstract ClassCode GetClassCode();

    /// <summary>
    /// The name of the <see cref="IAuthInitialize"/> factory function that
    /// should be used in conjunction with the credentials generated by this
    /// generator.
    /// </summary>
    /// <returns>
    /// name of the <c>IAuthInitialize</c> factory function
    /// </returns>
    public abstract string AuthInit
    {
      get;
    }

    /// <summary>
    /// The name of the <c>Authenticator</c> factory function that should be
    /// used on the java server in conjunction with the credentials generated
    /// by this generator.
    /// </summary>
    /// <returns>
    /// name of the <c>Authenticator</c> factory function
    /// </returns>
    public abstract string Authenticator
    {
      get;
    }

    /// <summary>
    /// Get a set of valid credentials generated using the given index.
    /// </summary>
    public abstract Properties<string, string> GetValidCredentials(int index);

    /// <summary>
    /// Get a set of valid credentials for the given credentials representing
    /// just the Principal (e.g. username). Used by the
    /// <see cref="AuthzCredentialGenerator"/> to get the credentials for
    /// valid users.
    /// </summary>
    /// <param name="principal">properties representing the principal</param>
    /// <returns>
    /// Valid credentials for the given <c>principal</c> or null
    /// if none are possible.
    /// </returns>
    public abstract Properties<string, string> GetValidCredentials(Properties<string, string> principal);

    /// <summary>
    /// Get a set of invalid credentials generated using the given index.
    /// </summary>
    public abstract Properties<string, string> GetInvalidCredentials(int index);

    /// <summary>
    /// Initialize the credential generator. This is provided separately from
    /// the <see cref="Init"/> method for convenience of implementations so
    /// that they do not need to store in <c>m_sysProps</c>. The latter is
    /// convenient for the users who do not need to store these properties
    /// rather can obtain it later by invoking <see cref="GetSystemProperties"/>
    /// </summary>
    /// <remarks>
    /// Required to be implemented by concrete classes that implement this
    /// abstract class.
    /// </remarks>
    /// <returns>
    /// A set of extra properties that should be added to Gemfire system
    /// properties when not null.
    /// </returns>
    /// <exception cref="IllegalArgumentException">
    /// when there is a problem during initialization
    /// </exception>
    protected abstract Properties<string, string> Init();
  }
}
