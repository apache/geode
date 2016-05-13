//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using GemStone.GemFire.DUnitFramework;

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.Cache.Generic;
  /// <summary>
  /// Enumeration for the various client server operations.
  /// </summary>
  public enum OperationCode
  {
    Get,
    Put,
    GetAll,
    PutAll,
    RemoveAll,
    Destroy,
    Invalidate,
    GetServerKeys,
    RegisterInterest,
    UnregisterInterest,
    Query,
    ExecuteCQ,
    ExecuteFunction,
    RegionDestroy
  }

  /// <summary>
  /// Encapsulates obtaining authorized and unauthorized credentials for a
  /// given operation in a region. Implementations will be for different
  /// kinds of authorization scheme and authentication scheme combos.
  /// </summary>
  public abstract class AuthzCredentialGenerator
  {

    /// <summary>
    /// Enumeration for various <c>AuthzCredentialGenerator</c> implementations.
    /// </summary>
    /// <remarks>
    /// The following schemes are supported as of now:
    /// <list type="number">
    /// <item><description>
    /// <c>DummyAuthorization</c> with <c>DummyAuthenticator</c>
    /// </description></item>
    /// <item><description>
    /// <c>XMLAuthorization</c> with <c>DummyAuthenticator</c></li>
    /// </description></item>
    /// <item><description>
    /// <c>XMLAuthorization</c> with <c>LDAPAuthenticator</c>
    /// </description></item>
    /// <item><description>
    /// <c>XMLAuthorization</c> with <c>PKCSAuthenticator</c>
    /// </description></item>
    /// <item><description>
    /// <c>XMLAuthorization</c> when using SSL sockets
    /// </description></item>
    /// </list>
    /// 
    /// To add a new authorization scheme the following needs to be done:
    /// <list type="number">
    /// <item><description>
    /// Add implementation for <c>AccessControl</c> on the java server.
    /// </description></item>
    /// <item><description>
    /// Choose the authentication schemes that it shall work with, from
    /// <see cref="CredentialGenerator.ClassCode"/>
    /// </description></item>
    /// <item><description>
    /// Add a new enumeration value for the scheme in this enum.
    /// </description></item>
    /// <item><description>
    /// Add an implementation for <c>AuthzCredentialGenerator</c>. Note the
    /// <see cref="AuthzCredentialGenerator.init"/> method where different
    /// authentication schemes can be passed and initialize differently for
    /// the authentication schemes that shall be handled by this authorization
    /// callback.
    /// </description></item>
    /// <item><description>
    /// Modify the <see cref="AuthzCredentialGenerator.create"/> method to add
    /// creation of an instance of the new implementation for the
    /// <c>ClassCode</c> enumeration value.
    /// </description></item>
    /// </list>
    /// All dunit tests will automagically start testing the new
    /// implementation after this.
    /// </remarks>
    public enum ClassCode
    {
      Dummy,
      XML
    }

    /// <summary>
    /// The <see cref="CredentialGenerator"/> being used.
    /// </summary>
    protected CredentialGenerator m_cGen;

    /// <summary>
    /// A set of system properties that should be added to the java server
    /// system properties before using the authorization module.
    /// </summary>
    private Properties<string, string> m_sysProps;

    /// <summary>
    /// A factory method to create a new instance of an
    /// <c>AuthzCredentialGenerator</c>
    /// for the given <c>ClassCode</c>. Caller
    /// is supposed to invoke
    /// <see cref="AuthzCredentialGenerator.init"/> immediately
    /// after obtaining the instance.
    /// </summary>
    /// <param name="classCode">
    /// the <c>ClassCode</c> of the <c>AuthzCredentialGenerator</c>
    /// implementation
    /// </param>
    /// <returns>
    /// an instance of <c>AuthzCredentialGenerator</c> for
    /// the given class code
    /// </returns>
    public static AuthzCredentialGenerator Create(ClassCode classCode)
    {
      switch (classCode)
      {
        case ClassCode.Dummy:
          //return new DummyAuthzCredentialGenerator();
          return null;
        case ClassCode.XML:
          return new XmlAuthzCredentialGenerator();
      }
      return null;
    }

    /// <summary>
    /// Initialize the authorized credential generator.
    /// </summary>
    /// <param name="cGen">
    /// an instance of <see cref="CredentialGenerator"/> of the credential
    /// implementation for which to obtain authorized/unauthorized credentials.
    /// </param>
    /// <returns>
    /// false when the given <c>CredentialGenerator</c> is incompatible with
    /// this authorization module.
    /// </returns>
    public bool Init(CredentialGenerator cGen)
    {
      this.m_cGen = cGen;
      try
      {
        this.m_sysProps = Init();
      }
      catch (Exception)
      {
        return false;
      }
      return true;
    }

    /// <summary>
    /// A set of extra properties that should be added to java server system
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
    /// Get the <c>CredentialGenerator</c> being used by this instance.
    /// </summary>
    public CredentialGenerator GetCredentialGenerator()
    {
      return this.m_cGen;
    }

    /// <summary>
    /// The <see cref="ClassCode"/> of the particular implementation.
    /// </summary>
    /// <returns>the <c>ClassCode</c></returns>
    public abstract ClassCode GetClassCode();

    /// <summary>
    /// The name of the <c>AccessControl</c> factory function that should be
    /// used as the authorization module on the java server side.
    /// </summary>
    /// <returns>name of the <c>AccessControl</c> factory function</returns>
    public abstract string AccessControl
    {
      get;
    }

    /// <summary>
    /// Get a set of credentials generated using the given index allowed to
    /// perform the given <see cref="OperationCode"/>s for the given regions.
    /// </summary>
    /// <param name="opCodes">
    /// the list of <see cref="OperationCode"/>s of the operations requiring
    /// authorization; should not be null
    /// </param>
    /// <param name="regionNames">
    /// list of the region names requiring authorization; a value of null
    /// indicates all regions
    /// </param>
    /// <param name="index">
    /// used to generate multiple such credentials by passing different
    /// values for this
    /// </param>
    /// <returns>
    /// the set of credentials authorized to perform the given operation in
    /// the given regions
    /// </returns>
    public Properties<string, string> GetAllowedCredentials(OperationCode[] opCodes,
        string[] regionNames, int index)
    {
      int numTries = GetNumPrincipalTries(opCodes, regionNames);
      if (numTries <= 0)
      {
        numTries = 1;
      }
      for (int tries = 0; tries < numTries; ++tries)
      {
        Properties<string, string> principal = GetAllowedPrincipal(opCodes, regionNames,
            (index + tries) % numTries);
        try
        {
          return this.m_cGen.GetValidCredentials(principal);
        }
        catch (IllegalArgumentException)
        {
        }
      }
      return null;
    }

    /// <summary>
    /// Get a set of credentials generated using the given index not allowed to
    /// perform the given <see cref="OperationCode"/>s for the given regions.
    /// The credentials are required to be valid for authentication.
    /// </summary>
    /// <param name="opCodes">
    /// the <see cref="OperationCode"/>s of the operations requiring
    /// authorization failure; should not be null
    /// </param>
    /// <param name="regionNames">
    /// list of the region names requiring authorization failure;
    /// a value of null indicates all regions
    /// </param>
    /// <param name="index">
    /// used to generate multiple such credentials by passing different
    /// values for this
    /// </param>
    /// <returns>
    /// the set of credentials that are not authorized to perform the given
    /// operation in the given region
    /// </returns>
    public Properties<string, string> GetDisallowedCredentials(OperationCode[] opCodes,
        string[] regionNames, int index)
    {
      // This may not be very correct since we use the value of
      // getNumPrincipalTries() but is used to avoid adding another method.
      // Also something like getNumDisallowedPrincipals() will be normally always
      // infinite, and the number here is just to perform some number of tries
      // before giving up.
      int numTries = GetNumPrincipalTries(opCodes, regionNames);
      if (numTries <= 0)
      {
        numTries = 1;
      }
      for (int tries = 0; tries < numTries; tries++)
      {
        Properties<string, string> principal = GetDisallowedPrincipal(opCodes, regionNames,
            (index + tries) % numTries);
        try
        {
          return this.m_cGen.GetValidCredentials(principal);
        }
        catch (IllegalArgumentException)
        {
        }
      }
      return null;
    }

    /// <summary>
    /// Initialize the authorized credential generator.
    /// </summary>
    /// <returns>
    /// A set of extra properties that should be added to java server
    /// system properties when not null.
    /// </returns>
    /// <exception cref="IllegalArgumentException">
    /// when the <see cref="CredentialGenerator"/> is incompatible with
    /// this authorization module.
    /// </exception>
    protected abstract Properties<string, string> Init();

    /// <summary>
    /// Get the number of tries to be done for obtaining valid credentials for
    /// the given operations in the given region. It is required that
    /// <c>getAllowedPrincipal</c> method returns valid principals for values
    /// of <c>index</c> from 0 through (n-1) where <c>n</c> is the value
    /// returned by this method. It is recommended that the principals so
    /// returned be unique for efficiency.
    /// </summary>
    /// <param name="opCodes">
    /// the <see cref="OperationCode"/>s of the operations requiring
    /// authorization
    /// </param>
    /// <param name="regionNames">
    /// list of the region names requiring authorization; a value of null
    /// indicates all regions
    /// </param>
    /// <returns>
    /// the number of principals allowed to perform the given operation in
    /// the given region
    /// </returns>
    protected abstract int GetNumPrincipalTries(OperationCode[] opCodes,
        string[] regionNames);

    /// <summary>
    /// Get a <c>Principal</c> generated using the given index allowed to
    /// perform the given <see cref="OperationCode"/>s for the given region.
    /// </summary>
    /// <param name="opCodes">
    /// the list of <see cref="OperationCode"/>s of the operations requiring
    /// authorization; should not be null
    /// </param>
    /// <param name="regionNames">
    /// list of the region names requiring authorization; a value of null
    /// indicates all regions
    /// </param>
    /// <param name="index">
    /// used to generate multiple such credentials by passing different
    /// values for this
    /// </param>
    /// <returns>
    /// the <c>Principal</c> authorized to perform the given operation in
    /// the given region
    /// </returns>
    protected abstract Properties<string, string> GetAllowedPrincipal(OperationCode[] opCodes,
        string[] regionNames, int index);

    /// <summary>
    /// Required to be implemented by concrete classes that implement this
    /// abstract class.
    /// </summary>
    /// <param name="opCodes">
    /// the <see cref="OperationCode"/>s of the operations requiring
    /// authorization failure; should not be null
    /// </param>
    /// <param name="regionNames">
    /// list of the region names requiring authorization failure;
    /// a value of null indicates all regions
    /// </param>
    /// <param name="index">
    /// used to generate multiple such credentials by passing different
    /// values for this
    /// </param>
    /// <returns>
    /// a <c>Principal</c> not authorized to perform the given operation
    /// in the given region
    /// </returns>
    protected abstract Properties<string, string> GetDisallowedPrincipal(OperationCode[] opCodes,
        string[] regionNames, int index);
  }

  /// <summary>
  /// Utility class for security generators.
  /// </summary>
  public static class Utility
  {
    /// <summary>
    /// Build the string to be passed to the cacheserver command for the given
    /// security authenticator/accessor properties.
    /// </summary>
    public static string GetServerArgs(string authenticator,
      string accessor, string accessorPP, Properties<string, string> extraProps,
      Properties<string, string> javaProps)
    {
      string args = string.Empty;
      if (authenticator != null && authenticator.Length > 0)
      {
        args += (" security-client-authenticator=" + authenticator);
      }
      if (accessor != null && accessor.Length > 0)
      {
        args += (" security-client-accessor=" + accessor);
      }
      if (accessorPP != null && accessorPP.Length > 0)
      {
        args += (" security-client-accessor-pp=" + accessorPP);
      }
      if (extraProps != null)
      {
        PropertyVisitorGeneric<string, string> visitor =
          delegate(string key, string value)
          {
            args += (' ' + key.ToString() + '=' + value);
          };
        extraProps.ForEach(visitor);
      }
      if (javaProps != null)
      {
        PropertyVisitorGeneric<string, string> visitor =
          delegate(string key, string value)
          {
            args += (" -J-D" + key.ToString() + '=' + value);
          };
        javaProps.ForEach(visitor);
      }
      args = args.Trim();
      return args;
    }

    /// <summary>
    /// Get the client system properties.
    /// </summary>
    public static void GetClientProperties(string authInit,
      Properties<string, string> credentials, ref Properties<string, string> props)
    {
      if (authInit != null && authInit.Length > 0)
      {
        if (props == null)
        {
          props = new Properties<string, string>();
        }
        string library;
        string factory;
        const string cppIndicator = "::";
        int libIndex = authInit.IndexOf(cppIndicator);
        if (libIndex > 0)
        {
          library = authInit.Substring(0, libIndex);
          factory = authInit.Substring(libIndex + cppIndicator.Length);
        }
        else
        {
          library = "GemStone.GemFire.Templates.Cache.Security";
          factory = authInit;
        }
        props.Insert("security-client-auth-library", library);
        props.Insert("security-client-auth-factory", factory);
        if (credentials != null)
        {
          props.AddAll(credentials);
        }
      }
    }
  }
}
