//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

namespace GemStone.GemFire.Templates.Cache.Security
{

  using GemStone.GemFire.Cache.Generic;

  ///<summary>
  /// An <see cref="GemStone.GemFire.Cache.Generic.IAuthInitialize"/> implementation
  /// that obtains the user name and password as the credentials from the
  /// given set of properties.
  /// </summary>
  /// <remarks>
  /// To use this class the <c>security-client-auth-library</c> property should
  /// be set to the name of the dll (<c>GemStone.GemFire.Templates.Cache.Security</c>)
  /// and the <c>security-client-auth-factory</c> property should be set to
  /// the fully qualified name of the static <c>Create</c> function viz.
  /// <c>GemStone.GemFire.Templates.Cache.Security.UserPasswordAuthInit.Create</c>
  /// </remarks>
  public class UserPasswordAuthInit : IAuthInitialize
  {
    /// <summary>
    /// Property name for username.
    /// </summary>
    public const string UserNameProp = "security-username";

    /// <summary>
    /// Property name for password. It is required that the client application
    /// obtains the password and sets it as a system property.
    /// </summary>
    public const string PasswordProp = "security-password";

    /// <summary>
    /// Static method that should be registered as the
    /// <c>security-client-auth-factory</c> property.
    /// </summary>
    public static IAuthInitialize Create()
    {
      return new UserPasswordAuthInit();
    }

    /// <summary>
    /// Default constructor -- does nothing.
    /// </summary>
    public UserPasswordAuthInit()
    {
    }

    /// <summary>
    /// Get the credentials by copying the username(<see cref="UserNameProp"/>)
    /// and password(<see cref="PasswordProp"/>) properties.
    /// </summary>
    public Properties<string, object> GetCredentials(
      Properties<string, string> props, string server)
    {
      Properties<string, object> newProps = new Properties<string, object>();

      string userName = props.Find(UserNameProp);
      if (userName == null)
      {
        throw new AuthenticationFailedException(string.Format(
          "UserPasswordAuthInit: user name property [{0}] not set.",
          UserNameProp));
      }
      newProps.Insert(UserNameProp, userName);
      string passwd = props.Find(PasswordProp);
      // If password is not provided then use empty string as the password.
      if (passwd == null)
      {
        passwd = string.Empty;
      }
      newProps.Insert(PasswordProp, passwd);

      return newProps;
    }

    /// <summary>
    /// Invoked before the cache goes down.
    /// </summary>
    public void Close()
    {
      // Nothing to be done.
    }
  }
}
