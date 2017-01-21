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

namespace Apache.Geode.Templates.Cache.Security
{

  using Apache.Geode.Client;

  ///<summary>
  /// An <see cref="Apache.Geode.Client.IAuthInitialize"/> implementation
  /// that obtains the user name and password as the credentials from the
  /// given set of properties.
  /// </summary>
  /// <remarks>
  /// To use this class the <c>security-client-auth-library</c> property should
  /// be set to the name of the dll (<c>Apache.Geode.Templates.Cache.Security</c>)
  /// and the <c>security-client-auth-factory</c> property should be set to
  /// the fully qualified name of the static <c>Create</c> function viz.
  /// <c>Apache.Geode.Templates.Cache.Security.UserPasswordAuthInit.Create</c>
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
