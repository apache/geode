
package security;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.Authenticator;

/**
 * Encapsulates obtaining valid and invalid credentials. Implementations will be
 * for different kinds of authentication schemes.
 * 
 * @author sumedh
 * @since 5.5
 */
public abstract class CredentialGenerator {

  /**
   * Enumeration for various {@link CredentialGenerator} implementations.
   * 
   * The following schemes are supported as of now:
   * <code>DummyAuthenticator</code>, <code>LdapUserAuthenticator</code>,
   * <code>PKCSAuthenticator</code>. In addition SSL socket mode with mutual
   * authentication is also supported.
   * 
   * To add a new authentication scheme the following needs to be done:
   * <ul>
   * <li>Add implementations for {@link AuthInitialize} and
   * {@link Authenticator} classes for clients/peers.</li>
   * <li>Add a new enumeration value for the scheme in this class. Notice the
   * size of <code>VALUES</code> array and increase that if it is getting
   * overflowed. Note the methods and fields for existing schemes and add for
   * the new one in a similar manner.</li>
   * <li>Add an implementation for {@link CredentialGenerator}.</li>
   * <li>Modify the CredentialGenerator.Factory#create [no such Factory exists] method to add
   * creation of an instance of the new implementation for the
   * <code>ClassCode</code> enumeration value.</li>
   * </ul>
   * All security dunit tests will automagically start testing the new
   * implementation after this.
   * 
   * @author sumedh
   * @since 5.5
   */
  public static final class ClassCode {

    private static final byte ID_DUMMY = 1;

    private static final byte ID_LDAP = 2;

    private static final byte ID_PKCS = 3;

    private static final byte ID_SSL = 4;

    private static byte nextOrdinal = 0;

    private static final ClassCode[] VALUES = new ClassCode[10];

    private static final Map CodeNameMap = new HashMap();

    public static final ClassCode DUMMY = new ClassCode(
        "templates.security.DummyAuthenticator.create", ID_DUMMY);

    public static final ClassCode LDAP = new ClassCode(
        "templates.security.LdapUserAuthenticator.create", ID_LDAP);

    public static final ClassCode PKCS = new ClassCode(
        "templates.security.PKCSAuthenticator.create", ID_PKCS);

    public static final ClassCode SSL = new ClassCode("SSL", ID_SSL);

    /** The name of this class. */
    private final String name;

    /** byte used as ordinal to represent this class */
    private final byte ordinal;

    /**
     * One of the following: ID_DUMMY, ID_LDAP, ID_PKCS
     */
    private final byte classType;

    /** Creates a new instance of class code. */
    private ClassCode(String name, byte classType) {
      this.name = name;
      this.classType = classType;
      this.ordinal = nextOrdinal++;
      VALUES[this.ordinal] = this;
      CodeNameMap.put(name, this);
    }

    public boolean isDummy() {
      return (this.classType == ID_DUMMY);
    }

    public boolean isLDAP() {
      return (this.classType == ID_LDAP);
    }

    public boolean isPKCS() {
      return (this.classType == ID_PKCS);
    }

    public boolean isSSL() {
      return (this.classType == ID_SSL);
    }

    /**
     * Returns the <code>ClassCode</code> represented by specified ordinal.
     */
    public static ClassCode fromOrdinal(byte ordinal) {
      return VALUES[ordinal];
    }

    /**
     * Returns the <code>ClassCode</code> represented by specified string.
     */
    public static ClassCode parse(String operationName) {
      return (ClassCode)CodeNameMap.get(operationName);
    }

    /**
     * Returns all the possible values.
     */
    public static List getAll() {
      List codes = new ArrayList();
      Iterator iter = CodeNameMap.values().iterator();
      while (iter.hasNext()) {
        codes.add(iter.next());
      }
      return codes;
    }

    /**
     * Returns the ordinal for this operation code.
     * 
     * @return the ordinal of this operation.
     */
    public byte toOrdinal() {
      return this.ordinal;
    }

    /**
     * Returns a string representation for this operation.
     * 
     * @return the name of this operation.
     */
    final public String toString() {
      return this.name;
    }

    /**
     * Indicates whether other object is same as this one.
     * 
     * @return true if other object is same as this one.
     */
    @Override
    final public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof ClassCode)) {
        return false;
      }
      final ClassCode other = (ClassCode)obj;
      return (other.ordinal == this.ordinal);
    }

    /**
     * Indicates whether other <code>ClassCode</code> is same as this one.
     * 
     * @return true if other <code>ClassCode</code> is same as this one.
     */
    final public boolean equals(final ClassCode opCode) {
      return (opCode != null && opCode.ordinal == this.ordinal);
    }

    /**
     * Returns a hash code value for this <code>ClassCode</code> which is the
     * same as its ordinal.
     * 
     * @return the ordinal of this operation.
     */
    @Override
    final public int hashCode() {
      return this.ordinal;
    }

  }

  /**
   * A set of properties that should be added to the Gemfire system properties
   * before using the authentication module.
   */
  private Properties sysProps = null;

  /**
   * A set of properties that should be added to the java system properties
   * before using the authentication module.
   */
  protected Properties javaProps = null;

  /**
   * A factory method to create a new instance of an {@link CredentialGenerator}
   * for the given {@link ClassCode}. Caller is supposed to invoke
   * {@link CredentialGenerator#init} immediately after obtaining the instance.
   * 
   * @param classCode
   *                the <code>ClassCode</code> of the
   *                <code>CredentialGenerator</code> implementation
   * 
   * @return an instance of <code>CredentialGenerator</code> for the given
   *         class code
   */
  public static CredentialGenerator create(ClassCode classCode) {
    switch (classCode.classType) {
      // Removing dummy one to reduce test run times
      // case ClassCode.ID_DUMMY:
      // return new DummyCredentialGenerator();
      case ClassCode.ID_LDAP:
        return new LdapUserCredentialGenerator();
        // case ClassCode.ID_SSL:ø
        // return new SSLCredentialGenerator();
      case ClassCode.ID_PKCS:
        return new PKCSCredentialGenerator();
      default:
        return null;
    }
  }

  /**
   * Initialize the credential generator.
   * 
   * @throws IllegalArgumentException
   *                 when there is a problem during initialization
   */
  public void init() throws IllegalArgumentException {
    this.sysProps = initialize();
  }

  /**
   * Initialize the credential generator. This is provided separately from the
   * {@link #init} method for convenience of implementations so that they do not
   * need to store in {@link #sysProps}. The latter is convenient for the users
   * who do not need to store these properties rather can obtain it later by
   * invoking {@link #getSystemProperties}
   * 
   * Required to be implemented by concrete classes that implement this abstract
   * class.
   * 
   * @return A set of extra properties that should be added to Gemfire system
   *         properties when not null.
   * 
   * @throws IllegalArgumentException
   *                 when there is a problem during initialization
   */
  protected abstract Properties initialize() throws IllegalArgumentException;

  /**
   * 
   * @return A set of extra properties that should be added to Gemfire system
   *         properties when not null.
   */
  public Properties getSystemProperties() {
    return this.sysProps;
  }

  /**
   * 
   * @return A set of extra properties that should be added to Gemfire system
   *         properties when not null.
   */
  public Properties getJavaProperties() {
    return this.javaProps;
  }

  /**
   * The {@link ClassCode} of this particular implementation.
   * 
   * @return the <code>ClassCode</code>
   */
  public abstract ClassCode classCode();

  /**
   * The name of the {@link AuthInitialize} factory function that should be used
   * in conjunction with the credentials generated by this generator.
   * 
   * @return name of the <code>AuthInitialize</code> factory function
   */
  public abstract String getAuthInit();

  /**
   * The name of the {@link Authenticator} factory function that should be used
   * in conjunction with the credentials generated by this generator.
   * 
   * @return name of the <code>Authenticator</code> factory function
   */
  public abstract String getAuthenticator();

  /**
   * Get a set of valid credentials generated using the given index.
   */
  public abstract Properties getValidCredentials(int index);

  /**
   * Get a set of valid credentials for the given {@link Principal}.
   * 
   * @return credentials for the given <code>Principal</code> or null if none
   *         possible.
   */
  public abstract Properties getValidCredentials(Principal principal);

  /**
   * Get a set of invalid credentials generated using the given index.
   */
  public abstract Properties getInvalidCredentials(int index);
}
