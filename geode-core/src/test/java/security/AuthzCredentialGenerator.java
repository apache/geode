
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

import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.security.AccessControl;

/**
 * Encapsulates obtaining authorized and unauthorized credentials for a given
 * operation in a region. Implementations will be for different kinds of
 * authorization scheme and authentication scheme combos.
 * 
 * @author sumedh
 * @since 5.5
 */
public abstract class AuthzCredentialGenerator {

  /**
   * Enumeration for various {@link AuthzCredentialGenerator} implementations.
   * 
   * The following schemes are supported as of now:
   * <ul>
   * <li><code>DummyAuthorization</code> with <code>DummyAuthenticator</code></li>
   * <li><code>XMLAuthorization</code> with <code>DummyAuthenticator</code></li>
   * <li><code>XMLAuthorization</code> with <code>LDAPAuthenticator</code></li>
   * <li><code>XMLAuthorization</code> with <code>PKCSAuthenticator</code></li>
   * <li><code>XMLAuthorization</code> when using SSL sockets</li>
   * </ul>
   * 
   * To add a new authorization scheme the following needs to be done:
   * <ul>
   * <li>Add implementation for {@link AccessControl}.</li>
   * <li>Choose the authentication schemes that it shall work with from
   * {@link CredentialGenerator.ClassCode}</li>
   * <li>Add a new enumeration value for the scheme in this class. Notice the
   * size of <code>VALUES</code> array and increase that if it is getting
   * overflowed. Note the methods and fields for existing schemes and add for
   * the new one in a similar manner.</li>
   * <li>Add an implementation for {@link AuthzCredentialGenerator}. Note the
   * {@link AuthzCredentialGenerator#init} method where different authentication
   * schemes can be passed and initialize differently for the authentication
   * schemes that shall be handled.</li>
   * <li>Modify the {@link AuthzCredentialGenerator#create} method to add
   * creation of an instance of the new implementation for the
   * <code>ClassCode</code> enumeration value.</li>
   * </ul>
   * All dunit tests will automagically start testing the new implementation
   * after this.
   * 
   * @author sumedh
   * @since 5.5
   */
  public static final class ClassCode {

    private static final byte ID_DUMMY = 1;

    private static final byte ID_XML = 2;

    private static byte nextOrdinal = 0;

    private static final ClassCode[] VALUES = new ClassCode[10];

    private static final Map CodeNameMap = new HashMap();

    public static final ClassCode DUMMY = new ClassCode(
        "templates.security.DummyAuthorization.create", ID_DUMMY);

    public static final ClassCode XML = new ClassCode(
        "templates.security.XmlAuthorization.create", ID_XML);

    /** The name of this class. */
    private final String name;

    /** byte used as ordinal to represent this class */
    private final byte ordinal;

    /**
     * One of the following: ID_DUMMY, ID_LDAP, ID_PKI
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

    public boolean isXml() {
      return (this.classType == ID_XML);
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
     * Returns the ordinal for this class code.
     * 
     * @return the ordinal of this class code.
     */
    public byte toOrdinal() {
      return this.ordinal;
    }

    /**
     * Returns a string representation for this class code.
     * 
     * @return the name of this class code.
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
     * @return the ordinal of this <code>ClassCode</code>.
     */
    @Override
    final public int hashCode() {
      return this.ordinal;
    }

  }

  /**
   * The {@link CredentialGenerator} being used.
   */
  protected CredentialGenerator cGen;

  /**
   * A set of system properties that should be added to the gemfire system
   * properties before using the authorization module.
   */
  private Properties sysProps;

  /**
   * A factory method to create a new instance of an
   * {@link AuthzCredentialGenerator} for the given {@link ClassCode}. Caller
   * is supposed to invoke {@link AuthzCredentialGenerator#init} immediately
   * after obtaining the instance.
   * 
   * @param classCode
   *                the <code>ClassCode</code> of the
   *                <code>AuthzCredentialGenerator</code> implementation
   * 
   * @return an instance of <code>AuthzCredentialGenerator</code> for the
   *         given class code
   */
  public static AuthzCredentialGenerator create(ClassCode classCode) {
    switch (classCode.classType) {
      case ClassCode.ID_DUMMY:
        return new DummyAuthzCredentialGenerator();
      case ClassCode.ID_XML:
        return new XmlAuthzCredentialGenerator();
      default:
        return null;
    }
  }

  /**
   * Initialize the authorized credential generator.
   * 
   * @param cGen
   *                an instance of {@link CredentialGenerator} of the credential
   *                implementation for which to obtain authorized/unauthorized
   *                credentials.
   * 
   * @return false when the given {@link CredentialGenerator} is incompatible
   *         with this authorization module.
   */
  public boolean init(CredentialGenerator cGen) {
    this.cGen = cGen;
    try {
      this.sysProps = init();
    }
    catch (IllegalArgumentException ex) {
      return false;
    }
    return true;
  }

  /**
   * 
   * @return A set of extra properties that should be added to Gemfire system
   *         properties when not null.
   */
  public Properties getSystemProperties() {
    return this.sysProps;
  }

  /**
   * Get the {@link CredentialGenerator} being used by this instance.
   */
  public CredentialGenerator getCredentialGenerator() {
    return this.cGen;
  }

  /**
   * The {@link ClassCode} of the particular implementation.
   * 
   * @return the <code>ClassCode</code>
   */
  public abstract ClassCode classCode();

  /**
   * The name of the {@link AccessControl} factory function that should be used
   * as the authorization module on the server side.
   * 
   * @return name of the <code>AccessControl</code> factory function
   */
  public abstract String getAuthorizationCallback();

  /**
   * Get a set of credentials generated using the given index allowed to perform
   * the given {@link OperationCode}s for the given regions.
   * 
   * @param opCodes
   *                the list of {@link OperationCode}s of the operations
   *                requiring authorization; should not be null
   * @param regionNames
   *                list of the region names requiring authorization; a value of
   *                null indicates all regions
   * @param index
   *                used to generate multiple such credentials by passing
   *                different values for this
   * 
   * @return the set of credentials authorized to perform the given operation in
   *         the given regions
   */
  public Properties getAllowedCredentials(OperationCode[] opCodes,
      String[] regionNames, int index) {

    int numTries = getNumPrincipalTries(opCodes, regionNames);
    if (numTries <= 0) {
      numTries = 1;
    }
    for (int tries = 0; tries < numTries; tries++) {
      Principal principal = getAllowedPrincipal(opCodes, regionNames,
          (index + tries) % numTries);
      try {
        return this.cGen.getValidCredentials(principal);
      }
      catch (IllegalArgumentException ex) {
      }
    }
    return null;
  }

  /**
   * Get a set of credentials generated using the given index not allowed to
   * perform the given {@link OperationCode}s for the given regions. The
   * credentials are required to be valid for authentication.
   * 
   * @param opCodes
   *                the {@link OperationCode}s of the operations requiring
   *                authorization failure; should not be null
   * @param regionNames
   *                list of the region names requiring authorization failure; a
   *                value of null indicates all regions
   * @param index
   *                used to generate multiple such credentials by passing
   *                different values for this
   * 
   * @return the set of credentials that are not authorized to perform the given
   *         operation in the given region
   */
  public Properties getDisallowedCredentials(OperationCode[] opCodes,
      String[] regionNames, int index) {

    // This may not be very correct since we use the value of
    // getNumPrincipalTries() but is used to avoid adding another method.
    // Also something like getNumDisallowedPrincipals() will be normally always
    // infinite, and the number here is just to perform some number of tries
    // before giving up.
    int numTries = getNumPrincipalTries(opCodes, regionNames);
    if (numTries <= 0) {
      numTries = 1;
    }
    for (int tries = 0; tries < numTries; tries++) {
      Principal principal = getDisallowedPrincipal(opCodes, regionNames,
          (index + tries) % numTries);
      try {
        return this.cGen.getValidCredentials(principal);
      }
      catch (IllegalArgumentException ex) {
      }
    }
    return null;
  }

  /**
   * Initialize the authorized credential generator.
   * 
   * Required to be implemented by concrete classes that implement this abstract
   * class.
   * 
   * @return A set of extra properties that should be added to Gemfire system
   *         properties when not null.
   * 
   * @throws IllegalArgumentException
   *                 when the {@link CredentialGenerator} is incompatible with
   *                 this authorization module.
   */
  protected abstract Properties init() throws IllegalArgumentException;

  /**
   * Get the number of tries to be done for obtaining valid credentials for the
   * given operations in the given region. It is required that
   * {@link #getAllowedPrincipal} method returns valid principals for values of
   * <code>index</code> from 0 through (n-1) where <code>n</code> is the
   * value returned by this method. It is recommended that the principals so
   * returned be unique for efficiency.
   * 
   * This will be used by {@link #getAllowedCredentials} to step through
   * different principals and obtain a set of valid credentials.
   * 
   * Required to be implemented by concrete classes that implement this abstract
   * class.
   * 
   * @param opCodes
   *                the {@link OperationCode}s of the operations requiring
   *                authorization
   * @param regionNames
   *                list of the region names requiring authorization; a value of
   *                null indicates all regions
   * 
   * @return the number of principals allowed to perform the given operation in
   *         the given region
   */
  protected abstract int getNumPrincipalTries(OperationCode[] opCodes,
      String[] regionNames);

  /**
   * Get a {@link Principal} generated using the given index allowed to perform
   * the given {@link OperationCode}s for the given region.
   * 
   * Required to be implemented by concrete classes that implement this abstract
   * class.
   * 
   * @param opCodes
   *                the {@link OperationCode}s of the operations requiring
   *                authorization
   * @param regionNames
   *                list of the region names requiring authorization; a value of
   *                null indicates all regions
   * @param index
   *                used to generate multiple such principals by passing
   *                different values for this
   * 
   * @return the {@link Principal} authorized to perform the given operation in
   *         the given region
   */
  protected abstract Principal getAllowedPrincipal(OperationCode[] opCodes,
      String[] regionNames, int index);

  /**
   * Get a {@link Principal} generated using the given index not allowed to
   * perform the given {@link OperationCode}s for the given region.
   * 
   * Required to be implemented by concrete classes that implement this abstract
   * class.
   * 
   * @param opCodes
   *                the {@link OperationCode}s of the operations requiring
   *                authorization failure
   * @param regionNames
   *                list of the region names requiring authorization failure; a
   *                value of null indicates all regions
   * @param index
   *                used to generate multiple such principals by passing
   *                different values for this
   * 
   * @return a {@link Principal} not authorized to perform the given operation
   *         in the given region
   */
  protected abstract Principal getDisallowedPrincipal(OperationCode[] opCodes,
      String[] regionNames, int index);
}
