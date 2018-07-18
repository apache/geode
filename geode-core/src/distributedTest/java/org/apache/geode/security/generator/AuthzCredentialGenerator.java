/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.security.generator;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.AccessControl;
import org.apache.geode.security.templates.DummyAuthorization;
import org.apache.geode.security.templates.XmlAuthorization;

/**
 * Encapsulates obtaining authorized and unauthorized credentials for a given operation in a region.
 * Implementations will be for different kinds of authorization scheme and authentication scheme
 * combos.
 *
 * @since GemFire 5.5
 */
public abstract class AuthzCredentialGenerator {

  private static final Logger logger = LogService.getLogger();

  /**
   * The {@link CredentialGenerator} being used.
   */
  protected CredentialGenerator generator;

  /**
   * A set of system properties that should be added to the gemfire system properties before using
   * the authorization module.
   */
  private Properties systemProperties;

  /**
   * A factory method to create a new instance of an {@link AuthzCredentialGenerator} for the given
   * {@link ClassCode}. Caller is supposed to invoke {@link AuthzCredentialGenerator#init}
   * immediately after obtaining the instance.
   *
   * @param classCode the {@code ClassCode} of the {@code AuthzCredentialGenerator} implementation
   *
   * @return an instance of {@code AuthzCredentialGenerator} for the given class code
   */
  public static AuthzCredentialGenerator create(final ClassCode classCode) {
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
   * @param generator an instance of {@link CredentialGenerator} of the credential implementation
   *        for which to obtain authorized/unauthorized credentials.
   *
   * @return false when the given {@link CredentialGenerator} is incompatible with this
   *         authorization module.
   */
  public boolean init(final CredentialGenerator generator) {
    this.generator = generator;
    try {
      this.systemProperties = init();
    } catch (IllegalArgumentException ex) {
      return false;
    }
    return true;
  }

  /**
   *
   * @return A set of extra properties that should be added to Gemfire system properties when not
   *         null.
   */
  public Properties getSystemProperties() {
    return this.systemProperties;
  }

  /**
   * Get the {@link CredentialGenerator} being used by this instance.
   */
  public CredentialGenerator getCredentialGenerator() {
    return this.generator;
  }

  /**
   * Initialize the authorized credential generator.
   *
   * Required to be implemented by concrete classes that implement this abstract class.
   *
   * @return A set of extra properties that should be added to Gemfire system properties when not
   *         null.
   *
   * @throws IllegalArgumentException when the {@link CredentialGenerator} is incompatible with this
   *         authorization module.
   */
  protected abstract Properties init() throws IllegalArgumentException;

  /**
   * The {@link ClassCode} of the particular implementation.
   *
   * @return the {@code ClassCode}
   */
  public abstract ClassCode classCode();

  /**
   * The name of the {@link AccessControl} factory function that should be used as the authorization
   * module on the server side.
   *
   * @return name of the {@code AccessControl} factory function
   */
  public abstract String getAuthorizationCallback();

  /**
   * Get a set of credentials generated using the given index allowed to perform the given
   * {@link OperationCode}s for the given regions.
   *
   * @param opCodes the list of {@link OperationCode}s of the operations requiring authorization;
   *        should not be null
   * @param regionNames list of the region names requiring authorization; a value of null indicates
   *        all regions
   * @param index used to generate multiple such credentials by passing different values for this
   *
   * @return the set of credentials authorized to perform the given operation in the given regions
   */
  public Properties getAllowedCredentials(final OperationCode[] opCodes, final String[] regionNames,
      final int index) {
    int numTries = getNumPrincipalTries(opCodes, regionNames);
    if (numTries <= 0) {
      numTries = 1;
    }

    for (int tries = 0; tries < numTries; tries++) {
      final Principal principal =
          getAllowedPrincipal(opCodes, regionNames, (index + tries) % numTries);
      try {
        return this.generator.getValidCredentials(principal);
      } catch (IllegalArgumentException ex) {
      }
    }
    return null;
  }

  /**
   * Get a set of credentials generated using the given index not allowed to perform the given
   * {@link OperationCode}s for the given regions. The credentials are required to be valid for
   * authentication.
   *
   * @param opCodes the {@link OperationCode}s of the operations requiring authorization failure;
   *        should not be null
   * @param regionNames list of the region names requiring authorization failure; a value of null
   *        indicates all regions
   * @param index used to generate multiple such credentials by passing different values for this
   *
   * @return the set of credentials that are not authorized to perform the given operation in the
   *         given region
   */
  public Properties getDisallowedCredentials(final OperationCode[] opCodes,
      final String[] regionNames, final int index) {
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
      final Principal principal =
          getDisallowedPrincipal(opCodes, regionNames, (index + tries) % numTries);
      try {
        return this.generator.getValidCredentials(principal);
      } catch (IllegalArgumentException ex) {
      }
    }
    return null;
  }

  /**
   * Get the number of tries to be done for obtaining valid credentials for the given operations in
   * the given region. It is required that {@link #getAllowedPrincipal} method returns valid
   * principals for values of {@code index} from 0 through (n-1) where {@code n} is the value
   * returned by this method. It is recommended that the principals so returned be unique for
   * efficiency.
   *
   * This will be used by {@link #getAllowedCredentials} to step through different principals and
   * obtain a set of valid credentials.
   *
   * Required to be implemented by concrete classes that implement this abstract class.
   *
   * @param opCodes the {@link OperationCode}s of the operations requiring authorization
   * @param regionNames list of the region names requiring authorization; a value of null indicates
   *        all regions
   *
   * @return the number of principals allowed to perform the given operation in the given region
   */
  protected abstract int getNumPrincipalTries(final OperationCode[] opCodes,
      final String[] regionNames);

  /**
   * Get a {@link Principal} generated using the given index allowed to perform the given
   * {@link OperationCode}s for the given region.
   *
   * Required to be implemented by concrete classes that implement this abstract class.
   *
   * @param opCodes the {@link OperationCode}s of the operations requiring authorization
   * @param regionNames list of the region names requiring authorization; a value of null indicates
   *        all regions
   * @param index used to generate multiple such principals by passing different values for this
   *
   * @return the {@link Principal} authorized to perform the given operation in the given region
   */
  protected abstract Principal getAllowedPrincipal(final OperationCode[] opCodes,
      final String[] regionNames, final int index);

  /**
   * Get a {@link Principal} generated using the given index not allowed to perform the given
   * {@link OperationCode}s for the given region.
   *
   * Required to be implemented by concrete classes that implement this abstract class.
   *
   * @param opCodes the {@link OperationCode}s of the operations requiring authorization failure
   * @param regionNames list of the region names requiring authorization failure; a value of null
   *        indicates all regions
   * @param index used to generate multiple such principals by passing different values for this
   *
   * @return a {@link Principal} not authorized to perform the given operation in the given region
   */
  protected abstract Principal getDisallowedPrincipal(final OperationCode[] opCodes,
      final String[] regionNames, final int index);

  /**
   * Enumeration for various {@link AuthzCredentialGenerator} implementations.
   *
   * <p>
   * The following schemes are supported as of now:
   * <ul>
   * <li>{@code DummyAuthorization} with {@code DummyAuthenticator}</li>
   * <li>{@code XMLAuthorization} with {@code DummyAuthenticator}</li>
   * <li>{@code XMLAuthorization} with {@code LDAPAuthenticator}</li>
   * <li>{@code XMLAuthorization} with {@code PKCSAuthenticator}</li>
   * <li>{@code XMLAuthorization} when using SSL sockets</li>
   * </ul>
   *
   * <p>
   * To add a new authorization scheme the following needs to be done:
   * <ul>
   * <li>Add implementation for {@link AccessControl}.</li>
   * <li>Choose the authentication schemes that it shall work with from
   * {@link CredentialGenerator.ClassCode}</li>
   * <li>Add a new enumeration value for the scheme in this class. Notice the size of {@code VALUES}
   * array and increase that if it is getting overflowed. Note the methods and fields for existing
   * schemes and add for the new one in a similar manner.</li>
   * <li>Add an implementation for {@link AuthzCredentialGenerator}. Note the
   * {@link AuthzCredentialGenerator#init} method where different authentication schemes can be
   * passed and initialize differently for the authentication schemes that shall be handled.</li>
   * <li>Modify the {@link AuthzCredentialGenerator#create} method to add creation of an instance of
   * the new implementation for the {@code ClassCode} enumeration value.</li>
   * </ul>
   *
   * <p>
   * All dunit tests will automagically start testing the new implementation after this.
   *
   * @since GemFire 5.5
   */
  public static class ClassCode {

    private static byte nextOrdinal = 0;

    private static final byte ID_DUMMY = 1;
    private static final byte ID_XML = 2;

    private static final ClassCode[] VALUES = new ClassCode[10];
    private static final Map CODE_NAME_MAP = new HashMap();

    public static final ClassCode DUMMY =
        new ClassCode(DummyAuthorization.class.getName() + ".create", ID_DUMMY);
    public static final ClassCode XML =
        new ClassCode(XmlAuthorization.class.getName() + ".create", ID_XML);

    /** The name of this class. */
    private final String name;

    /** byte used as ordinal to represent this class */
    private final byte ordinal;

    /**
     * One of the following: ID_DUMMY, ID_LDAP, ID_PKI
     */
    private final byte classType;

    /** Creates a new instance of class code. */
    private ClassCode(final String name, final byte classType) {
      this.name = name;
      this.classType = classType;
      this.ordinal = nextOrdinal++;
      VALUES[this.ordinal] = this;
      CODE_NAME_MAP.put(name, this);
    }

    public boolean isDummy() {
      return this.classType == ID_DUMMY;
    }

    public boolean isXml() {
      return this.classType == ID_XML;
    }

    /**
     * Returns the {@code ClassCode} represented by specified ordinal.
     */
    public static ClassCode fromOrdinal(final byte ordinal) {
      return VALUES[ordinal];
    }

    /**
     * Returns the {@code ClassCode} represented by specified string.
     */
    public static ClassCode parse(final String operationName) {
      return (ClassCode) CODE_NAME_MAP.get(operationName);
    }

    /**
     * Returns all the possible values.
     */
    public static List getAll() {
      final List codes = new ArrayList();
      for (Iterator iter = CODE_NAME_MAP.values().iterator(); iter.hasNext();) {
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
    @Override
    public String toString() {
      return this.name;
    }

    /**
     * Indicates whether other object is same as this one.
     *
     * @return true if other object is same as this one.
     */
    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof ClassCode)) {
        return false;
      }
      final ClassCode other = (ClassCode) obj;
      return other.ordinal == this.ordinal;
    }

    /**
     * Indicates whether other {@code ClassCode} is same as this one.
     *
     * @return true if other {@code ClassCode} is same as this one.
     */
    public boolean equals(final ClassCode opCode) {
      return opCode != null && opCode.ordinal == this.ordinal;
    }

    /**
     * Returns a hash code value for this {@code ClassCode} which is the same as its ordinal.
     *
     * @return the ordinal of this {@code ClassCode}.
     */
    @Override
    public int hashCode() {
      return this.ordinal;
    }
  }
}
