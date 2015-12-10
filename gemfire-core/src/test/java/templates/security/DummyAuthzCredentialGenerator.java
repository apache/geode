/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package templates.security;

import java.security.Principal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;

public class DummyAuthzCredentialGenerator extends AuthzCredentialGenerator {

  public static final byte READER_ROLE = 1;

  public static final byte WRITER_ROLE = 2;

  public static final byte ADMIN_ROLE = 3;

  private static Set readerOpsSet;

  private static Set writerOpsSet;

  static {

    readerOpsSet = new HashSet();
    for (int index = 0; index < DummyAuthorization.READER_OPS.length; index++) {
      readerOpsSet.add(DummyAuthorization.READER_OPS[index]);
    }
    writerOpsSet = new HashSet();
    for (int index = 0; index < DummyAuthorization.WRITER_OPS.length; index++) {
      writerOpsSet.add(DummyAuthorization.WRITER_OPS[index]);
    }
  }

  public DummyAuthzCredentialGenerator() {
  }

  protected Properties init() throws IllegalArgumentException {

    if (!this.cGen.classCode().isDummy()) {
      throw new IllegalArgumentException(
          "DummyAuthorization module only works with DummyAuthenticator");
    }
    return null;
  }

  public ClassCode classCode() {
    return ClassCode.DUMMY;
  }

  public String getAuthorizationCallback() {
    return "templates.security.DummyAuthorization.create";
  }

  public static byte getRequiredRole(OperationCode[] opCodes) {

    byte roleType = ADMIN_ROLE;
    boolean requiresReader = true;
    boolean requiresWriter = true;

    for (int opNum = 0; opNum < opCodes.length; opNum++) {
      if (requiresReader && !readerOpsSet.contains(opCodes[opNum])) {
        requiresReader = false;
      }
      if (requiresWriter && !writerOpsSet.contains(opCodes[opNum])) {
        requiresWriter = false;
      }
    }
    if (requiresReader) {
      roleType = READER_ROLE;
    }
    else if (requiresWriter) {
      roleType = WRITER_ROLE;
    }
    return roleType;
  }

  private Principal getPrincipal(byte roleType, int index) {

    String[] admins = new String[] { "root", "admin", "administrator" };
    switch (roleType) {
      case READER_ROLE:
        return new UsernamePrincipal("reader" + index);
      case WRITER_ROLE:
        return new UsernamePrincipal("writer" + index);
      default:
        return new UsernamePrincipal(admins[index % admins.length]);
    }
  }

  protected Principal getAllowedPrincipal(OperationCode[] opCodes,
      String[] regionNames, int index) {

    byte roleType = getRequiredRole(opCodes);
    return getPrincipal(roleType, index);
  }

  protected Principal getDisallowedPrincipal(OperationCode[] opCodes,
      String[] regionNames, int index) {

    byte roleType = getRequiredRole(opCodes);
    byte disallowedRoleType;
    switch (roleType) {
      case READER_ROLE:
        disallowedRoleType = WRITER_ROLE;
        break;
      case WRITER_ROLE:
        disallowedRoleType = READER_ROLE;
        break;
      default:
        disallowedRoleType = READER_ROLE;
        break;
    }
    return getPrincipal(disallowedRoleType, index);
  }

  protected int getNumPrincipalTries(OperationCode[] opCodes,
      String[] regionNames) {
    return 5;
  }

}
