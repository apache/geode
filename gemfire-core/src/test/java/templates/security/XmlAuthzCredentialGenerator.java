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
import com.gemstone.gemfire.util.test.TestUtil;

public class XmlAuthzCredentialGenerator extends AuthzCredentialGenerator {

  private static final String dummyXml = "authz-dummy.xml";

  private static final String ldapXml = "authz-ldap.xml";

  private static final String pkcsXml = "authz-pkcs.xml";

  private static final String sslXml = "authz-ssl.xml";

  private static final String[] QUERY_REGIONS = { "/Portfolios", "/Positions",
      "/AuthRegion" };

  public static OperationCode[] READER_OPS = { OperationCode.GET,
      OperationCode.REGISTER_INTEREST, OperationCode.UNREGISTER_INTEREST,
      OperationCode.KEY_SET, OperationCode.CONTAINS_KEY, OperationCode.EXECUTE_FUNCTION };

  public static OperationCode[] WRITER_OPS = { OperationCode.PUT,
      OperationCode.DESTROY, OperationCode.INVALIDATE, OperationCode.REGION_CLEAR };

  public static OperationCode[] QUERY_OPS = { OperationCode.QUERY,
      OperationCode.EXECUTE_CQ, OperationCode.STOP_CQ, OperationCode.CLOSE_CQ };

  private static final byte READER_ROLE = 1;

  private static final byte WRITER_ROLE = 2;

  private static final byte QUERY_ROLE = 3;

  private static final byte ADMIN_ROLE = 4;

  private static Set readerOpsSet;

  private static Set writerOpsSet;

  private static Set queryOpsSet;

  private static Set queryRegionSet;

  static {

    readerOpsSet = new HashSet();
    for (int index = 0; index < READER_OPS.length; index++) {
      readerOpsSet.add(READER_OPS[index]);
    }
    writerOpsSet = new HashSet();
    for (int index = 0; index < WRITER_OPS.length; index++) {
      writerOpsSet.add(WRITER_OPS[index]);
    }
    queryOpsSet = new HashSet();
    for (int index = 0; index < QUERY_OPS.length; index++) {
      queryOpsSet.add(QUERY_OPS[index]);
    }
    queryRegionSet = new HashSet();
    for (int index = 0; index < QUERY_REGIONS.length; index++) {
      queryRegionSet.add(QUERY_REGIONS[index]);
    }
  }

  public XmlAuthzCredentialGenerator() {
  }

  protected Properties init() throws IllegalArgumentException {

    Properties sysProps = new Properties();
    String dirName = "/lib/";
    if (this.cGen.classCode().isDummy()) {
      String xmlFilename = TestUtil.getResourcePath(XmlAuthzCredentialGenerator.class, dirName + dummyXml);
      sysProps.setProperty(XmlAuthorization.DOC_URI_PROP_NAME, xmlFilename);
    }
    else if (this.cGen.classCode().isLDAP()) {
      String xmlFilename = TestUtil.getResourcePath(XmlAuthzCredentialGenerator.class, dirName + ldapXml);
      sysProps.setProperty(XmlAuthorization.DOC_URI_PROP_NAME, xmlFilename);
    }
    // else if (this.cGen.classCode().isPKCS()) {
    // sysProps
    // .setProperty(XmlAuthorization.DOC_URI_PROP_NAME, dirName + pkcsXml);
    // }
    // else if (this.cGen.classCode().isSSL()) {
    // sysProps
    // .setProperty(XmlAuthorization.DOC_URI_PROP_NAME, dirName + sslXml);
    // }
    else {
      throw new IllegalArgumentException(
          "No XML defined for XmlAuthorization module to work with "
              + this.cGen.getAuthenticator());
    }
    return sysProps;
  }

  public ClassCode classCode() {
    return ClassCode.XML;
  }

  public String getAuthorizationCallback() {
    return "templates.security.XmlAuthorization.create";
  }

  private Principal getDummyPrincipal(byte roleType, int index) {

    String[] admins = new String[] { "root", "admin", "administrator" };
    int numReaders = 3;
    int numWriters = 3;

    switch (roleType) {
      case READER_ROLE:
        return new UsernamePrincipal("reader" + (index % numReaders));
      case WRITER_ROLE:
        return new UsernamePrincipal("writer" + (index % numWriters));
      case QUERY_ROLE:
        return new UsernamePrincipal("reader" + ((index % 2) + 3));
      default:
        return new UsernamePrincipal(admins[index % admins.length]);
    }
  }

  private Principal getLdapPrincipal(byte roleType, int index) {

    final String userPrefix = "gemfire";
    final int[] readerIndices = { 3, 4, 5 };
    final int[] writerIndices = { 6, 7, 8 };
    final int[] queryIndices = { 9, 10 };
    final int[] adminIndices = { 1, 2 };

    switch (roleType) {
      case READER_ROLE:
        int readerIndex = readerIndices[index % readerIndices.length];
        return new UsernamePrincipal(userPrefix + readerIndex);
      case WRITER_ROLE:
        int writerIndex = writerIndices[index % writerIndices.length];
        return new UsernamePrincipal(userPrefix + writerIndex);
      case QUERY_ROLE:
        int queryIndex = queryIndices[index % queryIndices.length];
        return new UsernamePrincipal(userPrefix + queryIndex);
      default:
        int adminIndex = adminIndices[index % adminIndices.length];
        return new UsernamePrincipal(userPrefix + adminIndex);
    }
  }

  private byte getRequiredRole(OperationCode[] opCodes, String[] regionNames) {

    byte roleType = ADMIN_ROLE;
    boolean requiresReader = true;
    boolean requiresWriter = true;
    boolean requiresQuery = true;

    for (int opNum = 0; opNum < opCodes.length; opNum++) {
      OperationCode opCode = opCodes[opNum];
      if (requiresReader && !readerOpsSet.contains(opCode)) {
        requiresReader = false;
      }
      if (requiresWriter && !writerOpsSet.contains(opCode)) {
        requiresWriter = false;
      }
      if (requiresQuery && !queryOpsSet.contains(opCode)) {
        requiresQuery = false;
      }
    }
    if (requiresReader) {
      roleType = READER_ROLE;
    }
    else if (requiresWriter) {
      roleType = WRITER_ROLE;
    }
    else if (requiresQuery) {
      if (regionNames != null && regionNames.length > 0) {
        for (int index = 0; index < regionNames.length; index++) {
          String regionName = XmlAuthorization
              .normalizeRegionName(regionNames[index]);
          if (requiresQuery && !queryRegionSet.contains(regionName)) {
            requiresQuery = false;
            break;
          }
        }
        if (requiresQuery) {
          roleType = QUERY_ROLE;
        }
      }
    }
    return roleType;
  }

  protected Principal getAllowedPrincipal(OperationCode[] opCodes,
      String[] regionNames, int index) {

    if (this.cGen.classCode().isDummy()) {
      byte roleType = getRequiredRole(opCodes, regionNames);
      return getDummyPrincipal(roleType, index);
    }
    else if (this.cGen.classCode().isLDAP()) {
      byte roleType = getRequiredRole(opCodes, regionNames);
      return getLdapPrincipal(roleType, index);
    }
    return null;
  }

  protected Principal getDisallowedPrincipal(OperationCode[] opCodes,
      String[] regionNames, int index) {

    byte roleType = getRequiredRole(opCodes, regionNames);
    byte disallowedRoleType = READER_ROLE;
    switch (roleType) {
      case READER_ROLE:
        disallowedRoleType = WRITER_ROLE;
        break;
      case WRITER_ROLE:
        disallowedRoleType = READER_ROLE;
        break;
      case QUERY_ROLE:
        disallowedRoleType = READER_ROLE;
        break;
      case ADMIN_ROLE:
        disallowedRoleType = READER_ROLE;
        break;
    }
    if (this.cGen.classCode().isDummy()) {
      return getDummyPrincipal(disallowedRoleType, index);
    }
    else if (this.cGen.classCode().isLDAP()) {
      return getLdapPrincipal(disallowedRoleType, index);
    }
    return null;
  }

  protected int getNumPrincipalTries(OperationCode[] opCodes,
      String[] regionNames) {
    return 5;
  }

}
