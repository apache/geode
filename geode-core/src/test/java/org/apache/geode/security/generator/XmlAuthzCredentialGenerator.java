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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.security.templates.UsernamePrincipal;
import org.apache.geode.security.templates.XmlAuthorization;
import org.apache.geode.util.test.TestUtil;

public class XmlAuthzCredentialGenerator extends AuthzCredentialGenerator {

  private static final String dummyXml = "authz-dummy.xml";
  private static final String ldapXml = "authz-ldap.xml";
  private static final String pkcsXml = "authz-pkcs.xml";
  private static final String sslXml = "authz-ssl.xml";

  private static final String[] QUERY_REGIONS = {"/Portfolios", "/Positions", "/AuthRegion"};

  public static OperationCode[] READER_OPS =
      {OperationCode.GET, OperationCode.REGISTER_INTEREST, OperationCode.UNREGISTER_INTEREST,
          OperationCode.KEY_SET, OperationCode.CONTAINS_KEY, OperationCode.EXECUTE_FUNCTION};

  public static OperationCode[] WRITER_OPS = {OperationCode.PUT, OperationCode.DESTROY,
      OperationCode.INVALIDATE, OperationCode.REGION_CLEAR};

  public static OperationCode[] QUERY_OPS = {OperationCode.QUERY, OperationCode.EXECUTE_CQ,
      OperationCode.STOP_CQ, OperationCode.CLOSE_CQ};

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

  @Override
  protected Properties init() throws IllegalArgumentException {
    final Properties sysProps = new Properties();
    final String dirName = "/org/apache/geode/security/generator/";

    if (this.generator.classCode().isDummy()) {
      final String xmlFilename =
          TestUtil.getResourcePath(XmlAuthzCredentialGenerator.class, dirName + dummyXml);
      sysProps.setProperty(XmlAuthorization.DOC_URI_PROP_NAME, xmlFilename);

    } else if (this.generator.classCode().isLDAP()) {
      final String xmlFilename =
          TestUtil.getResourcePath(XmlAuthzCredentialGenerator.class, dirName + ldapXml);
      sysProps.setProperty(XmlAuthorization.DOC_URI_PROP_NAME, xmlFilename);

      // } else if (this.generator.classCode().isPKCS()) {
      // sysProps.setProperty(XmlAuthorization.DOC_URI_PROP_NAME, dirName + pkcsXml);
      // }
      // } else if (this.generator.classCode().isSSL()) {
      // sysProps.setProperty(XmlAuthorization.DOC_URI_PROP_NAME, dirName + sslXml);
      // }

    } else {
      throw new IllegalArgumentException("No XML defined for XmlAuthorization module to work with "
          + this.generator.getAuthenticator());
    }
    return sysProps;
  }

  @Override
  public ClassCode classCode() {
    return ClassCode.XML;
  }

  @Override
  public String getAuthorizationCallback() {
    return XmlAuthorization.class.getName() + ".create";
  }

  private Principal getDummyPrincipal(final byte roleType, final int index) {
    final String[] admins = new String[] {"root", "admin", "administrator"};
    final int numReaders = 3;
    final int numWriters = 3;

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

  @Override
  protected Principal getAllowedPrincipal(final OperationCode[] opCodes, final String[] regionNames,
      final int index) {
    if (this.generator.classCode().isDummy()) {
      final byte roleType = getRequiredRole(opCodes, regionNames);
      return getDummyPrincipal(roleType, index);

    } else if (this.generator.classCode().isLDAP()) {
      final byte roleType = getRequiredRole(opCodes, regionNames);
      return getLdapPrincipal(roleType, index);
    }

    return null;
  }

  @Override
  protected Principal getDisallowedPrincipal(final OperationCode[] opCodes,
      final String[] regionNames, final int index) {
    final byte roleType = getRequiredRole(opCodes, regionNames);

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

    if (this.generator.classCode().isDummy()) {
      return getDummyPrincipal(disallowedRoleType, index);

    } else if (this.generator.classCode().isLDAP()) {
      return getLdapPrincipal(disallowedRoleType, index);
    }

    return null;
  }

  @Override
  protected int getNumPrincipalTries(final OperationCode[] opCodes, final String[] regionNames) {
    return 5;
  }

  private Principal getLdapPrincipal(final byte roleType, final int index) {
    final String userPrefix = "gemfire";
    final int[] readerIndices = {3, 4, 5};
    final int[] writerIndices = {6, 7, 8};
    final int[] queryIndices = {9, 10};
    final int[] adminIndices = {1, 2};

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

  private byte getRequiredRole(final OperationCode[] opCodes, final String[] regionNames) {
    byte roleType = ADMIN_ROLE;
    boolean requiresReader = true;
    boolean requiresWriter = true;
    boolean requiresQuery = true;

    for (int opNum = 0; opNum < opCodes.length; opNum++) {
      final OperationCode opCode = opCodes[opNum];
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

    } else if (requiresWriter) {
      roleType = WRITER_ROLE;

    } else if (requiresQuery) {
      if (regionNames != null && regionNames.length > 0) {
        for (int index = 0; index < regionNames.length; index++) {
          final String regionName = XmlAuthorization.normalizeRegionName(regionNames[index]);
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
}
