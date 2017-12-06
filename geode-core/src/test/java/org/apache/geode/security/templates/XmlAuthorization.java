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
package org.apache.geode.security.templates;

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.cache.operations.OperationContext;
import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.cache.operations.QueryOperationContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AccessControl;
import org.apache.geode.security.NotAuthorizedException;

/**
 * An implementation of the {@link AccessControl} interface that allows authorization using the
 * permissions as specified in the given XML file.
 *
 * The format of the XML file is specified in <a href="authz5_5.dtd"/>. It implements a role-based
 * authorization at the operation level for each region. Each principal name may be associated with
 * a set of roles. The name of the principal is obtained using the {@link Principal#getName()}
 * method and no other information of the principal is utilized. Each role can be provided
 * permissions to execute operations for each region.
 *
 * The top-level element in the XML is "acl" tag that contains the "role" and "permission" tags. The
 * "role" tag contains the list of users that have been given that role. The name of the role is
 * specified in the "role" attribute and the users are contained in the "user" tags insided the
 * "role" tag.
 *
 * The "permissions" tag contains the list of operations allowed for a particular region. The role
 * name is specified as the "role" attribute, the list of comma separated region names as the
 * optional "regions" attribute and the operation names are contained in the "operation" tags inside
 * the "permissions" tag. The allowed operation names are: GET, PUT, PUTALL, DESTROY,
 * REGISTER_INTEREST, UNREGISTER_INTEREST, CONTAINS_KEY, KEY_SET, QUERY, EXECUTE_CQ, STOP_CQ,
 * CLOSE_CQ, REGION_CLEAR, REGION_CREATE, REGION_DESTROY. These correspond to the operations in the
 * {@link OperationCode} enumeration with the same name.
 *
 * When no region name is specified then the operation is allowed for all regions in the cache. Any
 * permissions specified for regions using the "regions" attribute override these permissions. This
 * allows users to provide generic permissions without any region name, and override for specific
 * regions specified using the "regions" attribute. A cache-level operation (e.g.
 * {@link OperationCode#REGION_DESTROY}) specified for a particular region is ignored i.e. the
 * cache-level operations are only applicable when no region name is specified. A
 * {@link OperationCode#QUERY} operation is permitted when either the {@code QUERY} permission is
 * provided at the cache-level for the user or when {@code QUERY} permission is provided for all the
 * regions that are part of the query string.
 *
 * Any roles specified in the "user" tag that do not have a specified permission set using the
 * "permission" tags are ignored. When no {@link Principal} is associated with the current
 * connection, then empty user name is used to search for the roles so an empty user name can be
 * used to specify roles of unauthenticated clients (i.e. {@code Everyone}).
 *
 * This sample implementation is useful only for pre-operation checks and should not be used for
 * post-operation authorization since it does nothing useful for post-operation case.
 *
 * @since GemFire 5.5
 */
public class XmlAuthorization implements AccessControl {

  public static final String DOC_URI_PROP_NAME = "security-authz-xml-uri";

  private static final Object sync = new Object();
  private static final String EMPTY_VALUE = "";

  private static final String TAG_ROLE = "role";
  private static final String TAG_USER = "user";
  private static final String TAG_PERMS = "permission";
  private static final String TAG_OP = "operation";

  private static final String ATTR_ROLENAME = "name";
  private static final String ATTR_ROLE = "role";
  private static final String ATTR_REGIONS = "regions";
  private static final String ATTR_FUNCTION_IDS = "functionIds";
  private static final String ATTR_FUNCTION_OPTIMIZE_FOR_WRITE = "optimizeForWrite";
  private static final String ATTR_FUNCTION_KEY_SET = "keySet";

  private static String currentDocUri = null;
  private static Map<String, HashSet<String>> userRoles = null;
  private static Map<String, Map<String, Map<OperationCode, FunctionSecurityPrmsHolder>>> rolePermissions =
      null;
  private static NotAuthorizedException xmlLoadFailure = null;

  private final Map<String, Map<OperationCode, FunctionSecurityPrmsHolder>> allowedOps;

  protected LogWriter systemLogWriter;
  protected LogWriter securityLogWriter;

  /**
   * Public static factory method to create an instance of {@code XmlAuthorization}. The fully
   * qualified name of the class
   * ({@code org.apache.geode.security.templates.XmlAuthorization.create}) should be mentioned as
   * the {@code security-client-accessor} system property to enable pre-operation authorization
   * checks as implemented in this class.
   *
   * @return an object of {@code XmlAuthorization} class
   */
  public static AccessControl create() {
    return new XmlAuthorization();
  }

  /**
   * Clear all the statically cached information.
   */
  public static void clear() {
    XmlAuthorization.currentDocUri = null;
    if (XmlAuthorization.userRoles != null) {
      XmlAuthorization.userRoles.clear();
      XmlAuthorization.userRoles = null;
    }
    if (XmlAuthorization.rolePermissions != null) {
      XmlAuthorization.rolePermissions.clear();
      XmlAuthorization.rolePermissions = null;
    }
    XmlAuthorization.xmlLoadFailure = null;
  }

  /**
   * Change the region name to a standard format having single '/' as separator and starting with a
   * '/' as in standard POSIX paths
   */
  public static String normalizeRegionName(final String regionName) {
    if (regionName == null || regionName.length() == 0) {
      return EMPTY_VALUE;
    }

    char[] resultName = new char[regionName.length() + 1];
    boolean changed = false;
    boolean isPrevCharSlash = false;
    int startIndex;

    if (regionName.charAt(0) != '/') {
      changed = true;
      startIndex = 0;
    } else {
      isPrevCharSlash = true;
      startIndex = 1;
    }

    resultName[0] = '/';
    int resultLength = 1;

    // Replace all more than one '/'s with a single '/'
    for (int index = startIndex; index < regionName.length(); ++index) {
      char currChar = regionName.charAt(index);
      if (currChar == '/') {
        if (isPrevCharSlash) {
          changed = true;
          continue;
        }
        isPrevCharSlash = true;
      } else {
        isPrevCharSlash = false;
      }
      resultName[resultLength++] = currChar;
    }

    // Remove any trailing slash
    if (resultName[resultLength - 1] == '/') {
      --resultLength;
      changed = true;
    }

    if (changed) {
      return new String(resultName, 0, resultLength);
    } else {
      return regionName;
    }
  }

  private XmlAuthorization() {
    this.allowedOps = new HashMap<String, Map<OperationCode, FunctionSecurityPrmsHolder>>();
    this.systemLogWriter = null;
    this.securityLogWriter = null;
  }

  /**
   * Initialize the {@code XmlAuthorization} callback for a client having the given principal.
   *
   * This method caches the full XML authorization file the first time it is invoked and caches all
   * the permissions for the provided {@code principal} to speed up lookup the
   * {@code authorizeOperation} calls. The permissions for the principal are maintained as a
   * {@link Map} of region name to the {@link HashSet} of operations allowed for that region. A
   * global entry with region name as empty string is also made for permissions provided for all the
   * regions.
   *
   * @param principal the principal associated with the authenticated client
   * @param cache reference to the cache object
   * @param remoteMember the {@link DistributedMember} object for the remote authenticated client
   *
   * @throws NotAuthorizedException if some exception condition happens during the initialization
   *         while reading the XML; in such a case all subsequent client operations will throw
   *         {@code NotAuthorizedException}
   */
  @Override
  public void init(final Principal principal, final DistributedMember remoteMember,
      final Cache cache) throws NotAuthorizedException {
    synchronized (sync) {
      XmlAuthorization.init(cache);
    }

    this.systemLogWriter = cache.getLogger();
    this.securityLogWriter = cache.getSecurityLogger();

    String name;
    if (principal != null) {
      name = principal.getName();
    } else {
      name = EMPTY_VALUE;
    }

    HashSet<String> roles = XmlAuthorization.userRoles.get(name);
    if (roles != null) {
      for (String roleName : roles) {
        Map<String, Map<OperationCode, FunctionSecurityPrmsHolder>> regionOperationMap =
            XmlAuthorization.rolePermissions.get(roleName);
        if (regionOperationMap != null) {
          for (Map.Entry<String, Map<OperationCode, FunctionSecurityPrmsHolder>> regionEntry : regionOperationMap
              .entrySet()) {
            String regionName = regionEntry.getKey();
            Map<OperationCode, FunctionSecurityPrmsHolder> regionOperations =
                this.allowedOps.get(regionName);
            if (regionOperations == null) {
              regionOperations = new HashMap<OperationCode, FunctionSecurityPrmsHolder>();
              this.allowedOps.put(regionName, regionOperations);
            }
            regionOperations.putAll(regionEntry.getValue());
          }
        }
      }
    }
  }

  /**
   * Return true if the given operation is allowed for the cache/region.
   *
   * This looks up the cached permissions of the principal in the map for the provided region name.
   * If none are found then the global permissions with empty region name are looked up. The
   * operation is allowed if it is found this permission list.
   *
   * @param regionName When null then it indicates a cache-level operation, else the name of the
   *        region for the operation.
   * @param context the data required by the operation
   *
   * @return true if the operation is authorized and false otherwise
   */
  @Override
  public boolean authorizeOperation(String regionName, final OperationContext context) {
    Map<OperationCode, FunctionSecurityPrmsHolder> operationMap;

    // Check GET permissions for updates from server to client
    if (context.isClientUpdate()) {
      operationMap = this.allowedOps.get(regionName);
      if (operationMap == null && regionName.length() > 0) {
        operationMap = this.allowedOps.get(EMPTY_VALUE);
      }
      if (operationMap != null) {
        return operationMap.containsKey(OperationCode.GET);
      }
      return false;
    }

    OperationCode opCode = context.getOperationCode();
    if (opCode.isQuery() || opCode.isExecuteCQ() || opCode.isCloseCQ() || opCode.isStopCQ()) {
      // First check if cache-level permission has been provided
      operationMap = this.allowedOps.get(EMPTY_VALUE);
      boolean globalPermission = (operationMap != null && operationMap.containsKey(opCode));
      Set<String> regionNames = ((QueryOperationContext) context).getRegionNames();
      if (regionNames == null || regionNames.size() == 0) {
        return globalPermission;
      }

      for (String r : regionNames) {
        regionName = normalizeRegionName(r);
        operationMap = this.allowedOps.get(regionName);
        if (operationMap == null) {
          if (!globalPermission) {
            return false;
          }
        } else if (!operationMap.containsKey(opCode)) {
          return false;
        }
      }
      return true;
    }

    final String normalizedRegionName = normalizeRegionName(regionName);
    operationMap = this.allowedOps.get(normalizedRegionName);
    if (operationMap == null && normalizedRegionName.length() > 0) {
      operationMap = this.allowedOps.get(EMPTY_VALUE);
    }
    if (operationMap != null) {
      if (context.getOperationCode() != OperationCode.EXECUTE_FUNCTION) {
        return operationMap.containsKey(context.getOperationCode());

      } else {
        if (!operationMap.containsKey(context.getOperationCode())) {
          return false;

        } else {
          if (!context.isPostOperation()) {
            FunctionSecurityPrmsHolder functionParameter =
                operationMap.get(context.getOperationCode());
            ExecuteFunctionOperationContext functionContext =
                (ExecuteFunctionOperationContext) context;
            // OnRegion execution
            if (functionContext.getRegionName() != null) {
              if (functionParameter.isOptimizeForWrite() != null && functionParameter
                  .isOptimizeForWrite().booleanValue() != functionContext.isOptimizeForWrite()) {
                return false;
              }
              if (functionParameter.getFunctionIds() != null && !functionParameter.getFunctionIds()
                  .contains(functionContext.getFunctionId())) {
                return false;
              }
              if (functionParameter.getKeySet() != null && functionContext.getKeySet() != null) {
                if (functionContext.getKeySet().containsAll(functionParameter.getKeySet())) {
                  return false;
                }
              }
              return true;

            } else {// On Server execution
              if (functionParameter.getFunctionIds() != null && !functionParameter.getFunctionIds()
                  .contains(functionContext.getFunctionId())) {
                return false;
              }
              return true;
            }

          } else {
            ExecuteFunctionOperationContext functionContext =
                (ExecuteFunctionOperationContext) context;
            FunctionSecurityPrmsHolder functionParameter =
                operationMap.get(context.getOperationCode());
            if (functionContext.getRegionName() != null) {
              if (functionContext.getResult() instanceof ArrayList
                  && functionParameter.getKeySet() != null) {
                ArrayList<String> resultList = (ArrayList) functionContext.getResult();
                Set<String> nonAllowedKeys = functionParameter.getKeySet();
                if (resultList.containsAll(nonAllowedKeys)) {
                  return false;
                }
              }
              return true;

            } else {
              ArrayList<String> resultList = (ArrayList) functionContext.getResult();
              final String inSecureItem = "Insecure item";
              if (resultList.contains(inSecureItem)) {
                return false;
              }
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  /**
   * Clears the cached information for this principal.
   */
  @Override
  public void close() {
    this.allowedOps.clear();
  }

  /** Get the attribute value for a given attribute name of a node. */
  private static String getAttributeValue(final Node node, final String attrName) {
    NamedNodeMap attrMap = node.getAttributes();
    Node attrNode;
    if (attrMap != null && (attrNode = attrMap.getNamedItem(attrName)) != null) {
      return ((Attr) attrNode).getValue();
    }
    return EMPTY_VALUE;
  }

  /** Get the string contained in the first text child of the node. */
  private static String getNodeValue(final Node node) {
    NodeList childNodes = node.getChildNodes();
    for (int index = 0; index < childNodes.getLength(); index++) {
      Node childNode = childNodes.item(index);
      if (childNode.getNodeType() == Node.TEXT_NODE) {
        return childNode.getNodeValue();
      }
    }
    return EMPTY_VALUE;
  }

  /**
   * Cache authorization information for all users statically. This method is not thread-safe and is
   * should either be invoked only once, or the caller should take the appropriate locks.
   *
   * @param cache reference to the cache object for the distributed system
   */
  private static void init(final Cache cache) throws NotAuthorizedException {
    final LogWriter systemLogWriter = cache.getLogger();
    final String xmlDocumentUri =
        (String) cache.getDistributedSystem().getSecurityProperties().get(DOC_URI_PROP_NAME);

    try {
      if (xmlDocumentUri == null) {
        throw new NotAuthorizedException(
            "No ACL file defined using tag [" + DOC_URI_PROP_NAME + "] in system properties");
      }
      if (xmlDocumentUri.equals(XmlAuthorization.currentDocUri)) {
        if (XmlAuthorization.xmlLoadFailure != null) {
          throw XmlAuthorization.xmlLoadFailure;
        }
        return;
      }

      final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setIgnoringComments(true);
      factory.setIgnoringElementContentWhitespace(true);
      factory.setValidating(true);

      final DocumentBuilder builder = factory.newDocumentBuilder();
      final XmlErrorHandler errorHandler = new XmlErrorHandler(systemLogWriter, xmlDocumentUri);
      builder.setErrorHandler(errorHandler);
      builder.setEntityResolver(new AuthzDtdResolver());

      final Document xmlDocument = builder.parse(xmlDocumentUri);

      XmlAuthorization.userRoles = new HashMap<String, HashSet<String>>();
      XmlAuthorization.rolePermissions =
          new HashMap<String, Map<String, Map<OperationCode, FunctionSecurityPrmsHolder>>>();

      final NodeList roleUserNodes = xmlDocument.getElementsByTagName(TAG_ROLE);

      for (int roleIndex = 0; roleIndex < roleUserNodes.getLength(); roleIndex++) {
        final Node roleUserNode = roleUserNodes.item(roleIndex);
        final String roleName = getAttributeValue(roleUserNode, ATTR_ROLENAME);
        final NodeList userNodes = roleUserNode.getChildNodes();

        for (int userIndex = 0; userIndex < userNodes.getLength(); userIndex++) {
          final Node userNode = userNodes.item(userIndex);

          if (TAG_USER.equals(userNode.getNodeName())) {
            final String userName = getNodeValue(userNode);
            HashSet<String> userRoleSet = XmlAuthorization.userRoles.get(userName);
            if (userRoleSet == null) {
              userRoleSet = new HashSet<String>();
              XmlAuthorization.userRoles.put(userName, userRoleSet);
            }
            userRoleSet.add(roleName);

          } else {
            throw new SAXParseException(
                "Unknown tag [" + userNode.getNodeName() + "] as child of tag [" + TAG_ROLE + ']',
                null);
          }
        }
      }

      final NodeList rolePermissionNodes = xmlDocument.getElementsByTagName(TAG_PERMS);

      for (int permIndex = 0; permIndex < rolePermissionNodes.getLength(); permIndex++) {
        final Node rolePermissionNode = rolePermissionNodes.item(permIndex);
        final String roleName = getAttributeValue(rolePermissionNode, ATTR_ROLE);
        Map<String, Map<OperationCode, FunctionSecurityPrmsHolder>> regionOperationMap =
            XmlAuthorization.rolePermissions.get(roleName);

        if (regionOperationMap == null) {
          regionOperationMap =
              new HashMap<String, Map<OperationCode, FunctionSecurityPrmsHolder>>();
          XmlAuthorization.rolePermissions.put(roleName, regionOperationMap);
        }

        final NodeList operationNodes = rolePermissionNode.getChildNodes();
        final HashMap<OperationCode, FunctionSecurityPrmsHolder> operationMap =
            new HashMap<OperationCode, FunctionSecurityPrmsHolder>();

        for (int opIndex = 0; opIndex < operationNodes.getLength(); opIndex++) {
          final Node operationNode = operationNodes.item(opIndex);

          if (TAG_OP.equals(operationNode.getNodeName())) {
            final String operationName = getNodeValue(operationNode);
            final OperationCode code = OperationCode.valueOf(operationName);

            if (code == null) {
              throw new SAXParseException("Unknown operation [" + operationName + ']', null);
            }

            if (code != OperationCode.EXECUTE_FUNCTION) {
              operationMap.put(code, null);

            } else {
              final String optimizeForWrite =
                  getAttributeValue(operationNode, ATTR_FUNCTION_OPTIMIZE_FOR_WRITE);
              final String functionAttr = getAttributeValue(operationNode, ATTR_FUNCTION_IDS);
              final String keysAttr = getAttributeValue(operationNode, ATTR_FUNCTION_KEY_SET);

              Boolean isOptimizeForWrite;
              HashSet<String> functionIds;
              HashSet<String> keySet;

              if (optimizeForWrite == null || optimizeForWrite.length() == 0) {
                isOptimizeForWrite = null;
              } else {
                isOptimizeForWrite = Boolean.parseBoolean(optimizeForWrite);
              }

              if (functionAttr == null || functionAttr.length() == 0) {
                functionIds = null;
              } else {
                final String[] functionArray = functionAttr.split(",");
                functionIds = new HashSet<String>();
                for (int strIndex = 0; strIndex < functionArray.length; ++strIndex) {
                  functionIds.add((functionArray[strIndex]));
                }
              }

              if (keysAttr == null || keysAttr.length() == 0) {
                keySet = null;
              } else {
                final String[] keySetArray = keysAttr.split(",");
                keySet = new HashSet<String>();
                for (int strIndex = 0; strIndex < keySetArray.length; ++strIndex) {
                  keySet.add((keySetArray[strIndex]));
                }
              }

              final FunctionSecurityPrmsHolder functionContext =
                  new FunctionSecurityPrmsHolder(isOptimizeForWrite, functionIds, keySet);
              operationMap.put(code, functionContext);
            }

          } else {
            throw new SAXParseException("Unknown tag [" + operationNode.getNodeName()
                + "] as child of tag [" + TAG_PERMS + ']', null);
          }
        }

        final String regionNames = getAttributeValue(rolePermissionNode, ATTR_REGIONS);
        if (regionNames == null || regionNames.length() == 0) {
          regionOperationMap.put(EMPTY_VALUE, operationMap);
        } else {
          final String[] regionNamesSplit = regionNames.split(",");
          for (int strIndex = 0; strIndex < regionNamesSplit.length; ++strIndex) {
            regionOperationMap.put(normalizeRegionName(regionNamesSplit[strIndex]), operationMap);
          }
        }
      }
      XmlAuthorization.currentDocUri = xmlDocumentUri;

    } catch (Exception ex) {
      String message;
      if (ex instanceof NotAuthorizedException) {
        message = ex.getMessage();
      } else {
        message = ex.getClass().getName() + ": " + ex.getMessage();
      }
      systemLogWriter.warning("XmlAuthorization.init: " + message);
      XmlAuthorization.xmlLoadFailure = new NotAuthorizedException(message, ex);
      throw XmlAuthorization.xmlLoadFailure;
    }
  }

  private static class AuthzDtdResolver implements EntityResolver {
    final Pattern authzPattern = Pattern.compile("authz.*\\.dtd");

    @Override
    public InputSource resolveEntity(final String publicId, final String systemId)
        throws SAXException, IOException {
      try {
        final Matcher matcher = authzPattern.matcher(systemId);
        if (matcher.find()) {
          final String dtdName = matcher.group(0);
          final InputStream stream = XmlAuthorization.class.getResourceAsStream(dtdName);
          return new InputSource(stream);
        }

      } catch (Exception e) {
        // do nothing, use the default resolver
      }

      return null;
    }
  }
}
