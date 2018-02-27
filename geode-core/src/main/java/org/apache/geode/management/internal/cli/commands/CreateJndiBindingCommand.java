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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.cli.result.ResultBuilder.buildResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.internal.datasource.ConfigProperty;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.exceptions.EntityExistsException;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateJndiBindingFunction;
import org.apache.geode.management.internal.cli.functions.JndiBindingConfiguration;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateJndiBindingCommand implements GfshCommand {
  private static final Logger logger = LogService.getLogger();

  static final String CREATE_JNDIBINDING = "create jndi-binding";
  static final String CREATE_JNDIBINDING__HELP =
      "Create a jndi binding that holds the configuration for the XA datasource.";
  static final String BLOCKING_TIMEOUT_SECONDS = "blocking-timeout-seconds";
  static final String BLOCKING_TIMEOUT_SECONDS__HELP =
      "This element specifies the maximum time to block while waiting for a connection before throwing an exception.";
  static final String CONNECTION_POOLED_DATASOURCE_CLASS = "conn-pooled-datasource-class";
  static final String CONNECTION_POOLED_DATASOURCE_CLASS__HELP =
      "This is the fully qualified name of the connection pool implementation to hold XS datasource connections.";
  static final String CONNECTION_URL = "connection-url";
  static final String CONNECTION_URL__HELP =
      "This is the JDBC driver connection URL string, for example, jdbc:hsqldb:hsql://localhost:1701.";
  static final String IDLE_TIMEOUT_SECONDS = "idle-timeout-seconds";
  static final String IDLE_TIMEOUT_SECONDS__HELP =
      "This element specifies the time a connection may be idle before being closed.";
  static final String INIT_POOL_SIZE = "init-pool-size";
  static final String INIT_POOL_SIZE__HELP =
      "This element specifies the initial number of connections the pool should hold.";
  static final String JDBC_DRIVER_CLASS = "jdbc-driver-class";
  static final String JDBC_DRIVER_CLASS__HELP =
      "This is the fully qualified name of the JDBC driver class.";
  static final String JNDI_NAME = "name";
  static final String JNDI_NAME__HELP = "Name of the binding to be created.";
  static final String LOGIN_TIMEOUT_SECONDS = "login-timeout-seconds";
  static final String LOGIN_TIMEOUT_SECONDS__HELP =
      "Time in seconds after which the client thread for retrieving connection will experience timeout.";
  static final String MANAGED_CONN_FACTORY_CLASS = "managed-conn-factory-class";
  static final String MANAGED_CONN_FACTORY_CLASS__HELP =
      "This is the fully qualified name of the connection factory implementation.";
  static final String MAX_POOL_SIZE = "max-pool-size";
  static final String MAX_POOL_SIZE__HELP =
      "This element specifies the maximum number of connections for a pool. No more than the max-pool-size number of connections will be created in a pool.";
  static final String PASSWORD = "password";
  static final String PASSWORD__HELP =
      "This element specifies the default password used when creating a new connection.";
  static final String TRANSACTION_TYPE = "transaction-type";
  static final String TRANSACTION_TYPE__HELP = "Type of the transaction.";
  static final String TYPE = "type";
  static final String TYPE__HELP =
      "Type of the XA datasource. Type of region to create. The following types are pre-defined by the product: MANAGED, SIMPLE, POOLED, XAPOOLED.";
  static final String USERNAME = "username";
  static final String USERNAME__HELP =
      "This element specifies the default username used when creating a new connection.";
  static final String XA_DATASOURCE_CLASS = "xa-datasource-class";
  static final String XA_DATASOURCE_CLASS__HELP =
      "The fully qualified name of the javax.sql.XADataSource implementation class.";
  static final String IFNOTEXISTS__HELP =
      "Skip the create operation when a Jndi binding with the same name already exists. The default is to overwrite the entry (false).";
  static final String DATASOURCE_CONFIG_PROPERTIES = "datasource-config-properties";
  static final String DATASOURCE_CONFIG_PROPERTIES_HELP =
      "Properties for the custom XSDataSource driver. Append json string containing (name, type, value) to set any property. Eg: --datasource-config-properties={'name':'name1','type':'type1','value':'value1'},{'name':'name2','type':'type2','value':'value2'}";

  @CliCommand(value = CREATE_JNDIBINDING, help = CREATE_JNDIBINDING__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result createJDNIBinding(
      @CliOption(key = BLOCKING_TIMEOUT_SECONDS,
          help = BLOCKING_TIMEOUT_SECONDS__HELP) Integer blockingTimeout,
      @CliOption(key = CONNECTION_POOLED_DATASOURCE_CLASS,
          help = CONNECTION_POOLED_DATASOURCE_CLASS__HELP) String connectionPooledDatasource,
      @CliOption(key = CONNECTION_URL, mandatory = true,
          help = CONNECTION_URL__HELP) String connectionUrl,
      @CliOption(key = IDLE_TIMEOUT_SECONDS, help = IDLE_TIMEOUT_SECONDS__HELP) Integer idleTimeout,
      @CliOption(key = INIT_POOL_SIZE, help = INIT_POOL_SIZE__HELP) Integer initPoolSize,
      @CliOption(key = JDBC_DRIVER_CLASS, mandatory = true,
          help = JDBC_DRIVER_CLASS__HELP) String jdbcDriver,
      @CliOption(key = JNDI_NAME, mandatory = true, help = JNDI_NAME__HELP) String jndiName,
      @CliOption(key = LOGIN_TIMEOUT_SECONDS,
          help = LOGIN_TIMEOUT_SECONDS__HELP) Integer loginTimeout,
      @CliOption(key = MANAGED_CONN_FACTORY_CLASS,
          help = MANAGED_CONN_FACTORY_CLASS__HELP) String managedConnFactory,
      @CliOption(key = MAX_POOL_SIZE, help = MAX_POOL_SIZE__HELP) Integer maxPoolSize,
      @CliOption(key = PASSWORD, help = PASSWORD__HELP) String password,
      @CliOption(key = TRANSACTION_TYPE, help = TRANSACTION_TYPE__HELP) String transactionType,
      @CliOption(key = TYPE, mandatory = true,
          help = TYPE__HELP) JndiBindingConfiguration.DATASOURCE_TYPE type,
      @CliOption(key = USERNAME, help = USERNAME__HELP) String username,
      @CliOption(key = XA_DATASOURCE_CLASS, help = XA_DATASOURCE_CLASS__HELP) String xaDataSource,
      @CliOption(key = CliStrings.IFNOTEXISTS, help = IFNOTEXISTS__HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean ifNotExists,
      @CliOption(key = DATASOURCE_CONFIG_PROPERTIES, optionContext = "splittingRegex=,(?![^{]*\\})",
          help = DATASOURCE_CONFIG_PROPERTIES_HELP) ConfigProperty[] dsConfigProperties)
      throws IOException, SAXException, ParserConfigurationException, TransformerException {

    JndiBindingConfiguration configuration = new JndiBindingConfiguration();
    configuration.setBlockingTimeout(blockingTimeout);
    configuration.setConnectionPoolDatasource(connectionPooledDatasource);
    configuration.setConnectionUrl(connectionUrl);
    configuration.setIdleTimeout(idleTimeout);
    configuration.setInitPoolSize(initPoolSize);
    configuration.setJdbcDriver(jdbcDriver);
    configuration.setJndiName(jndiName);
    configuration.setLoginTimeout(loginTimeout);
    configuration.setManagedConnFactory(managedConnFactory);
    configuration.setMaxPoolSize(maxPoolSize);
    configuration.setPassword(password);
    configuration.setTransactionType(transactionType);
    configuration.setType(type);
    configuration.setUsername(username);
    configuration.setXaDatasource(xaDataSource);
    if (dsConfigProperties != null && dsConfigProperties.length > 0)
      configuration.setDatasourceConfigurations(Arrays.asList(dsConfigProperties));

    Result result;
    boolean persisted = false;
    ClusterConfigurationService service = getSharedConfiguration();

    if (service != null) {
      if (isBindingAlreadyExists(jndiName))
        throw new EntityExistsException(
            CliStrings.format("Jndi binding with jndi-name \"{0}\" already exists.", jndiName),
            ifNotExists);
      updateXml(configuration);
      persisted = true;
    }

    Set<DistributedMember> targetMembers = findMembers(null, null);
    if (targetMembers.size() > 0) {
      List<CliFunctionResult> jndiCreationResult = executeAndGetFunctionResult(
          new CreateJndiBindingFunction(), configuration, targetMembers);
      result = buildResult(jndiCreationResult);
    } else {
      if (persisted)
        result = ResultBuilder.createInfoResult(CliStrings.format(
            "No members found. Cluster configuration is updated with jndi-binding \"{0}\".",
            jndiName));
      else
        result = ResultBuilder.createInfoResult("No members found.");
    }

    result.setCommandPersisted(persisted);

    return result;
  }

  boolean isBindingAlreadyExists(String jndiName)
      throws IOException, SAXException, ParserConfigurationException {

    Configuration config = getSharedConfiguration().getConfiguration("cluster");

    Document document = XmlUtils.createDocumentFromXml(config.getCacheXmlContent());
    NodeList jndiBindings = document.getElementsByTagName("jndi-binding");

    if (jndiBindings == null || jndiBindings.getLength() == 0) {
      return false;
    } else {
      for (int i = 0; i < jndiBindings.getLength(); i++) {
        Element eachBinding = (Element) jndiBindings.item(i);
        if (eachBinding.getAttribute("jndi-name").equals(jndiName))
          return true;
      }
    }
    return false;
  }

  void updateXml(JndiBindingConfiguration configuration)
      throws TransformerException, IOException, SAXException, ParserConfigurationException {
    // cluster group config should always be present
    Configuration config = getSharedConfiguration().getConfiguration("cluster");

    Document document = XmlUtils.createDocumentFromXml(config.getCacheXmlContent());
    Node cacheNode = document.getElementsByTagName("cache").item(0);

    NodeList jndiBindingsNode = document.getElementsByTagName("jndi-bindings");

    if (jndiBindingsNode == null || jndiBindingsNode.getLength() == 0) {
      Element jndiBindings = document.createElement("jndi-bindings");
      cacheNode.appendChild(jndiBindings);
    }

    Element jndiBinding = document.createElement("jndi-binding");
    jndiBindingsNode.item(0).appendChild(jndiBinding);

    for (Object key : configuration.getParamsAsMap().keySet()) {
      if (configuration.getParamsAsMap().get(key) != null)
        jndiBinding.setAttribute(key.toString(),
            configuration.getParamsAsMap().get(key).toString());
    }

    for (ConfigProperty configProperty : configuration.getDatasourceConfigurations()) {
      Element configPropertyElement = document.createElement("config-property");
      jndiBinding.appendChild(configPropertyElement);

      Node configPropertyName = document.createElement("config-property-name");
      configPropertyName.setTextContent(configProperty.getName());

      Node configPropertyType = document.createElement("config-property-type");
      configPropertyType.setTextContent(configProperty.getType());

      Node configPropertyValue = document.createElement("config-property-value");
      configPropertyValue.setTextContent(configProperty.getValue());

      configPropertyElement.appendChild(configPropertyName);
      configPropertyElement.appendChild(configPropertyType);
      configPropertyElement.appendChild(configPropertyValue);
    }

    String newXml = XmlUtils.prettyXml(document.getFirstChild());
    config.setCacheXmlContent(newXml);

    getSharedConfiguration().getConfigurationRegion().put("cluster", config);
  }
}
