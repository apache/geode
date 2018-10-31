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

import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.configuration.JndiBindingsType.JndiBinding.ConfigProperty;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.commands.CreateJndiBindingCommand.DATASOURCE_TYPE;
import org.apache.geode.management.internal.cli.exceptions.EntityExistsException;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateJndiBindingFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateDataSourceCommand extends SingleGfshCommand {
  private static final Logger logger = LogService.getLogger();

  static final String CREATE_DATA_SOURCE = "create data-source";
  static final String CREATE_DATA_SOURCE__HELP = "Create a JDBC data source.";
  static final String POOLED_DATA_SOURCE_FACTORY_CLASS = "pooled-data-source-factory-class";
  private static final String DEFAULT_POOLED_DATA_SOURCE_FACTORY_CLASS =
      "org.apache.geode.connectors.jdbc.JdbcPooledDataSourceFactory";
  static final String POOLED_DATA_SOURCE_FACTORY_CLASS__HELP =
      "This class will be created, by calling its no-arg constructor, and used as the pool of this data source. "
          + " The class must implement org.apache.geode.datasource.PooledDataSourceFactory. Only valid if --pooled."
          + " Defaults to: " + DEFAULT_POOLED_DATA_SOURCE_FACTORY_CLASS;
  static final String URL = "url";
  static final String URL__HELP =
      "This is the JDBC data source URL string, for example, jdbc:hsqldb:hsql://localhost:1701.";
  static final String NAME = "name";
  static final String NAME__HELP = "Name of the data source to be created.";
  static final String PASSWORD = "password";
  static final String PASSWORD__HELP =
      "This element specifies the default password used when creating a new connection.";
  static final String USERNAME = "username";
  static final String USERNAME__HELP =
      "This element specifies the default username used when creating a new connection.";
  static final String POOLED = "pooled";
  static final String POOLED__HELP =
      "By default a pooled data source is created. If this option is false then a non-pooled data source is created.";
  static final String IFNOTEXISTS__HELP =
      "Skip the create operation when a data source with the same name already exists.  Without specifying this option, this command execution results into an error.";
  static final String POOL_PROPERTIES = "pool-properties";
  static final String POOL_PROPERTIES_HELP =
      "Properties for a pooled data source. Only valid if --pooled. These properties will be used to configure the database data source unless the name begins with \"pool.\"."
          + " If that prefix is used it will be used to configure the pool data source."
          + "The value is a comma separated list of json strings. Each json string contains a name and value. "
          + "For example: --properties={'name':'name1','value':'value1'},{'name':'name2','value':'value2'}";

  @CliCommand(value = CREATE_DATA_SOURCE, help = CREATE_DATA_SOURCE__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE,
      interceptor = "org.apache.geode.management.internal.cli.commands.CreateDataSourceInterceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel createJDNIBinding(
      @CliOption(key = POOLED_DATA_SOURCE_FACTORY_CLASS,
          help = POOLED_DATA_SOURCE_FACTORY_CLASS__HELP) String pooledDataSourceFactoryClass,
      @CliOption(key = URL, mandatory = true,
          help = URL__HELP) String url,
      @CliOption(key = NAME, mandatory = true, help = NAME__HELP) String jndiName,
      @CliOption(key = USERNAME, help = USERNAME__HELP) String username,
      @CliOption(key = PASSWORD, help = PASSWORD__HELP) String password,
      @CliOption(key = CliStrings.IFNOTEXISTS, help = IFNOTEXISTS__HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean ifNotExists,
      @CliOption(key = POOLED, help = POOLED__HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "true") boolean pooled,
      @CliOption(key = POOL_PROPERTIES, optionContext = "splittingRegex=,(?![^{]*\\})",
          help = POOL_PROPERTIES_HELP) DataSourceProperty[] poolProperties) {

    JndiBindingsType.JndiBinding configuration = new JndiBindingsType.JndiBinding();
    configuration.setCreatedByDataSourceCommand(true);
    configuration.setConnPooledDatasourceClass(pooledDataSourceFactoryClass);
    configuration.setConnectionUrl(url);
    configuration.setJndiName(jndiName);
    configuration.setPassword(password);
    if (pooled) {
      configuration.setType(DATASOURCE_TYPE.POOLED.getType());
    } else {
      configuration.setType(DATASOURCE_TYPE.SIMPLE.getType());
    }
    configuration.setUserName(username);
    if (poolProperties != null && poolProperties.length > 0) {
      List<ConfigProperty> configProperties = configuration.getConfigProperties();
      for (DataSourceProperty dataSourceProperty : poolProperties) {
        String name = dataSourceProperty.getName();
        String value = dataSourceProperty.getValue();
        configProperties.add(new ConfigProperty(name, "type", value));
      }
    }

    InternalConfigurationPersistenceService service =
        (InternalConfigurationPersistenceService) getConfigurationPersistenceService();

    if (service != null) {
      CacheConfig cacheConfig = service.getCacheConfig("cluster");
      if (cacheConfig != null) {
        JndiBindingsType.JndiBinding existing =
            CacheElement.findElement(cacheConfig.getJndiBindings(), jndiName);
        if (existing != null) {
          throw new EntityExistsException(
              CliStrings.format("Data source named \"{0}\" already exists.", jndiName),
              ifNotExists);
        }
      }
    }

    Set<DistributedMember> targetMembers = findMembers(null, null);
    if (targetMembers.size() > 0) {
      List<CliFunctionResult> jndiCreationResult = executeAndGetFunctionResult(
          new CreateJndiBindingFunction(), configuration, targetMembers);
      ResultModel result = ResultModel.createMemberStatusResult(jndiCreationResult);
      result.setConfigObject(configuration);
      return result;
    } else {
      return ResultModel.createInfo("No members found.");
    }
  }

  @Override
  public void updateClusterConfig(String group, CacheConfig config, Object element) {
    config.getJndiBindings().add((JndiBindingsType.JndiBinding) element);
  }

  public static class DataSourceProperty {
    @Override
    public String toString() {
      return "DataSourceProperty [name=" + name + ", value=" + value + "]";
    }

    private String name;
    private String value;

    public DataSourceProperty() {}

    public DataSourceProperty(String name, String value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

  }
}
