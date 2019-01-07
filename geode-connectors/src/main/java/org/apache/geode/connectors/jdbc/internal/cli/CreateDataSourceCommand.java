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
package org.apache.geode.connectors.jdbc.internal.cli;

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.configuration.JndiBindingsType.JndiBinding.ConfigProperty;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.commands.CreateJndiBindingCommand.DATASOURCE_TYPE;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateJndiBindingFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class CreateDataSourceCommand extends SingleGfshCommand {
  static final String CREATE_DATA_SOURCE = "create data-source";
  static final String CREATE_DATA_SOURCE__HELP = EXPERIMENTAL
      + "Creates a JDBC data source and verifies connectivity to an external JDBC database.";
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
      "The password that may be required by the external JDBC database when creating a new connection.";
  static final String USERNAME = "username";
  static final String USERNAME__HELP =
      "The username that may be required by the external JDBC database when creating a new connection.";
  static final String POOLED = "pooled";
  static final String POOLED__HELP =
      "By default a pooled data source is created. If this option is false then a non-pooled data source is created.";
  static final String IFNOTEXISTS__HELP =
      "Skip the create operation when a data source with the same name already exists.  Without specifying this option, this command execution results into an error.";
  static final String POOL_PROPERTIES = "pool-properties";
  static final String POOL_PROPERTIES_HELP =
      "Used to configure pool properties of a pooled data source. Only valid if --pooled is specified."
          + "The value is a comma separated list of json strings. Each json string contains a name and value. "
          + "If the name starts with \"pool.\", then it will be used to configure the pool data source. "
          + "Otherwise the name value pair will be used to configure the database data source. "
          + "For example 'pool.name1' configures the pool and 'name2' configures the database in the following: "
          + "--pool-properties={'name':'pool.name1','value':'value1'},{'name':'name2','value':'value2'}";

  @CliCommand(value = CREATE_DATA_SOURCE, help = CREATE_DATA_SOURCE__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE,
      interceptor = "org.apache.geode.connectors.jdbc.internal.cli.CreateDataSourceInterceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel createDataSource(
      @CliOption(key = POOLED_DATA_SOURCE_FACTORY_CLASS,
          help = POOLED_DATA_SOURCE_FACTORY_CLASS__HELP) String pooledDataSourceFactoryClass,
      @CliOption(key = URL, mandatory = true,
          help = URL__HELP) String url,
      @CliOption(key = NAME, mandatory = true, help = NAME__HELP) String name,
      @CliOption(key = USERNAME, help = USERNAME__HELP) String username,
      @CliOption(key = PASSWORD, help = PASSWORD__HELP) String password,
      @CliOption(key = CliStrings.IFNOTEXISTS, help = IFNOTEXISTS__HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean ifNotExists,
      @CliOption(key = POOLED, help = POOLED__HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean pooled,
      @CliOption(key = POOL_PROPERTIES, optionContext = "splittingRegex=,(?![^{]*\\})",
          help = POOL_PROPERTIES_HELP) PoolProperty[] poolProperties) {

    JndiBindingsType.JndiBinding configuration = new JndiBindingsType.JndiBinding();
    configuration.setConnPooledDatasourceClass(pooledDataSourceFactoryClass);
    configuration.setConnectionUrl(url);
    configuration.setJndiName(name);
    configuration.setPassword(password);
    if (pooled) {
      configuration.setType(DATASOURCE_TYPE.POOLED.getType());
    } else {
      configuration.setType(DATASOURCE_TYPE.SIMPLE.getType());
    }
    configuration.setUserName(username);
    if (poolProperties != null && poolProperties.length > 0) {
      List<ConfigProperty> configProperties = configuration.getConfigProperties();
      for (PoolProperty dataSourceProperty : poolProperties) {
        String propName = dataSourceProperty.getName();
        String propValue = dataSourceProperty.getValue();
        configProperties.add(new ConfigProperty(propName, "type", propValue));
      }
    }

    InternalConfigurationPersistenceService service = getConfigurationPersistenceService();

    if (service != null) {
      CacheConfig cacheConfig = service.getCacheConfig("cluster");
      if (cacheConfig != null && CacheElement.exists(cacheConfig.getJndiBindings(), name)) {
        String message =
            CliStrings.format("Jndi binding with jndi-name \"{0}\" already exists.", name);
        return ifNotExists ? ResultModel.createInfo("Skipping: " + message)
            : ResultModel.createError(message);
      }
    }

    Set<DistributedMember> targetMembers = findMembers(null, null);
    if (targetMembers.size() > 0) {
      Object[] arguments = new Object[] {configuration, true};
      List<CliFunctionResult> jndiCreationResult = executeAndGetFunctionResult(
          new CreateJndiBindingFunction(), arguments, targetMembers);
      ResultModel result =
          ResultModel.createMemberStatusResult(jndiCreationResult, EXPERIMENTAL, null, false, true);
      result.setConfigObject(configuration);
      return result;
    } else {
      if (service != null) {
        ResultModel result =
            ResultModel.createInfo("No members found, data source saved to cluster configuration.");
        result.setConfigObject(configuration);
        return result;
      } else {
        return ResultModel.createError("No members found and cluster configuration unavailable.");
      }
    }
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object element) {
    config.getJndiBindings().add((JndiBindingsType.JndiBinding) element);
    return true;
  }

  @CliAvailabilityIndicator({CREATE_DATA_SOURCE})
  public boolean commandAvailable() {
    return isOnlineCommandAvailable();
  }

  public static class PoolProperty {
    private String name;
    private String value;

    public PoolProperty() {}

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

    @Override
    public String toString() {
      return "PoolProperty [name=" + name + ", value=" + value + "]";
    }
  }
}
