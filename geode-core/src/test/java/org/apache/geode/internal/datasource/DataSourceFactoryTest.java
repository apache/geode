/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.datasource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.junit.Test;

import org.apache.geode.datasource.PooledDataSourceFactory;

public class DataSourceFactoryTest {

  private final Map<String, String> inputs = new HashMap<>();
  private final List<ConfigProperty> configProperties = new ArrayList<>();

  @Test
  public void creatPoolPropertiesWithNullInputReturnsEmptyOutput() {
    Properties output = DataSourceFactory.createPoolProperties(null, null);

    assertThat(output.isEmpty()).isTrue();
  }

  @Test
  public void creatPoolPropertiesWithOneConfigDataSourcePropertyReturnsEmptyOutput() {
    configProperties.add(new ConfigProperty("n1", "v1", null));

    Properties output = DataSourceFactory.createPoolProperties(inputs, configProperties);

    assertThat(output.isEmpty()).isTrue();
  }

  @Test
  public void creatPoolPropertiesWithOneConfigPoolPropertyReturnsOneOutput() {
    configProperties.add(new ConfigProperty("pool.n1", "v1", null));

    Properties output = DataSourceFactory.createPoolProperties(inputs, configProperties);

    assertThat(output.size()).isEqualTo(1);
    assertThat(output.getProperty("n1")).isEqualTo("v1");
  }

  @Test
  public void creatPoolPropertiesWithOneUpperCaseConfigPoolPropertyReturnsOneOutput() {
    configProperties.add(new ConfigProperty("POOL.N1", "v1", null));

    Properties output = DataSourceFactory.createPoolProperties(inputs, configProperties);

    assertThat(output.size()).isEqualTo(1);
    assertThat(output.getProperty("N1")).isEqualTo("v1");
  }

  @Test
  public void creatPoolPropertiesWithEmptyInputReturnsEmptyOutput() {
    Properties output = DataSourceFactory.createPoolProperties(inputs, configProperties);

    assertThat(output.isEmpty()).isTrue();
  }

  @Test
  public void creatPoolPropertiesWithNullValueInputReturnsEmptyOutput() {
    inputs.put("name", null);

    Properties output = DataSourceFactory.createPoolProperties(inputs, configProperties);

    assertThat(output.isEmpty()).isTrue();
  }

  @Test
  public void creatPoolPropertiesWithEmptyStringValueInputReturnsEmptyOutput() {
    inputs.put("name", "");

    Properties output = DataSourceFactory.createPoolProperties(inputs, configProperties);

    assertThat(output.isEmpty()).isTrue();
  }

  @Test
  public void creatPoolPropertiesWithOneInputReturnsOneOutput() {
    inputs.put("name", "value");

    Properties output = DataSourceFactory.createPoolProperties(inputs, configProperties);

    assertThat(output.size()).isEqualTo(1);
    assertThat(output.getProperty("name")).isEqualTo("value");
  }

  @Test
  public void creatPoolPropertiesWithIgnoredInputKeysReturnsEmptyOutput() {
    inputs.put("type", "value");
    inputs.put("jndi-name", "value");
    inputs.put("transaction-type", "value");
    inputs.put("conn-pooled-datasource-class", "value");
    inputs.put("managed-conn-factory-class", "value");
    inputs.put("xa-datasource-class", "value");

    Properties output = DataSourceFactory.createPoolProperties(inputs, configProperties);

    assertThat(output.isEmpty()).isTrue();
  }

  @Test
  public void creatPoolPropertiesWithIgnoredAndValidInputsReturnsValidOutputs() {
    inputs.put("name1", "");
    inputs.put("name2", null);
    inputs.put("type", "value");
    inputs.put("jndi-name", "value");
    inputs.put("transaction-type", "value");
    inputs.put("conn-pooled-datasource-class", "value");
    inputs.put("managed-conn-factory-class", "value");
    inputs.put("xa-datasource-class", "value");
    inputs.put("validname1", "value1");
    inputs.put("validname2", "value2");
    configProperties.add(new ConfigProperty("pool.n1", "v1", null));
    configProperties.add(new ConfigProperty("dataSourceProp", "dataSourceValue", null));
    configProperties.add(new ConfigProperty("pool.n2", "v2", null));

    Properties output = DataSourceFactory.createPoolProperties(inputs, configProperties);

    assertThat(output.size()).isEqualTo(4);
    assertThat(output.getProperty("validname1")).isEqualTo("value1");
    assertThat(output.getProperty("validname2")).isEqualTo("value2");
    assertThat(output.getProperty("n1")).isEqualTo("v1");
    assertThat(output.getProperty("n2")).isEqualTo("v2");
  }

  @Test
  public void createDataSourcePropertiesWithNullReturnsEmpty() {
    Properties output = DataSourceFactory.createDataSourceProperties(null);

    assertThat(output).isEmpty();
  }

  @Test
  public void createDataSourcePropertiesWithEmptyListReturnsEmpty() {
    Properties output = DataSourceFactory.createDataSourceProperties(configProperties);

    assertThat(output).isEmpty();
  }

  @Test
  public void createDataSourcePropertiesWithPoolPropertyReturnsEmpty() {
    configProperties.add(new ConfigProperty("pool.n1", "v1", null));

    Properties output = DataSourceFactory.createDataSourceProperties(configProperties);

    assertThat(output).isEmpty();
  }

  @Test
  public void createDataSourcePropertiesWithNonPoolPropertyReturnsOne() {
    configProperties.add(new ConfigProperty("n1", "v1", null));

    Properties output = DataSourceFactory.createDataSourceProperties(configProperties);

    assertThat(output.size()).isEqualTo(1);
    assertThat(output.getProperty("n1")).isEqualTo("v1");
  }

  @Test
  public void createDataSourcePropertiesWithMuliplePropertiesReturnsJustNonPool() {
    configProperties.add(new ConfigProperty("n1", "v1", null));
    configProperties.add(new ConfigProperty("pool.n3", "v3", null));
    configProperties.add(new ConfigProperty("n2", "v2", null));

    Properties output = DataSourceFactory.createDataSourceProperties(configProperties);

    assertThat(output.size()).isEqualTo(2);
    assertThat(output.getProperty("n1")).isEqualTo("v1");
    assertThat(output.getProperty("n2")).isEqualTo("v2");
  }

  public static class TestPooledDataSourceFactory implements PooledDataSourceFactory {
    @Override
    public DataSource createDataSource(Properties poolProperties, Properties dataSourceProperties) {
      return new TestDataSource(poolProperties, dataSourceProperties);
    }
  }

  public static class TestDataSource implements DataSource {
    private final Properties poolProperties;
    private final Properties dataSourceProperties;

    public TestDataSource(Properties poolProperties, Properties dataSourceProperties) {
      this.poolProperties = poolProperties;
      this.dataSourceProperties = dataSourceProperties;
    }

    public Properties getPoolProperties() {
      return this.poolProperties;
    }

    public Properties getDataSourceProperties() {
      return this.dataSourceProperties;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {}

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {}

    @Override
    public int getLoginTimeout() throws SQLException {
      return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return false;
    }

    @Override
    public Connection getConnection() throws SQLException {
      return null;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return null;
    }

  }

  @Test
  public void getPooledDataSourceUsesConnPooledDataSourceClass() throws DataSourceCreateException {
    inputs.put("conn-pooled-datasource-class",
        "org.apache.geode.internal.datasource.DataSourceFactoryTest$TestPooledDataSourceFactory");

    DataSource dataSource = new DataSourceFactory().getPooledDataSource(inputs, configProperties);

    assertThat(dataSource).isInstanceOf(TestDataSource.class);
  }

  @Test
  public void getPooledDataSourcePassesCorrectPropertiesToConnPooledDataSourceClass()
      throws DataSourceCreateException {
    inputs.put("conn-pooled-datasource-class",
        "org.apache.geode.internal.datasource.DataSourceFactoryTest$TestPooledDataSourceFactory");
    inputs.put("name1", "");
    inputs.put("name2", null);
    inputs.put("type", "value");
    inputs.put("jndi-name", "value");
    inputs.put("transaction-type", "value");
    inputs.put("managed-conn-factory-class", "value");
    inputs.put("xa-datasource-class", "value");
    inputs.put("validname1", "value1");
    inputs.put("validname2", "value2");
    configProperties.add(new ConfigProperty("pool.n1", "v1", null));
    configProperties.add(new ConfigProperty("dataSourceProp", "dataSourceValue", null));
    configProperties.add(new ConfigProperty("pool.n2", "v2", null));

    TestDataSource dataSource =
        (TestDataSource) new DataSourceFactory().getPooledDataSource(inputs, configProperties);

    Properties poolProperties = dataSource.getPoolProperties();
    Properties dataSourceProperties = dataSource.getDataSourceProperties();
    assertThat(poolProperties.size()).isEqualTo(4);
    assertThat(poolProperties.getProperty("validname1")).isEqualTo("value1");
    assertThat(poolProperties.getProperty("validname2")).isEqualTo("value2");
    assertThat(poolProperties.getProperty("n1")).isEqualTo("v1");
    assertThat(poolProperties.getProperty("n2")).isEqualTo("v2");
    assertThat(dataSourceProperties.size()).isEqualTo(1);
    assertThat(dataSourceProperties.getProperty("dataSourceProp")).isEqualTo("dataSourceValue");
  }

  @Test
  public void getPooledDataSourceFailsIfClassDoesNotExist() throws DataSourceCreateException {
    inputs.put("conn-pooled-datasource-class", "doesNotExist");

    Throwable throwable =
        catchThrowable(() -> new DataSourceFactory().getPooledDataSource(inputs, configProperties));

    assertThat(throwable).isInstanceOf(DataSourceCreateException.class).hasMessage(
        "DataSourceFactory::getPooledDataSource:Exception creating ConnectionPoolDataSource.Exception string=java.lang.ClassNotFoundException: doesNotExist");
  }

}
