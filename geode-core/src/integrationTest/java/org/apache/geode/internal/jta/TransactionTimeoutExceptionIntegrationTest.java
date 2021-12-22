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
package org.apache.geode.internal.jta;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.naming.Context;
import javax.sql.DataSource;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;
import javax.transaction.xa.XAException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.process.ProcessUtils;

/**
 * Fixed up (best as possible) and re-enabled JTA tests from ExceptionsDUnitTest. Treat these as
 * characterization tests because I think some of this behavior is not strictly correct.
 */
public class TransactionTimeoutExceptionIntegrationTest {

  private static final Pattern NEW_DB_PATTERN = Pattern.compile("newDB");
  private static final int DATA_SOURCE_POOL_SIZE = 2;
  private static final int BLOCKING_TIMEOUT_SECONDS = 6;
  private static final int IDLE_TIMEOUT_SECONDS = 600;
  private static final int LOGIN_TIMEOUT_SECONDS = 2;
  private static final int TRANSACTION_TIMEOUT_SECONDS = 2;

  private Cache cache;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    String derbySystemHome = temporaryFolder.newFolder("derby").getAbsolutePath();
    System.setProperty("derby.system.home", derbySystemHome);

    String newDB = "newDB_" + ProcessUtils.identifyPid();

    String input =
        IOUtils.toString(getResource("TransactionTimeoutExceptionIntegrationTest_cachejta.xml"),
            Charset.defaultCharset());
    input = NEW_DB_PATTERN.matcher(input).replaceAll(newDB);
    String output = modifyCacheJtaXml(input, newDB);
    File cacheJtaXmlFile = temporaryFolder.newFile("cachejta.xml");

    FileUtils.writeStringToFile(cacheJtaXmlFile, output, Charset.defaultCharset());

    Properties props = new Properties();
    props.setProperty(CACHE_XML_FILE, cacheJtaXmlFile.getAbsolutePath());

    cache = new CacheFactory(props).create();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  @Test
  public void testBlockingTimeOut() throws Exception {
    Context jndiContext = cache.getJNDIContext();
    DataSource dataSource = (DataSource) jndiContext.lookup("java:/XAPooledDataSource");
    UserTransaction utx = (UserTransaction) jndiContext.lookup("java:/UserTransaction");
    utx.begin();
    dataSource.getConnection();

    // sleep longer than the blocking timeout (nothing exposed to await on)
    Thread.sleep(Duration.ofSeconds(BLOCKING_TIMEOUT_SECONDS * 2).toMillis());

    Throwable thrown = catchThrowable(utx::commit);

    // NOTE: XAException is double-wrapped in SystemException (probably unintentional)
    assertThat(thrown)
        .isInstanceOf(SystemException.class);
    assertThat(thrown.getCause())
        .isInstanceOf(SystemException.class)
        .hasCauseInstanceOf(XAException.class)
        .hasMessageContaining("No current connection");
  }

  @Test
  public void testLoginTimeOut() throws Exception {
    Context jndiContext = cache.getJNDIContext();
    DataSource dataSource = (DataSource) jndiContext.lookup("java:/XAPooledDataSource");
    for (int i = 0; i < DATA_SOURCE_POOL_SIZE; i++) {
      dataSource.getConnection();
    }

    Throwable thrown = catchThrowable(dataSource::getConnection);

    assertThat(thrown)
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Login time-out exceeded");
  }

  @Test
  public void testTransactionTimeOut() throws Exception {
    Context jndiContext = cache.getJNDIContext();
    DataSource dataSource = (DataSource) jndiContext.lookup("java:/XAPooledDataSource");
    dataSource.getConnection();
    UserTransaction utx = (UserTransaction) jndiContext.lookup("java:/UserTransaction");
    utx.begin();
    utx.setTransactionTimeout(TRANSACTION_TIMEOUT_SECONDS);

    // sleep longer than the transaction timeout (nothing exposed to await on)
    Thread.sleep(Duration.ofSeconds(TRANSACTION_TIMEOUT_SECONDS * 2).toMillis());

    Throwable thrown = catchThrowable(utx::commit);

    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Transaction is null, cannot commit a null transaction");
  }

  private String modifyCacheJtaXml(String jtaConfig, String newDB) {
    String begin = "<jndi-binding type=\"XAPooledDataSource\"";
    String end = "</jndi-binding>";
    String jndiBinding =
        "<jndi-binding type=\"XAPooledDataSource\" " +
            "jndi-name=\"XAPooledDataSource\" " +
            "jdbc-driver-class=\"org.apache.derby.jdbc.EmbeddedDriver\" " +
            "init-pool-size=\"" + DATA_SOURCE_POOL_SIZE + "\" " +
            "max-pool-size=\"" + DATA_SOURCE_POOL_SIZE + "\" " +
            "idle-timeout-seconds=\"" + IDLE_TIMEOUT_SECONDS + "\" " +
            "blocking-timeout-seconds=\"" + BLOCKING_TIMEOUT_SECONDS + "\" " +
            "login-timeout-seconds=\"" + LOGIN_TIMEOUT_SECONDS + "\" " +
            "conn-pooled-datasource-class=\"org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource\" "
            +
            "xa-datasource-class=\"org.apache.derby.jdbc.EmbeddedXADataSource\" " +
            "user-name=\"mitul\" " +
            "password=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\" " +
            "connection-url=\"jdbc:derby:" + newDB + ";create=true\" >";
    String configProperty =
        "<config-property>" +
            "<config-property-name>description</config-property-name>" +
            "<config-property-type>java.lang.String</config-property-type>" +
            "<config-property-value>hi</config-property-value>" +
            "</config-property>" +
            "<config-property>" +
            "<config-property-name>user</config-property-name>" +
            "<config-property-type>java.lang.String</config-property-type>" +
            "<config-property-value>jeeves</config-property-value>" +
            "</config-property>" +
            "<config-property>" +
            "<config-property-name>password</config-property-name>" +
            "<config-property-type>java.lang.String</config-property-type>" +
            "<config-property-value>" +
            "83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a" +
            "</config-property-value>" +
            "</config-property>" +
            "<config-property>" +
            "<config-property-name>databaseName</config-property-name>" +
            "<config-property-type>java.lang.String</config-property-type>" +
            "<config-property-value>" + newDB + "</config-property-value>" +
            "</config-property>" +
            System.lineSeparator();

    String modifiedConfig = jndiBinding + configProperty;

    int indexOfBegin = jtaConfig.indexOf(begin);
    int indexOfEnd = jtaConfig.indexOf(end, indexOfBegin);

    return new StringBuilder(jtaConfig).replace(indexOfBegin, indexOfEnd, modifiedConfig)
        .toString();
  }
}
