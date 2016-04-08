/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.datasource;

/**
 * JavaBean for datasource and poold properties.
 * 
 *         This class now contains only those paramaters which are needed
 *         by the Gemfire DataSource configuration. This maps to those
 *         paramaters which are specified as attributes of <jndi-binding>tag.
 *         Those parameters which are specified as attributes of <property>tag
 *         are not stored.
 *  
 */
import java.io.*;

public class ConfiguredDataSourceProperties implements Serializable {

  private static final long serialVersionUID = 1241739895646314739L;
  private transient PrintWriter dataSourcePW;
  private int loginTimeOut;
  private String user;
  private String password;
  private String url;
  private String jdbcDriver;
  private int initialPoolSize = DataSourceResources.CONNECTION_POOL_DEFAULT_INIT_LIMIT;
  private int maxPoolSize = DataSourceResources.CONNECTION_POOL_DEFAULT_MAX_LIMIT;
  private int expirationTime = DataSourceResources.CONNECTION_POOL_DEFAULT_EXPIRATION_TIME;
  private int timeOut = DataSourceResources.CONNECTION_POOL_DEFAULT_CLIENT_TIME_OUT;
  private String connPoolDSClass = null;
  private String xadsClass = null;
  private String mcfClass = null;
  private String txnType = null;

  /** Creates a new instance of DataSourceProperties */
  public ConfiguredDataSourceProperties() {
  }

  //Get Methods for DataSource Properties
  /**
   * Returns the login time
   */
  public int getLoginTimeOut() {
    return loginTimeOut;
  }

  /**
   * Returns the default username
   */
  public String getUser() {
    return user;
  }

  /**
   * Returns the default password
   */
  public String getPassword() {
    return password;
  }

  /**
   * Returns the jdbc driver
   */
  public String getJDBCDriver() {
    return jdbcDriver;
  }

  /**
   * Returns the init pool size.
   */
  public int getInitialPoolSize() {
    return initialPoolSize;
  }

  /**
   * Returns the maximum pool size.
   */
  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  /**
   * Returns the db URL.
   */
  public String getURL() {
    return url;
  }

  /**
   * Returns the max time at which the connection will expire
   */
  public int getConnectionExpirationTime() {
    return expirationTime;
  }

  /**
   * Returns the max time at which the connection will time out.
   */
  public int getConnectionTimeOut() {
    return timeOut;
  }

  /**
   * Returns the class name of the ConnectionPoolDataSource
   */
  public String getConnectionPoolDSClass() {
    return connPoolDSClass;
  }

  /**
   * Returns the class name of the XADataSource.
   */
  public String getXADSClass() {
    return xadsClass;
  }

  /**
   * Returns the log writer for the datasource
   */
  public PrintWriter getPrintWriter() {
    return dataSourcePW;
  }

  /**
   * Returns the class name for managed connection factory.
   * 
   * @return String
   */
  public String getMCFClass() {
    return mcfClass;
  }

  /**
   * Returns the transaction type.
   * 
   * @return "XATransaction"|"NoTransaction"|"LocalTransaction"
   */
  public String getTranType() {
    return txnType;
  }

  /**
   * Sets the login time
   * 
   * @param loginTime
   */
  public void setLoginTimeOut(int loginTime) {
    if (loginTime > 0) loginTimeOut = loginTime;
  }

  /**
   * Sets the database user name .
   * 
   * @param usr
   */
  public void setUser(String usr) {
    this.user = usr;
  }

  /**
   * Sets the database user password .
   * 
   * @param passwd
   */
  public void setPassword(String passwd) {
    this.password = passwd;
  }

  /**
   * Sets the database driver name.
   * 
   * @param confDriver
   */
  public void setJDBCDriver(String confDriver) {
    this.jdbcDriver = confDriver;
  }

  /**
   * Sets the initiale pool size.
   * 
   * @param inpoolSize
   */
  public void setInitialPoolSize(int inpoolSize) {
    if (inpoolSize >= 0) initialPoolSize = inpoolSize;
  }

  /**
   * Sets the maximum pool size
   * 
   * @param mxpoolSize
   */
  public void setMaxPoolSize(int mxpoolSize) {
    if (mxpoolSize > 0) maxPoolSize = mxpoolSize;
  }

  /**
   * Sets the max idle time
   * 
   * @param mxIdleTime
   */
  /**
   * * Sets the db URL.
   * 
   * @param urlStr
   */
  public void setURL(String urlStr) {
    url = urlStr;
  }

  /**
   * Sets the connection expiration time
   * 
   * @param time
   */
  public void setConnectionExpirationTime(int time) {
    if (time > 0) expirationTime = time;
  }

  /**
   * Sets the connection time out.
   * 
   * @param time
   */
  public void setConnectionTimeOut(int time) {
    if (time > 0) timeOut = time;
  }

  /**
   * Sets the ConnectionPoolDataSource class name
   * 
   * @param classname
   */
  public void setConnectionPoolDSClass(String classname) {
    connPoolDSClass = classname;
  }

  /**
   * Sets the XADatasource class name
   * 
   * @param classname
   */
  public void setXADSClass(String classname) {
    xadsClass = classname;
  }

  /**
   * Sets the log writer.
   * 
   * @param pw
   */
  public void setPrintWriter(PrintWriter pw) {
    dataSourcePW = pw;
  }

  /**
   * Sets the MCFClass class name.
   * 
   * @param classname
   */
  public void setMCFClass(String classname) {
    mcfClass = classname;
  }

  /**
   * Sets the Transaction support type for Managed Connections. It can be one of
   * "XATransaction" | "NoTransaction" |"LocalTransaction"
   * 
   * @param type transaction type.
   */
  public void setTransactionType(String type) {
    txnType = type;
  }
}
