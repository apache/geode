package com.gemstone.gemfire.mgmt.DataBrowser.controller;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.mgmt.DataBrowser.connection.ClientConfiguration;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ConnectionFailureException;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.DSConfiguration;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.EndPoint;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireConnection;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireConnectionListener;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireMemberListener;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.JMXOperationFailureException;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.SecurityAttributes;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.internal.JMXCallExecutor;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.internal.DataBrowserCustomClassLoader;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.internal.QueryExecutionHelper;
import com.gemstone.gemfire.mgmt.DataBrowser.model.IMemberEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberConfigurationPrms;
import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryExecutionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQQuery;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQResult;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomMsgDispatcher;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomUIMessages;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;
import com.gemstone.gemfire.security.AuthenticationFailedException;

/**
 * Abstract class ,Act as bridge between view and model. It delegates the view
 * operation to model. e.g establishing connection with DS, Executing query,
 * getting snapshot
 * 
 * Also registers itself as member change listener with model
 * 
 * @author mjha
 */

public abstract class DataBrowserController implements GemFireMemberListener, GemFireConnectionListener {
  // TODO MGH - Any advantage if this was read from config?
  private static final String CLIENT_AUTHINITIALIZE_INTERFACE   = "com.gemstone.gemfire.security.AuthInitialize";
  
  private QueryExecutionHelper queryHelper;
  private GemFireConnection           connection = null;
  private ConfigurationValidator      configValidator;
  private SecurityAttributes          secAttributes;
  private boolean                     isSecurityAttributesChanged;
  private DSConfiguration             currentConfiguartion;
  final static CustomMsgDispatcher msgDispatcher_     = new CustomMsgDispatcher();

  private DataBrowserCustomClassLoader dataBrowserCustomClassLoader = null;

  public DataBrowserController() {
    configValidator = new ConfigurationValidator();
  }
  
  private void initializeApplicationClassLoader(){
    dataBrowserCustomClassLoader = new DataBrowserCustomClassLoader(new URL[] { }, ClassLoader.getSystemClassLoader());
    Thread.currentThread().setContextClassLoader(dataBrowserCustomClassLoader);
    
    String[] applicationClasses = DataBrowserPreferences.getApplicationClasses();
    try {
      setApplicationClassJars(applicationClasses);
    }
    catch (IOException e) {
      // TODO do we need to log??
    }
  }

  // MGH - Adding a method to indicate whether a connection is present
  //       This controller currently does not multiplex connections.
  //       When that happens, we should add a new public API that 
  //       takes a DSSnapShot (or some other way to identify a DS)
  //       and return to the caller a flag indicating whether there
  //       is an active connection to that DS.
  public boolean hasConnection() {
    return null != connection;
  }
  /**
   * Disconnect from currently connected DS
   * 
   * @return true if disconnection happens successfully
   */
  // TODO MGH - no need to have any return value here
  public boolean disconnect() {
    if (connection != null) {
      connection.removeConnectionNotificationListener(this);
      connection.removeGemFireMemberListener(this);
      connection.close();
//      connection.removeGemFireMemberListener(this);
      connection = null;
      // set flags to stop query executions, if query is already in queue
      queryHelper.shutDown();
      queryHelper = null;
      currentConfiguartion = null;
      secAttributes = null;
      return true;
    }
    return false;
  }

  /**
   * Establish connection with DS based on configuration
   * 
   * @return true if disconnection happens successfully
   */
  public boolean connect(DSConfiguration configuration)
      throws ConnectionFailureException {

    if (connection != null)
      return false;
 
    long connectionTimeoutInterval = DataBrowserPreferences.getConnectionTimeoutInterval();   
    JMXCallExecutor exec = new JMXCallExecutor(connectionTimeoutInterval);
    try {
      connection = exec.makeConnection(configuration);
    }
    catch (JMXOperationFailureException e) {
      throw new ConnectionFailureException(e);
    }
    
    currentConfiguartion = configuration;
    // initialize query helper for new connection
    queryHelper = QueryExecutionHelper.getInstance();
    queryHelper.start(connection);
    
    connection.addGemFireMemberListener(this);
    connection.addConnectionNotificationListener(this);
    secAttributes = configuration.getSecAttributes();
    
    return true;
  }
  
  public QueryResult getLastQueryResults(){
    if(connection == null)
      return null;
    
    return queryHelper.getLastQueryResults();
  }
  
  public void stopCq(String name){
   if(this.connection != null) {
     try {
      this.connection.stopCQ(name);
    }
    catch (CQException e) {      
      LogUtil.error("Error occured  while stopping CQ" + e);
    }    
   }
  }
  
  public void closeCq(String name){
    if(this.connection != null) {
      try {
       this.connection.closeCQ(name);
     }
     catch (CQException e) {      
       LogUtil.error("Error occured  while stopping CQ" + e);
     }    
    }
  }
  

  /**
   * Execute the query on server specified in queryPrms
   * 
   * @param queryPrms
   *          instance of {@link QueryConfigurationPrms} , required to perform
   *          query
   * @throws MissingSecuritiesPropException 
   * @throws ClassNotFoundException 
   */
  public void executeQuery(QueryConfigurationPrms queryPrms)
      throws InvalidConfigurationException, QueryExecutionException, MissingSecuritiesPropException, AuthenticationFailedException {
    if (connection == null)
      return;
    
    //Perform the cleanup before executing the query. This is required to fix BUG669.
    if(!(queryPrms instanceof CQConfiguarationPrms)) {
      queryHelper.setLastQueryResults(null);
      msgDispatcher_.sendCustomMessage(CustomUIMessages.DISPOSE_QUERY_RESULTS, new ArrayList(), new ArrayList());    
    }
    
    GemFireMember member = queryPrms.getMember();
    boolean securityEnabled = isSecurityEnabled(member) ;
    if(securityEnabled && secAttributes == null){
      throw new MissingSecuritiesPropException(member.getId() +" running in secure mode" + "\n" + "Specify security");
    }
    
    if(dataBrowserCustomClassLoader == null)
      initializeApplicationClassLoader();
    
    if (securityEnabled && isSecurityAttributesChanged) {
      try {
        DSConfiguration config = currentConfiguartion;
        String authImpl = validateAndCLientAutherticationProperties(secAttributes.getSecurityPluginPath());
        secAttributes.setAuthImplString(authImpl);
        config.setSecAttributes(secAttributes);
        disconnect();
        connect(config);
      }
      catch (ConnectionFailureException e) {
        throw new QueryExecutionException(e);
      }
      catch (ClassNotFoundException e) {
        throw new QueryExecutionException(e);
      }
      catch (IOException e) {
        throw new QueryExecutionException(e);
      }
      try {
        addJar(secAttributes.getSecurityPluginPath());
      }
      catch (IOException e) {
        throw new QueryExecutionException(e);
      }

      isSecurityAttributesChanged = false;
    }
   
    boolean validated = configValidator.validateQueryPrms(queryPrms);
    if (!validated) {
      String message = configValidator.getMessage();
      throw new InvalidConfigurationException(message);
    }

    if (queryHelper != null){
      if (queryPrms.isCQ()) {
        CQConfiguarationPrms cqPrms = (CQConfiguarationPrms)queryPrms;
        String queryString = cqPrms.getQueryString();
        ICQueryEventListener cQEventLstnr = cqPrms.getCQeventListner();
        CQQuery cQuery = connection
            .newCQ(cqPrms.getName(), queryString, member);
        cQEventLstnr.setQuery(cQuery);
        CQResult queryResult = cQuery.getQueryResult();
        ICQueryEventListener qeventListner = cqPrms.getCQeventListner();
        queryResult.addCQEventListener(qeventListner);
        queryResult.addTypeListener(qeventListner);
        cqPrms.setCQuery(cQuery);
      }
      queryHelper.submitQuery(queryPrms);
    }
  }
  
  public void executeCQ(CQConfiguarationPrms cqPrms)
      throws InvalidConfigurationException, QueryExecutionException,
      MissingSecuritiesPropException {
    executeQuery(cqPrms);
  }
  
  private String validateAndCLientAutherticationProperties(String securityPluginPath) throws ClassNotFoundException, IOException{
    String authImpl = null;
    if(securityPluginPath != null && securityPluginPath.length() != 0){
      try {
        Class<?> authClass = ClassLoader.getSystemClassLoader().loadClass(CLIENT_AUTHINITIALIZE_INTERFACE);
        authImpl = DataBrowserCustomClassLoader.getAuthProp(securityPluginPath, authClass);
      }
      catch (ClassNotFoundException e) {
        String errMsg = "Implementation of "+CLIENT_AUTHINITIALIZE_INTERFACE + " not found in classpath";
        LogUtil.error(errMsg);
        throw new ClassNotFoundException(errMsg);
      }
      catch (IOException e) {
        LogUtil.error(e.getMessage());
        throw e;
      }
    }
    return authImpl;
  }
  
  private boolean isSecurityEnabled(GemFireMember member){
    boolean enabled = false;
    MemberConfigurationPrms[] config = member.getConfig();
    for (int i = 0; i < config.length; i++) {
      if(config[i].getName().equals("security-client-authenticator")){
        Object value = config[i].getValue();
        if(value != null){
          if(value instanceof String){
            String val= (String)value;
            if(val.trim().length() != 0){
              enabled = true;
              return enabled;
            }
          }
        }
      }
    }
    return enabled ;
  }
  
  /**
   * Api, to add jar of custom classes used as region entry in cache severs
   * region, at runtime
   * 
   * @param jars
   * @throws IOException 
   */
  public void setApplicationClassJars(String[] jars) throws IOException {
    if(dataBrowserCustomClassLoader == null)
      return;
    
    for (int i = 0; i < jars.length; i++) {
      addJar(jars[i]);
    }
    
    try {

      DataBrowserCustomClassLoader.loadApplicationClass(dataBrowserCustomClassLoader, jars);
    }
    catch (ClassNotFoundException e) {
      // TODO need to log???
    }
  }
  
  private void addJar(String jar) throws IOException {
    File file = new File(jar);
    URL url = file.toURI().toURL();
    dataBrowserCustomClassLoader.addURL(url);
  }

  /**
   * 
   * @return instance {@link DSSnapShot} of Ds connected
   */
  public DSSnapShot getDSSnapShot() {
    if (connection == null)
      return null;
    DSSnapShot snapShot = new DSSnapShot(connection);
    return snapShot;
  }
  
  public DSConfiguration getCurrentConnection(){
    return currentConfiguartion;
  }
  
  public void setSecurityAttributes(SecurityAttributes attr) {
    secAttributes = attr;
    isSecurityAttributesChanged = true;
  }

  public abstract void memberEventReceived(IMemberEvent memEvent);
  
  
  public void setConnectionRefreshInterval(long interval) {
   if(null != this.connection) {
     this.connection.setRefreshInterval(interval);
   }
  }

  /**
   * Validator to validate connection configuration
   * 
   * @author mjha
   */
  private static class ConfigurationValidator {

    private String msg;

    public boolean validateConnectionConfiguartion(ClientConfiguration configuration) {
      List<EndPoint> cacheServers = configuration.getCacheServers();

      if (cacheServers == null || cacheServers.size() == 0) {

        msg = "Host/port List provided is not valid";

        return false;
      }

      return true;
    }

    public boolean validateQueryPrms(QueryConfigurationPrms prms) {
      String queryString = prms.getQueryString();
      GemFireMember member = prms.getMember();
      IQueryExecutionListener queryExecutionListener = prms
          .getQueryExecutionListener();

      if (queryString == null) {
        msg = "no query string found to query";
        return false;
      } 
      else if (member == null) {
        msg = "pass valid member to pass query";
        return false;
      } 
      else if (queryExecutionListener == null) {
        msg = "call back object to process query is null";
        return false;
      }

      return true;
    }

    public String getMessage() {
      return msg;
    }
  }
}
