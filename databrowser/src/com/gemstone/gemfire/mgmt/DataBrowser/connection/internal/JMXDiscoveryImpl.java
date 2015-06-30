/*========================================================================= 
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection.internal;

import java.io.IOException;
import java.io.InvalidClassException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.management.JMException;
import javax.management.JMX;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServerConnection;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import com.gemstone.gemfire.admin.ConfigurationParameter;
import com.gemstone.gemfire.admin.GemFireMemberStatus;
import com.gemstone.gemfire.admin.RegionSubRegionSnapshot;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.management.CacheServerMXBean;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.GemFireProperties;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.internal.JmxManagerLocatorRequest;
import com.gemstone.gemfire.management.internal.JmxManagerLocatorResponse;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.cli.shell.JMXConnectionException;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ConnectionClosedException;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ConnectionFailureException;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ConnectionTerminatedEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GFMemberDiscovery;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireConnectionListener;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireMemberListener;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.VersionMismatchException;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.CacheServerInfo;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberStatus;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberConfigurationPrms;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberCrashedEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberFactory;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberJoinedEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberLeftEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberUpdatedEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.region.RegionFactory;
import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

public class JMXDiscoveryImpl implements GFMemberDiscovery, NotificationListener {
  public static final String                                            JMX_URI                        = "service:jmx:rmi://{0}/jndi/rmi://{0}:{1}/jmxconnector";
  public static final String                                            JMX_URI_MANAGER                = "service:jmx:rmi://{0}/jndi/rmi://{0}:{1}/jmxrmi";
  public static final String                                            MBEAN_PROPERTY_BEAN_ID         = "id";
  public static final String                                            MBEAN_PROPERTY_BEAN_TYPE       = "type";
  public static final String                                            MBEAN_GEMFIRE_APPLICATION_TYPE = "Application";
  public static final String                                            MBEAN_GEMFIRE_CACHEVM_TYPE     = "CacheVm";
  public static final String                                            MBEAN_DOMAIN_GEMFIRE_NAME      = "GemFire";
  public static final String                                            MBEAN_AGENT_TYPE               = "Agent";

  // MGH: FindBugs reports a false positive for most of these private variables
  // as being unused.
  private com.gemstone.gemfire.mgmt.DataBrowser.model.DistributedSystem system_model;
  private JMXConnector                                                  connector;
  private MBeanServerConnection                                         connServer;
  private ObjectName                                                    agent;
  private ObjectName                                                    ds;
  private Map<String, Pool>                                             connections;
  private Map<String, ObjectName>                                       jmxinfo;
  private Timer                                                         updateTimer;
  private JMXNotificationListener                                       jmxNotifListener;
  private List<GemFireMemberListener>                                   memberListeners;
  private List<GemFireConnectionListener>                               connectionListeners;
  private volatile boolean                                              terminated;
  private long                                                          refreshInterval;
  private ObjectName[] servers;
  private DistributedSystemMXBean distributedSystemMXBeanProxy; 
  private MemberMXBean managerMemberMXBeanProxy = null;

  public JMXDiscoveryImpl(String hostName, int port, String gfeSysName, String userName, String password)
      throws ConnectionFailureException {
    system_model = new com.gemstone.gemfire.mgmt.DataBrowser.model.DistributedSystem(
        gfeSysName);
    connections = Collections.synchronizedMap(new HashMap<String, Pool>());
    jmxinfo = Collections.synchronizedMap(new HashMap<String, ObjectName>());
    memberListeners = new ArrayList<GemFireMemberListener>();
    connectionListeners = new ArrayList<GemFireConnectionListener>();

    establishConnectionUsingJMXManager(hostName, port, userName, password);
    //establishConnection(hostName, port);
    jmxNotifListener = new JMXNotificationListener(this);

    jmxNotifListener.start();

    setRefreshInterval(DataBrowserPreferences.getDSRefreshInterval());
  }

  boolean isTerminated() {
    return terminated;
  }

  MBeanServerConnection getConnection() {
    if (!isTerminated())
      return this.connServer;

    return null;
  }

  ObjectName getAdminDistributedSystem() {
    return this.ds;
  }

  public void addGemFireMemberListener(GemFireMemberListener listener) {
    this.memberListeners.add(listener);
  }

  public void addConnectionNotificationListener(
      GemFireConnectionListener listener) {
    this.connectionListeners.add(listener);
  }

  public void removeConnectionNotificationListener(
      GemFireConnectionListener listener) {
    this.connectionListeners.remove(listener);
  }

  public void close() {
    if (this.connServer != null) {
      this.terminated = true;
      this.updateTimer.cancel();
      this.jmxNotifListener.stop();
      try {
        connector.close();
      } catch (IOException e) {
        LogUtil.warning("Exception while closing RMI connection", e);
      }

      this.connector = null;
      this.connServer = null;

      LogUtil.info("Connection to the GemFire system is closed...");
    }
  }

  
  public DistributedSystemMXBean getDistributedSystemMXBeanProxy() {
    return distributedSystemMXBeanProxy;
  }

  public GemFireMember getMember(String id) {
    return this.system_model.getMember(id);
  }

  public GemFireMember[] getMembers() {
    return this.system_model.getMembers().toArray(new GemFireMember[0]);
  }

  public QueryService getQueryService(GemFireMember member) {
    Pool pool = null;

    if (!this.connections.containsKey(member.getId())) {
      int           socketReadTimeout    = DataBrowserPreferences.getSocketReadTimeoutInterval();
      String        memberName           = member.getName();
      StringBuilder cacheServersHostPort = new StringBuilder();
      
      LogUtil.info("Preparing Pool for member :" + memberName);
      PoolFactory factory = PoolManager.createFactory();
      factory.setSubscriptionEnabled(true);
      factory.setReadTimeout(socketReadTimeout);
      CacheServerInfo[] cacheServers = member.getCacheServers();
      for (int i = 0; i < cacheServers.length; i++) {
        factory.addServer(cacheServers[i].getBindAddress(), cacheServers[i].getPort());
        cacheServersHostPort.append(cacheServers[i]);
        if (i != cacheServers.length - 1) {
          cacheServersHostPort.append(",");
        }
      }
      pool = factory.create(member.getId() + "-Pool");
      LogUtil.fine("Prepared Pool for member :" + memberName
          + " with client pool read timeout(ms) : " + socketReadTimeout
          + ", Cache Servers : " + cacheServersHostPort);
      this.connections.put(member.getId(), pool);
      return pool.getQueryService();
    }

    pool = this.connections.get(member.getId());
    QueryService service = pool.getQueryService();
    return service;
  }

  public void removeGemFireMemberListener(GemFireMemberListener listener) {
    this.memberListeners.remove(listener);
  }

  // TODO MGH - change the signature to eliminate the return value as it served
  // no purpose
  // The implementation does not adhere to the signature of this method. A
  // VersionMismatchException
  // will never be seen by a caller in a catch statement.
  // FIXIT
  protected void registerDSMembers() throws ConnectionClosedException,
      JMException, IOException, ConnectionFailureException {
    String[] params = {}, signature = {};

    List<ObjectName> result = new ArrayList<ObjectName>();

    // Execute manageSystemMemberApplications
    ObjectName[] rets = (ObjectName[]) invokeOperation("manageSystemMemberApplications", ds, params, signature);

    if (null != rets) {
      result.addAll(Arrays.asList(rets));
      LogUtil.info("Member Applications Added ==>" + result.size());
    }

    rets = (ObjectName[]) invokeOperation("manageCacheVms", ds, params, signature);

    if (null != rets) {
      result.addAll(Arrays.asList(rets));
      LogUtil.info("CacheVms Added ==>" + result.size());
    }

    for(ObjectName member : result) {
        registerDSMember(member); 
    }
  }
  
  
  protected void cleanupMember(String memberId, boolean isCrashed) {
    memberId = memberId.replaceAll("-", ":").replaceFirst(":", "-");
    Pool pool = this.connections.remove(memberId);
    if (pool != null && !pool.isDestroyed())
      pool.destroy();

    this.jmxinfo.remove(memberId);

    GemFireMember member = this.system_model.removeMember(memberId);
    if (member != null) {
      for (GemFireMemberListener listener : this.memberListeners) {
        if (isCrashed) {
          listener.memberEventReceived(new MemberCrashedEvent(member));
        } else {
          listener.memberEventReceived(new MemberLeftEvent(member));
        }
      }
    }
  }

  protected boolean registerDSMemberMbean(ObjectName member) throws Exception{
    List<CacheServerInfo> cacheServers = new ArrayList<CacheServerInfo>();
    MemberMXBean memberMbean = JMX.newMXBeanProxy(connServer, member, MemberMXBean.class);
    String memberId = memberMbean.getId();
    
    if(memberMbean.isServer()){
      for(ObjectName server : servers) {
        String serverName = server.getKeyProperty("member");
        LogUtil.fine("server: " + serverName);
        // check if the cache server belongs to this member
        if(serverName.equals(member.getKeyProperty("member"))){
          CacheServerMXBean cmbean = JMX.newMXBeanProxy(connServer, server, CacheServerMXBean.class);
          CacheServerInfo cs_info = new CacheServerInfo(
              getCacheServerAddress(cmbean, memberMbean), cmbean.getPort(), true);
          cacheServers.add(cs_info);
          LogUtil.fine("Server " + serverName + " on member " + memberMbean.getId() + 
              ": " + cs_info.toString());
        }
      }
      GemFireProperties props = memberMbean.listGemFireProperties();

      GemFireMember gfMember = MemberFactory.createGemFireMemberMbean(memberId, createConfiguration(props), cacheServers,
          createStatus(props, memberMbean), RegionFactory.getRootRegion(
              memberMbean.getMember(), distributedSystemMXBeanProxy, connServer,
              memberMbean.getRootRegionNames()));
      
      if (system_model.containsMember(memberId)) {
        LogUtil.info("Updating member :" + memberMbean.getMember());
        system_model.addMember(gfMember);

        for (GemFireMemberListener listener : this.memberListeners) {
          listener.memberEventReceived(new MemberUpdatedEvent(gfMember));
        }
      } else {
        LogUtil.info("Adding member :" + memberMbean.getMember());
        system_model.addMember(gfMember);

        for (GemFireMemberListener listener : this.memberListeners) {
          listener.memberEventReceived(new MemberJoinedEvent(gfMember));
        }
      }
      if(!isTerminated())
        jmxinfo.put(memberId, member);
    }
    
    return true;
  }
  
  private String getCacheServerAddress(CacheServerMXBean csmbean, MemberMXBean mbean) {
    // If cache server bind-address is set use that
    String cacheServerAddress = csmbean.getBindAddress();
    if (cacheServerAddress == null || cacheServerAddress.isEmpty()) {
      // Otherwise, if the system member bind-address is set, use that
      cacheServerAddress = mbean.listGemFireProperties().getBindAddress();
      if (cacheServerAddress == null || cacheServerAddress.isEmpty()) {
        // Default to mbean's host attribute 
        cacheServerAddress = mbean.getHost();
      }
    }
    return cacheServerAddress;
  }
  
  private List<MemberConfigurationPrms> createConfiguration(GemFireProperties props){
    List<MemberConfigurationPrms> confs = new ArrayList<MemberConfigurationPrms>();
    if(props.getSecurityClientAuthenticator() != null && !props.getSecurityClientAuthenticator().equals("")){
      MemberConfigurationPrms prm = new MemberConfigurationPrms();
      prm.setName("security-client-authenticator");
      prm.setValue(props.getSecurityClientAuthenticator());
      confs.add(prm);
    }
    return confs;
  }
  
  private MemberStatus createStatus(GemFireProperties props, MemberMXBean mbean){
    MemberStatus status = new MemberStatus();
    String bindAddress = props.getBindAddress();
    status.setBindAddress(bindAddress);
    try {
      // For host address: if bind-address is set, use that; otherwise use
      // member's host attribute from mbean 
      status.setHostAddress((bindAddress != null && bindAddress.length() > 0)
          ? InetAddress.getByName(bindAddress)
          : InetAddress.getByName(mbean.getHost()));
    } catch (UnknownHostException e) {
      LogUtil.warning("Unable to determine host address for the member with ID: " + mbean.getId(), e);
    }
    status.setMemberName(mbean.getName());
    status.setServerPort(props.getTcpPort());//not sure if this should be tcp
   
    return status;
    
  }
  
  protected boolean registerDSMember(ObjectName member)
      throws VersionMismatchException {
    String[] params = {}, signature = {};
    ObjectName cache;
    String memberID = member.getKeyProperty(MBEAN_PROPERTY_BEAN_ID);
    String type = member.getKeyProperty(MBEAN_PROPERTY_BEAN_TYPE);
  
    String methodLogPrefix = "JMXHelper registerDSMember [" + memberID + "]: ";
    LogUtil.fine(methodLogPrefix + "start");
  
    try {
      if (!MBEAN_GEMFIRE_APPLICATION_TYPE.equalsIgnoreCase(type) &&
    	  !MBEAN_GEMFIRE_CACHEVM_TYPE.equalsIgnoreCase(type)	  ) {
        LogUtil.warning("Member :"+memberID+" has an unknown type :"+type);
        return false;
      }
      
      boolean hasCache = ((Boolean)getAttribute(member, "hasCache")).booleanValue();
      if (hasCache) {
        LogUtil.info("Member Detected =>"+memberID);
        
        cache = (ObjectName) invokeOperation("manageCache", member,
            params, signature);
        GemFireMemberStatus snapshot = (GemFireMemberStatus) invokeOperation(
            "getSnapshot", cache, params, signature);
        // This is the only type we want.
        if (snapshot.getIsServer()) {
          RegionSubRegionSnapshot r_snapshot = (RegionSubRegionSnapshot) invokeOperation(
              "getRegionSnapshot", cache, params, signature);
          List<MemberConfigurationPrms> config_prms = getMemberConfiguration(member);
          List<CacheServerInfo> cs_info = getCacheServerInfo(cache);
  
          GemFireMember _member = MemberFactory.createGemFireMember(memberID,
              config_prms, cs_info, snapshot, r_snapshot);
  
          // If member exists already, just update it.
          if (system_model.containsMember(memberID)) {
            LogUtil.info("Updating member :" + _member.getName());
            system_model.addMember(_member);
  
            for (GemFireMemberListener listener : this.memberListeners) {
              listener.memberEventReceived(new MemberUpdatedEvent(_member));
            }
          } else {
            LogUtil.info("Adding member :" + _member.getName());
            system_model.addMember(_member);
  
            for (GemFireMemberListener listener : this.memberListeners) {
              listener.memberEventReceived(new MemberJoinedEvent(_member));
            }
          }
        } else {
          LogUtil.info("Skipping the member :" + memberID
              + " since it is not a cache server.");
        }
      }
    } catch (ConnectionClosedException e) {
      LogUtil.warning("Connection the GemFire system closed abruptly. "+e.getMessage());
      return false;
    } catch (VersionMismatchException ex) {
      LogUtil.error("Failed to register/update the member " + memberID, ex);
      throw ex;
    } catch (JMException ex) {
      LogUtil.error("Failed to register/update the member " + memberID, ex);
      return false;
    } catch (IOException ex) {
      LogUtil.error("Failed to register/update the member " + memberID, ex);
      return false;
    } finally {
      // Keep track of the ObjectName for this member, even in case of
      // exception.
      // If the problem is temporary, this will enable us to update the member.
      LogUtil.fine(methodLogPrefix + "end");
      if(!isTerminated())
       jmxinfo.put(memberID, member);
    }
  
    return true;
  }

  // TODO MGH - This method needs cleaning up. If we look or 5.8 and return
  private List<MemberConfigurationPrms> getMemberConfiguration(
      ObjectName objName) throws ConnectionClosedException {
    MBeanInfo info = getMBeanInfo(connServer, objName);
    List<MemberConfigurationPrms> result = new ArrayList<MemberConfigurationPrms>();
    MBeanOperationInfo op = getOperationInfo(info, "getConfiguration");

    // This operation is exposed in GemFire 5.8. If this operation is available,
    // then use it
    // Otherwise fetch these values from the MBean attributes (which is more
    // expensive than the operation).
    if (op != null) {
      String[] params = {}, signature = {};

      try {
        ConfigurationParameter[] paramters = (ConfigurationParameter[]) invokeOperation(
            "getConfiguration", objName, params, signature);
        for (int i = 0; i < paramters.length; i++) {
          MemberConfigurationPrms prm = new MemberConfigurationPrms();
          prm.setName(paramters[i].getName());
          prm.setValue(paramters[i].getValue());
          result.add(prm);
        }
        return result;
      } catch(ConnectionClosedException ex) {
        throw ex;
      
      } catch (Exception e) {
        LogUtil.warning("Unable to execute getConfiguration operation on "
            + objName, e);
      }
    }

    if (info != null) {
      MBeanAttributeInfo[] attrs = info.getAttributes();
      for (int i = 0; i < attrs.length; i++) {
        String name = attrs[i].getName();

        try {
          Object value = getAttribute(objName, attrs[i].getName());
          MemberConfigurationPrms prm = new MemberConfigurationPrms();
          prm.setName(name);
          prm.setValue(value);
          result.add(prm);
        } catch (Exception e) {
          LogUtil.warning("Unable to get the value of attribute"
              + attrs[i].getName() + " for object " + objName, e);
        }
      }
    }

    return result;
  }

  private List<CacheServerInfo> getCacheServerInfo(ObjectName cache) {
    List<CacheServerInfo> result = new ArrayList<CacheServerInfo>();
    String[] params = {}, signature = {};

    MBeanInfo          cacheMBeanInfo       = getMBeanInfo(connServer, cache);
    String         manageCacheServersOpName = "manageCacheServers";
    MBeanOperationInfo manageCacheServersOp = getOperationInfo(cacheMBeanInfo, manageCacheServersOpName);

    if (manageCacheServersOp != null) {
      try {
        //1. 1st attempt
        ObjectName[] cacheServers = (ObjectName[]) invokeOperation(manageCacheServersOpName, cache, params, signature);
        // Check if 2nd attempt is needed.
        if (cacheServers != null && cacheServers.length == 0) {        
          MBeanOperationInfo refreshOp = getOperationInfo(cacheMBeanInfo, "refresh");
  
          //2. Call refresh before 2nd attempt
          /* Refresh to ensure that SystemMemberCache MBean is refreshed before
           * reading notifyBySubscription from SystemMemberCacheServer MBean */
          if (refreshOp != null) {
            try {
              invokeOperation("refresh", cache, params, signature);
            } catch (Exception e) {
              LogUtil.warning("Unable to execute refresh operation on " + cache, e);
            }
            //3. 2nd attempt
            LogUtil.fine("Attempting again to invoke '" + manageCacheServersOpName + "' after refreshing Cache MBean");
            cacheServers = (ObjectName[]) invokeOperation(manageCacheServersOpName, cache, params, signature);
          }
        }

        for (int i = 0; i < cacheServers.length; i++) {
          try {
            String bindAddress = (String) getAttribute(cacheServers[i], "bindAddress");
            int    port        = ((Integer)getAttribute(cacheServers[i], "port")).intValue();
            boolean notifyBySubscription = ((Boolean)getAttribute(cacheServers[i], "notifyBySubscription")).booleanValue();

            CacheServerInfo cs_info = new CacheServerInfo(bindAddress, port,
                notifyBySubscription);
            result.add(cs_info);

          } catch (Exception ex) {
            LogUtil.warning(
                "Unable to retrieve information about the cacheServer :"
                    + cacheServers[i], ex);
          }
        }

        return result;
      } catch (Exception e) {
        LogUtil.warning("Unable to execute manageCacheServers operation on "
            + cache, e);
      }
    }

    return result;
  }
  
  private void establishConnectionUsingJMXManager(String hostName, int port, String userName, String password)throws ConnectionFailureException{
    try{
      Map<String, Object> env = new HashMap<String, Object>();
      if (userName != null && userName.length() > 0) {
        env.put(JMXConnector.CREDENTIALS, new String[]{userName, password});
      }
      if (System.getProperty("javax.net.ssl.keyStore") != null || System.getProperty("javax.net.ssl.trustStore") != null) {
        // use ssl to connect
        env.put("com.sun.jndi.rmi.factory.socket", new SslRMIClientSocketFactory());
      }

      int timeOut = new Long(DataBrowserPreferences.getConnectionTimeoutInterval()).intValue();
      String jmxManagerHost = null;
      int jmxManagerPort = 0;
      try {
        JmxManagerLocatorResponse locRes = JmxManagerLocatorRequest.send(hostName, port, timeOut);
        jmxManagerHost = locRes.getHost();
        jmxManagerPort = locRes.getPort();
      }
      catch (Exception e) {
        LogUtil.error("Exception while connecting to Locator on host: " + hostName + " and port: " + port+ " : " + e);
        LogUtil.info("Trying to connect to a JMX Manager on host: "+ hostName + " and port: " + port);
        //try to connect JMX Manager on same host
        jmxManagerHost = hostName;
        jmxManagerPort = port;
      }
      
      if(jmxManagerHost == null || jmxManagerPort == 0){
        throw new ConnectionFailureException("JMX manager not found");
      }

      String url = MessageFormat.format(JMX_URI_MANAGER, new Object[] {checkAndConvertToCompatibleIPv6Syntax(jmxManagerHost),
          String.valueOf(jmxManagerPort) });
      JMXServiceURL jmxurl = new JMXServiceURL(url);
    
      connector = JMXConnectorFactory.connect(jmxurl, env);
      connServer = connector.getMBeanServerConnection();
      connector.connect();
      LogUtil.info("Connected to the Gemfire distributed system");
      ds = MBeanJMXAdapter.getDistributedSystemName();
      distributedSystemMXBeanProxy = JMX.newMXBeanProxy(connServer, ds, DistributedSystemMXBean.class);
      ObjectName managerMemberObjectName = null;
      if (distributedSystemMXBeanProxy == null || !JMX.isMXBeanInterface(DistributedSystemMXBean.class)) {
        connector.close();
        throw new JMXConnectionException(JMXConnectionException.MANAGER_NOT_FOUND_EXCEPTION);
      }  else {
        managerMemberObjectName = distributedSystemMXBeanProxy.getMemberObjectName();
        if (managerMemberObjectName == null || !JMX.isMXBeanInterface(MemberMXBean.class)) {
          connector.close();
          throw new JMXConnectionException(JMXConnectionException.MANAGER_NOT_FOUND_EXCEPTION);
        } else {
          managerMemberMXBeanProxy = JMX.newMXBeanProxy(connServer, managerMemberObjectName, MemberMXBean.class);
        }
      }
      connector.addConnectionNotificationListener(this, null, null);
      servers = distributedSystemMXBeanProxy.listCacheServerObjectNames();
      if(servers.length == 0){
        LogUtil.info("No cacheservers found in the distributed system");
      }
      ObjectName[] members = distributedSystemMXBeanProxy.listMemberObjectNames();
      if(members.length == 0){
        LogUtil.info("No members found in the distributed system");
      }
      for(ObjectName member : members) {
        registerDSMemberMbean(member);
      }
    } catch(Exception e){
      throw new ConnectionFailureException(e);
    }
    
  }

  // JMX Connection establishment.
  private void establishConnection(String hostName, int port)
      throws ConnectionFailureException {
    try {
      String url = MessageFormat.format(JMX_URI, new Object[] {checkAndConvertToCompatibleIPv6Syntax(hostName),
          String.valueOf(port) });
      JMXServiceURL jmxurl = new JMXServiceURL(url);
      connector = JMXConnectorFactory.connect(jmxurl, null);
      connServer = connector.getMBeanServerConnection();

      String[] domains = connServer.getDomains();
      for (int i = 0; i < domains.length; i++) {
        LogUtil.info("DOMAIN :" + domains[i]);
        if (MBEAN_DOMAIN_GEMFIRE_NAME.equals(domains[i])) {
          Set<ObjectName> objectNames = connServer.queryNames(null,
              new ObjectName(MBEAN_DOMAIN_GEMFIRE_NAME + ":*"));
          for (ObjectName obj : objectNames) {
            String type = obj.getKeyProperty(MBEAN_PROPERTY_BEAN_TYPE);
            if (MBEAN_AGENT_TYPE.equalsIgnoreCase(type)) {
              agent = obj;
              break;
            }
          }
        }
      }

      if (agent == null) {
        throw new ConnectionFailureException(
            "Could not find the JMX Agent MBean...");
      }

      LogUtil.info("AGENT :" + agent);

      String[] params = {}, signature = {};
      ds = (ObjectName) invokeOperation("connectToSystem", agent, params, signature);

      if (ds == null) {
        throw new ConnectionFailureException(
            "Could not connect to GemFire Distributed System...");
      }

      connector.addConnectionNotificationListener(this, null, null);

      registerDSMembers();
    } catch (VersionMismatchException e) {
      throw e;
    } catch (ConnectionFailureException e) {
      throw e;
    } catch (ConnectionClosedException e) {
      throw new ConnectionFailureException(e, false);
    } catch (JMException e) {
      throw new ConnectionFailureException(e, false);
    } catch (IOException e) {
      throw new ConnectionFailureException(e, true);
    }
  }
  
  // JMX Operation invocation.
  public Object invokeOperation(String name, ObjectName objName, Object[] params, String[] signature)
      throws VersionMismatchException, ConnectionClosedException, JMException, IOException {

    Object result;
    try {
      LogUtil.fine("JMXHelper:JMXOperation [" + name + ", on " + objName
          + "]: started");
      
      if(isTerminated()) {
//        LogUtil.warning("Underlaying GemFire/JMX connection is closed. Can not proceed with the JMX operation [."+name+"] execution.");
        throw new ConnectionClosedException("Underlaying GemFire/JMX connection is closed. Can not proceed with the JMX operation [."+name+"] execution.");
      }
      
      result = connServer.invoke(objName, name, params, signature);
    } catch (IOException ex1) {
      Throwable cause = ex1.getCause();
      // while ((cause != null) && !(cause instanceof InvalidClassException))
      while ((null != cause)
          && !(cause.getClass().equals(InvalidClassException.class))) {
        cause = cause.getCause();
      }

      if(( null != cause ) && cause.getClass().equals(InvalidClassException.class)) {
        throw new VersionMismatchException(ex1);
      }

      throw ex1;
    
    } finally {
      LogUtil.fine("JMXHelper:JMXOperation [" + name + ", on " + objName + "]: ended");  
    }
    
    return result;
  }
  
  protected Object getAttribute(ObjectName objName, String name)
      throws VersionMismatchException, ConnectionClosedException, JMException,
      IOException {
    Object result;

    try {
      LogUtil.fine("JMXHelper::getAttribute [" + name + ", on " + objName + "]: started");
      
      if(isTerminated()) {
        LogUtil.warning("Underlaying GemFire/JMX connection is closed. Can not retrieve the JMX attribute [."+name+"].");
        throw new ConnectionClosedException();
      }
      
      result = connServer.getAttribute(objName, name);

    } catch (IOException e) {
      Throwable cause = e.getCause();
      while ((null != cause)
          && !(cause.getClass().equals(InvalidClassException.class))) {
        cause = cause.getCause();
      }

      if ((null != cause)
          && cause.getClass().equals(InvalidClassException.class)) {
        throw new VersionMismatchException(e);
      }

      throw e;
    } finally {
      LogUtil.fine("JMXHelper::getAttribute [" + name + ", on " + objName + "]: ended");
    }

    return result;
  }

  // TODO MGH - Fix this awkward code. Why the finally statement when the
  // exception is eaten?
  protected static MBeanInfo getMBeanInfo(MBeanServerConnection mbsc,
      ObjectName objName) {
    MBeanInfo result = null;

    try {
      result = mbsc.getMBeanInfo(objName);
    } catch (Exception e) {
      LogUtil.warning("Unable to get MBeanInfo for object :" + objName, e);
    }

    return result;
  }

  protected static MBeanOperationInfo getOperationInfo(MBeanInfo info,
      String name) {
    MBeanOperationInfo[] operations = info.getOperations();
    for (int i = 0; i < operations.length; i++) {
      if (operations[i].getName().equals(name))
        return operations[i];
    }

    return null;
  }

  public static String getMemberID(ObjectName obj) {
    return obj.getKeyProperty(MBEAN_PROPERTY_BEAN_ID);
  }
  
  public static String getMemberMbeanID(ObjectName obj) {
    return obj.getKeyProperty("member");
  }

  public void handleNotification(Notification notification, Object handback) {
    if (notification instanceof JMXConnectionNotification) {
      String type = notification.getType();
      if (JMXConnectionNotification.CLOSED.equalsIgnoreCase(type)
          || JMXConnectionNotification.FAILED.equalsIgnoreCase(type)) {
        
        LogUtil.warning("RECEIVED JMXConnectionNotification of type " + type
            + ".");

        terminated = true;
        connServer = null;

        if (connector != null) {
          try {
            connector.removeConnectionNotificationListener(this);
          } catch (ListenerNotFoundException e) { // This is impossible to
            // happen.
            LogUtil.warning(e.getLocalizedMessage());
          }
        }

        for (GemFireConnectionListener listener : this.connectionListeners) {
          LogUtil.info("CLOSE_LISTENER :" + listener.getClass());
          listener.connectionEventReceived(new ConnectionTerminatedEvent());
        }

      }
    }

  }
  
  public String getGemFireSystemVersion() {
    String result = "NO_VERSION";
    
    try {
     // result = (String)this.connServer.getAttribute(agent, "version");
      result = managerMemberMXBeanProxy.getVersion();
    } catch (Exception e) {
      LogUtil.warning("Failed to get the GemFire System version information...", e);
      return result;
    } 
    
    String temp = result;
    int index = temp.indexOf("Native version");
    
    if(index > 0) {
      temp = result.substring(0, index);
    }
    
    String [] arr = temp.split("\\s+");
    
    if(arr.length < 3)
     return result; 
    else
     return arr[2];
  }
  
  /** 
   * If the given host address contains a ":", considers it as an IPv6 address & 
   * returns the host based on RFC2732 requirements i.e. surrounds the given 
   * host address string with square brackets. If ":" is not found in the given 
   * string, simply returns the same string. 
   *  
   * @param hostAddress 
   *          host address to check if it's an IPv6 address 
   * @return for an IPv6 address returns compatible host address otherwise 
   *         returns the same string 
   */ 
   public static String checkAndConvertToCompatibleIPv6Syntax(String hostAddress) { 
     //if host string contains ":", considering it as an IPv6 Address 
     //Conforming to RFC2732 - http://www.ietf.org/rfc/rfc2732.txt 
    if (hostAddress.indexOf(":") != -1) { 
     LogUtil.info("IPv6 host address detected, using IPv6 syntax for host in JMX connection URL"); 
     hostAddress = "[" + hostAddress + "]"; 
     LogUtil.info("Compatible host address is : "+hostAddress); 
    } 
    return hostAddress; 
  } 
   
  public long getRefreshInterval() {
    return this.refreshInterval;
  } 

  public void setRefreshInterval(long time) {
    if(this.refreshInterval != time) {
      LogUtil.info("Updated the refreshInterval to "+time);
      this.refreshInterval = time;
      
      if(null != this.updateTimer)
       this.updateTimer.cancel();
      
      UpdateTask task = new UpdateTask();
      this.updateTimer = new Timer("DataBrowser-MemberUpdator", true);    
      this.updateTimer.schedule(task, refreshInterval, refreshInterval);
    
    } else {
      LogUtil.info("refreshInterval is same. Hence not updating...");
    }
  }
  
  private class UpdateTask extends TimerTask {
    @Override
    public void run() {
      LogUtil.info("Updating GemFire system members.");
      try {
        List<ObjectName> members = new ArrayList<ObjectName>();
        synchronized (JMXDiscoveryImpl.this.jmxinfo) {
          members.addAll(JMXDiscoveryImpl.this.jmxinfo.values());
        }

        for (ObjectName member : members) {
         if(isTerminated()) {
           LogUtil.warning("Since the underlaying GemFire/JMX connection is closed. Hence the member update task is complete");
           return;
         }
          registerDSMemberMbean(member);
        }
      } catch (Exception e1) {
        LogUtil.error("Failed to get information about GemFire system members.",  e1);
        return;
      }
      LogUtil.info("Updated GemFire system members.");     
    }
  }
  
}
