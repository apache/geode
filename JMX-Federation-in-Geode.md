### JMX Federation in Geode

This document explains how Geode JMX federation works and some internal mechanisms to get developers get started. Basic JMX knowledge is required to go through this document.

#### JMX Federation overview:

Geode defines number of MXBeans which can be used to monitor and manage Geode cluster. 
For list of MXBeans which are exposed by Geode see the link [Geode Documentation](http://gemfire.docs.pivotal.io/latest/userguide/index.html#managing/management/list_of_gemfire_mbeans_full.html#topic_48194A5BDF3F40F68E95A114DD702413)

These MXBeans follows standard JMX structure and protocols. For simplicity rest of the document will mention MBean instead of MXBean.

##### The concept of Single-Agent View
With a single-agent view, the application interacts with one MBeanServer. Application  development is much easier because they don't have to find the right MBeanServer to make a request on an MBean. One MBeanServer is responsible for aggregating all of the MBeans in the MBeanServers with which it is federated. This is very convenient because the application  can interact with a local MBeanServer that has services and adapters to interact with other local or remote MBeanServers. The location of the federated MBeanServers does not change how the application or manager is programmed to interact with its MBeanServer. 

For the sake of this discussion, we will refer to the single-agent MBeanServer as the Manager Node. The MBeanServers that the master interacts with (and the application does not) will be referred to as Managed Nodes .A distributed system may have one or more Manager nodes. It is advised to keep only one Manager node. All other nodes in the system are Managed nodes. Manager node is responsible for maintaining  proxies of all MBeans defined in other nodes. In addition there are some local MBeans defined in Manager node.

The following diagram shows how all MBeans are accessible from a master( i.e. Manager)  node.


[[images/Figure9.13.jpg]]



Apart from ease of use,  other goals in mind for Single-Agent view  were 
* It should be scalable.
* Performance impact of federation should be minimal. 
* User should be able to monitor the Distributed System from any other third party tool, compliant with JMX.


#### Federation Internals


##### Federation In Picture


[[images/Overview.png]]



##### Unique IDs for MBeans and Proxies
Each MBean from each node has a proxy counterpart with the Manager. This needs a unique identification mechanism. For this each Geode MBean is given a unique ObjectName. e.g. GemFire:type=Member,member=<name-or-dist-member-id> or GemFire:type=Member,service=Region,name=<regionName>,member=<name-or-dist-member-id>.
The <name-or-dist-member-id> part in ObjectName identifies a resource uniquely if the resource is distributed across members. For example a DistributedRegion or a PartitionedRegion.


            
##### MBean state
Most of the MBean state (not all) is maintained by implementing ****com.gemstone.gemfire.internal.statistics.StatisticsListener**** interface. This interface is a clean way to listen for any statistic update. Each Member periodically replicates all MBean states to Manager node/s. Rather than using JMX protocol we use internal Geode support for replication. Two system internal replicated regions are defined for this. ObjectName of the MBean being the key of this region and ****com.gemstone.gemfire.management.internal.FederationComponent**** is the value. 
Each MBean is represented by one FederationComponent. This component is optimized for serialization as it implements DataSerializable & DataSerializableFixedID.All Managed nodes replicate a set of FederationComponents to the Manager node/s.

At Manager node side FederationComponents are wrapped by a proxy handler which has the intelligence to delegate MBean state access to stored FederationComponents.

Following sequence diagram shows how a getter method on MBean is invoked on Manager side.

[[images/getter.png]]
          
Some of the MBean data changes very rarely or never. For example region definition. Such kind of data access and other MBean operations are delegated to FunctionService to fetch data from the respective node. The proxy handler for FederationComponents know where to delegate such a request. Also all setters on MBeans follows the same code path.

[[images/setter.png]]

##### Custom Data types
As MXBeans support custom data types , Geode proxy layer needs to be aware of different open data types like javax.management.openmbean.CompositeData and javax.management.openmbean.TabularData. Remember Geode proxy layer does not use JMX. Hence there was a need for a conversion layer which can understand OpenData types.
****com.gemstone.gemfire.management.internal.MXBeanProxyInvocationHandler**** is the class which handles conversion of all open data types as understood by JMX layer.

##### JMX connector
Manager node in addition to having proxies from all the nodes also opens up a JMX connector. Any JMX compliant client like JConsole can connect to it and do JMX operations on all the MBean proxies irrespective of whether that MBean belongs to local Manager node or a remote Managed node.
The default port on which the connector opens is 1099. User can change it by specifying "jmx-manager-port" cache config attribute while starting a manager.

A remote client can connect to exposed connector by standard JMX protocols. Code will change a bit depending on whether the JMX client has access to Geode jars or not.

    Map env = new HashMap();
    String[] creds = {"user", "pwd"};
    env.put(JMXConnector.CREDENTIALS, creds);

    JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:1099/jmxrmi");
    JMXConnector jmxc = JMXConnectorFactory.connect(url, env);
    MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
			
    ObjectName distSystemMBean = new ObjectName("GemFire:service=System,type=Distributed");

    //With client having access to Geode jar
    DistributedSystemMXBean dsMbean = JMX.newMBeanProxy(mbsc, distSystemMBean, DistributedSystemMXBean.class, true);
    String[] members = dsMbean.listMembers();

    //With client NOT having access to Geode jar
    String[] members = (String[])mbsc.invoke(distSystemMBean, "listMembers", null, null);

##### Java API 
Users also can access all the MBeans and proxies by Geode APIs. The main entry point for using MBeans is com.gemstone.gemfire.management.ManagementService.

    //Example Java API calls on a Manager node. For detailed APIs please see Geode javadocs.
    //To get hold of a ManagementService instance 
    ManagementService service = ManagementService.getManagementService(cache);

    //Access a local RegionMBean

    service.getLocalRegionMBean(regionPath);

    //Query MBeans from a Member.
    DistributedMember member = someMember;

    Set<ObjectName> objectNamesOfMember = service.queryMBeanNames(member);

    ObjectName objectName = objectNamesOfMember.get(0);//Lets assume we want to invoke some operation on the first MBean

    //Access an MBean instance or proxy
    Class  interfaceClass = RegionMXBean.class;
    RegionMXBean bean = service.getMBeanInstance(objectName, interfaceClass);// bean can be a local MBean or a proxy MBean depending on the ObjectName.




##### Notifications
All notifications originating from a Managed node MBean also gets propagated to Manager node and emitted against the corresponding proxy MBean. A notification is immediate and the client can assume to get a notification almost at the same time when it originated. There will be some network latency, but that should be acceptable for most real life monitoring systems.

A JMX client can register any notification handler even with a proxy MBean on Manager and it will get all the notifications from the corresponding Managed node MBean.
All Geode alerts are also wrapped in JMX notifications and emitted through Manager. Please see the following URL for more information [System Alerts] (http://gemfire.docs.pivotal.io/latest/userguide/index.html#getting_started/quickstart_examples/system_alerts_jmx.html )


See the following example to see how a notification listener is registered for a MBean and proxy. In both the cases the code remains same.

    //Example Java API calls on a Manager node. For detailed APIs please see Geode javadocs.
    //To get hold of a ManagementService instance 
    ManagementService service = ManagementService.getManagementService(cache);


    //Query MBeans from a Member.
    DistributedMember member = someMember;

    Set<ObjectName> objectNamesOfMember = service.queryMBeanNames(member);

    ObjectName objectName = objectNamesOfMember.get(0);//Lets assume we want to register a notification handler on the first MBean



    MBeanServer mbsc = ManagementFactory.getPlatformMBeanServer();

    mbsc.addNotificationListener(objectName, listener, null, null);//Listener can be any class implementing javax.management.NotificationListener.



##### Manager Node
Any node of Geode can act as a Manager provided it has been allowed to act as a Manager.    
Following cache config property determines whether a node starts as a manager.
* jmx-manager : Identifies a node as a potential manager , but does not start the manager. 
* jmx-manager-start : Start as a manager when the node starts.



##### Manager Node Transition
Only potential managers can be transitioned into a manager at run time. All locators are potential manager by default. Users can disable a Locator to become a manager by specifying jmx-manager=false while starting a locator.
When GFSH or Pulse tries to connect to a Locator , it transitions self as a Manager and start to build up proxies for all the MBeans. If Locator has been disabled to act as a Manager, it will try to look up for any potential Managers in the system and mandate any one of them to act as a Manager.

Users can also manually transition any node to Manager by invoking ManagerMXBean.startManager(). But as no Manager will be present before doing this they have to access the local node to do so.


##### Overhead of Manager nodes 
Manager has been designed to be as less resource consuming as possible by using proven Geode serialization and replication protocols. However some memory and CPU consumption is obvious. So it is advisable to keep Manager nodes separate from Data or Server nodes.

##### Fail over of Manager nodes
If a manager node stops/crashes all JMX clients connected to the manager gets disconnected. However GFSH and Pulse has inbuilt mechanism to fail over to a new manager. GFSH and Pulse ask the locator for another Manager end point and connect with it. Normal JMX clients have to do it manually.