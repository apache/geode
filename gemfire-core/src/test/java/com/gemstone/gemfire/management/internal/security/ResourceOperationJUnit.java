package com.gemstone.gemfire.management.internal.security;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.security.ResourceOperationContext.ResourceOperationCode;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.junit.UnitTest;

@Category(UnitTest.class)
public class ResourceOperationJUnit  extends TestCase {
	
	public static class TestUsernamePrincipal implements Principal,
			Serializable {

		private final String userName;

		public TestUsernamePrincipal(String userName) {
			this.userName = userName;
		}

		public String getName() {
			return this.userName;
		}

		@Override
		public String toString() {
			return this.userName;
		}

	}

	public static class TestAuthenticator implements Authenticator {

		@Override
		public void close() {

		}

		@Override
		public void init(Properties securityProps, LogWriter systemLogger,
				LogWriter securityLogger) throws AuthenticationFailedException {

		}

		@Override
		public Principal authenticate(Properties props, DistributedMember member)
				throws AuthenticationFailedException {
			String user = props.getProperty(ManagementInterceptor.USER_NAME);
			String pwd = props.getProperty(ManagementInterceptor.PASSWORD);
			if (user!=null && !user.equals(pwd) && !"".equals(user))
				throw new AuthenticationFailedException(
						"Wrong username/password");
			System.out.println("Authentication successful!! for " + user);
			return new TestUsernamePrincipal(user);
		}

	}
	
	public static class TestAccessControl implements AccessControl {

		private Principal principal=null;
		@Override
		public void close() {
			
		}

		@Override
		public void init(Principal principal, DistributedMember remoteMember,
				Cache cache) throws NotAuthorizedException {
			this.principal = principal;
		}

		@Override
		public boolean authorizeOperation(String regionName,
				OperationContext context) {
			if(principal.getName().equals("tushark")) {				
				ResourceOperationCode authorizedOps[] = {
						ResourceOperationCode.LIST_DS,
						ResourceOperationCode.READ_DS,
						ResourceOperationCode.CHANGE_ALERT_LEVEL_DS,
						ResourceOperationCode.LOCATE_ENTRY_REGION
				};
				
				System.out.println("Context received " + context);
				
				//if(context instanceof JMXOperationContext) {
					ResourceOperationContext ctx = (ResourceOperationContext)context;
					System.out.println("Checking for code " + ctx.getResourceOperationCode());
					boolean found = false;
					for(ResourceOperationCode code : authorizedOps) {
						if(ctx.getResourceOperationCode().equals(code)){
							found =true;
							System.out.println("found code " + code.toString());
							break;
						}							
					}
					if(found)
						return true;
					System.out.println("Did not find code " + ctx.getResourceOperationCode());
					return false;
				//}
			}			
			return false;
		}
		
	}
	
	public void testJMXOperationContext() {		
		System.setProperty("resource-auth-accessor", "com.gemstone.gemfire.management.internal.security.ResourceOperationJUnit$TestAccessControl");
		System.setProperty("resource-authenticator", "com.gemstone.gemfire.management.internal.security.ResourceOperationJUnit$TestAuthenticator");
		GemFireCacheImpl cache = null;
		DistributedSystem ds = null;
		Properties pr = new Properties();
		pr.put("name", "testJMXOperationContext");
		pr.put(DistributionConfig.JMX_MANAGER_NAME, "true");
		pr.put(DistributionConfig.JMX_MANAGER_START_NAME, "true");
		int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
		pr.put(DistributionConfig.JMX_MANAGER_PORT_NAME, String.valueOf(port));
		pr.put(DistributionConfig.HTTP_SERVICE_PORT_NAME, "0");
		ds = getSystem(pr);
		cache = (GemFireCacheImpl) CacheFactory.create(ds);
		ObjectName name = MBeanJMXAdapter.getDistributedSystemName();
		
		String[] methods = {
				"listCacheServerObjectNames",
				"viewRemoteClusterStatus",
				"getTotalHeapSize",
				"setQueryCollectionsDepth",
				"getQueryCollectionsDepth",
				"changeAlertLevel",
				"backupAllMembers",
				"revokeMissingDiskStores",
				"shutDownAllMembers",
				"queryData",
				"queryDataForCompressedResult",
				"setQueryResultSetLimit",				
		};
		
		ResourceOperationCode expectedCodes[] = {
				ResourceOperationCode.LIST_DS,
				ResourceOperationCode.LIST_DS,
				ResourceOperationCode.READ_DS,
				ResourceOperationCode.QUERYDATA_DS,
				ResourceOperationCode.READ_DS,
				ResourceOperationCode.CHANGE_ALERT_LEVEL_DS,
				ResourceOperationCode.BACKUP_DS,
				ResourceOperationCode.REMOVE_DISKSTORE_DS,
				ResourceOperationCode.SHUTDOWN_DS,
				ResourceOperationCode.QUERYDATA_DS,
				ResourceOperationCode.QUERYDATA_DS,
				ResourceOperationCode.QUERYDATA_DS
		};
				
		for(int i=0;i<methods.length;i++) {
			String methodName = methods[i];
			JMXOperationContext context = new JMXOperationContext(name, methodName);
			assertEquals(expectedCodes[i],
					context.getResourceOperationCode());
			assertEquals(OperationCode.RESOURCE, context.getOperationCode());
		}
		
		JMXConnector cs = getGemfireMBeanServer(port, "tushark", "tushark");;
		MBeanServerConnection mbeanServer =null;
		try {
			mbeanServer = cs.getMBeanServerConnection();
			mbeanServer.invoke(MBeanJMXAdapter.getDistributedSystemName(), "listCacheServerObjectNames", null, null);
			String oldLevel = (String)mbeanServer.getAttribute(MBeanJMXAdapter.getDistributedSystemName(), "AlertLevel");
			System.out.println("Old Level = " + oldLevel);
			mbeanServer.invoke(MBeanJMXAdapter.getDistributedSystemName(), "changeAlertLevel", new Object[]{"WARNING"},new String[]{
				String.class.getCanonicalName()
			});
			String newLevel = (String)mbeanServer.getAttribute(MBeanJMXAdapter.getDistributedSystemName(), "AlertLevel");
			System.out.println("New Level = " + newLevel);
			
			
			//Checking accessControlMXBean
			System.out.println("Checking access via AccessControlMbean");			
			ResourceOperationCode authorizedOps[] = {
          ResourceOperationCode.LIST_DS,
          ResourceOperationCode.READ_DS,
          ResourceOperationCode.CHANGE_ALERT_LEVEL_DS,
          ResourceOperationCode.LOCATE_ENTRY_REGION
      };
			ObjectName accControlON = new ObjectName(ManagementInterceptor.OBJECT_NAME_ACCESSCONTROL);
			for(ResourceOperationCode c : authorizedOps) {
			  boolean result = (Boolean) mbeanServer.invoke(accControlON, "authorize"
	          , new Object[]{ResourceOperationCode.CHANGE_ALERT_LEVEL_DS.toString()}
	          , new String[]{String.class.getCanonicalName()}); 
	      assertTrue(result);
			}
			
			boolean result = (Boolean) mbeanServer.invoke(accControlON, "authorize"
          , new Object[]{ResourceOperationCode.ADMIN_DS.toString()}
          , new String[]{String.class.getCanonicalName()}); 
      assertFalse(result);			
			
		} catch (InstanceNotFoundException e1) {
		  e1.printStackTrace();
			fail("Error while invoking JMXRMI " + e1.getMessage());
		} catch (MBeanException e1) {
		  e1.printStackTrace();
			fail("Error while invoking JMXRMI " + e1.getMessage());
		} catch (ReflectionException e1) {
			fail("Error while invoking JMXRMI " + e1.getMessage());
		} catch (IOException e1) {
			fail("Error while invoking JMXRMI " + e1.getMessage());
		} catch (AttributeNotFoundException e) {
			fail("Error while invoking JMXRMI" + e.getMessage());
		} catch (MalformedObjectNameException e) {
		  fail("Error while invoking JMXRMI" + e.getMessage());
    }
		
		try {
			mbeanServer.invoke(MBeanJMXAdapter.getDistributedSystemName(),
					"backupAllMembers", 
					new Object[]{"targetPath","baseLinePath"}, 
					new String[]{String.class.getCanonicalName(), String.class.getCanonicalName()});
			fail("Should not be authorized for backupAllMembers");
		} catch (SecurityException e) {
			//expected
		} catch(Exception e){
		  e.printStackTrace();
			fail("Unexpected exception : " + e.getMessage());
		}
		
		try {
			mbeanServer.invoke(MBeanJMXAdapter.getDistributedSystemName(),
					"shutDownAllMembers",null,null);
			fail("Should not be authorized for shutDownAllMembers");
		} catch (SecurityException e) {
			//expected
		} catch(Exception e){
			fail("Unexpected exception : " + e.getMessage());
		}
		
		checkCLIContext(mbeanServer);
		
		try {
			cs.close();
		} catch (IOException e) {
			fail("Unexpected exception : " + e.getMessage());
		}
		
		
		
		
		cache.close();
		ds.disconnect();
	}
	
  private void checkCLIContext(MBeanServerConnection mbeanServer) {
    DistributedSystemMXBean proxy = JMX.newMXBeanProxy(mbeanServer, MBeanJMXAdapter.getDistributedSystemName(),
        DistributedSystemMXBean.class);
    ObjectName managerMemberObjectName = proxy.getMemberObjectName();
    MemberMXBean memberMXBeanProxy = JMX.newMXBeanProxy(mbeanServer, managerMemberObjectName, MemberMXBean.class);
    try {
      Map<String,String> map = new HashMap<String,String>();
      map.put("APP","GFSH");
      String result = memberMXBeanProxy.processCommand("locate entry --key=k1 --region=/region1", map);
      System.out.println("Result = " + result);
    } catch (Exception e) {
      System.out.println("Excpetion e " + e.getMessage());
      e.printStackTrace();
    }
  }

	public void testCLIOperationContext() {	
		System.setProperty("resource-auth-accessor", "com.gemstone.gemfire.management.internal.security.ResourceOperationJUnit$TestAccessControl");
		System.setProperty("resource-authenticator", "com.gemstone.gemfire.management.internal.security.ResourceOperationJUnit$TestAuthenticator");
		GemFireCacheImpl cache = null;
		DistributedSystem ds = null;
		Properties pr = new Properties();
		pr.put("name", "testJMXOperationContext");
		pr.put(DistributionConfig.JMX_MANAGER_NAME, "true");
		pr.put(DistributionConfig.JMX_MANAGER_START_NAME, "true");
		int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
		pr.put(DistributionConfig.JMX_MANAGER_PORT_NAME, String.valueOf(port));
		pr.put(DistributionConfig.HTTP_SERVICE_PORT_NAME, "0");
		ds = getSystem(pr);
		cache = (GemFireCacheImpl) CacheFactory.create(ds);
		
		String[] commands = {
				"put --key=k1 --value=v1 --region=/region1",
				"locate entry --key=k1 --region=/region1",
				"query --query=\"select * from /region1\"",
				"export data --region=value --file=value --member=value",
				"import data --region=value --file=value --member=value",
				"rebalance"
		};
		
		ResourceOperationCode expectedCodes[] = {
				ResourceOperationCode.PUT_REGION,
				ResourceOperationCode.LOCATE_ENTRY_REGION,
				ResourceOperationCode.QUERYDATA_DS,
				ResourceOperationCode.EXPORT_DATA_REGION,
				ResourceOperationCode.IMPORT_DATA_REGION,
				ResourceOperationCode.REBALANCE_DS
		};
		
		for(int i=0;i<commands.length;i++){
			CLIOperationContext ctx = new CLIOperationContext(commands[i]);
			System.out.println("Context " + ctx);
			assertEquals(expectedCodes[i],ctx.getResourceOperationCode());
		}
		
		cache.close();
		ds.disconnect();
	}
	
	
	
	private JMXConnector getGemfireMBeanServer(int port, String user, String pwd) {
		JMXServiceURL url;
		try {
			url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:"+ port +"/jmxrmi");
			if(user!=null){
				Map env = new HashMap();
			    String[] creds = {user, pwd};
			    env.put(JMXConnector.CREDENTIALS, creds);
			    JMXConnector jmxc =  JMXConnectorFactory.connect(url,env);
			    return jmxc;
			} else {
				JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
				return jmxc;
			}
		} catch (MalformedURLException e) {
			fail("Error connecting to port=" + port  + " " + e.getMessage());
		} catch (IOException e) {
			fail("Error connecting to port=" + port  + " " + e.getMessage());
		}
		return null;
	}



	private static DistributedSystem getSystem(Properties properties) {
	    return DistributedSystem.connect(properties);
	  }

}
