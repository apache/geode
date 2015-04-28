package com.gemstone.gemfire.management.internal.security;

import java.lang.management.ManagementFactory;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXPrincipal;
import javax.management.remote.MBeanServerForwarder;
import javax.security.auth.Subject;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.Authenticator;

@SuppressWarnings("rawtypes")
public class ManagementInterceptor implements JMXAuthenticator {

	public static final String USER_NAME = "security-username";
	public static final String PASSWORD = "security-password";
	public static final String OBJECT_NAME_ACCESSCONTROL = "GemFire:service=AccessControl,type=Distributed";
	private MBeanServerWrapper mBeanServerForwarder;
	private Logger logger;  

	public ManagementInterceptor(Logger logger) {
		this.logger = logger;		
		this.mBeanServerForwarder = new MBeanServerWrapper(this);
		registerAccessContorlMbean();
		LogService.getLogger().info("Starting management interceptor");
	}

	private void registerAccessContorlMbean() {    
    try {
      com.gemstone.gemfire.management.internal.security.AccessControl acc = new com.gemstone.gemfire.management.internal.security.AccessControl(this);
      ObjectName name = new ObjectName(OBJECT_NAME_ACCESSCONTROL);
      MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
      Set<ObjectName> names = platformMBeanServer.queryNames(name, null);
      if(names.isEmpty()) {
        try {
          platformMBeanServer.registerMBean(acc, name);
          logger.info("Registered AccessContorlMBean on " + name);
        } catch (InstanceAlreadyExistsException e) {
          throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource",e);
        } catch (MBeanRegistrationException e) {
          throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource",e);
        } catch (NotCompliantMBeanException e) {
          throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource",e);
        }
      }
    } catch (MalformedObjectNameException e) {      
      e.printStackTrace();
    }
  }

  @Override
	public Subject authenticate(Object credentials) {
		String username = null, password = null;
		if (!(credentials instanceof String[])) {
			// Special case for null so we get a more informative message
			if (credentials == null) {
				// throw new SecurityException("Credentials required");
				username = "empty";
				password = "emptypwd";
			}
			// throw new SecurityException("Credentials should be String[]");
			username = "empty";
			password = "emptypwd";
			
			//TODO ***** Remove empty stuff
			
		} else {
			final String[] aCredentials = (String[]) credentials;
			username = (String) aCredentials[0];
			password = (String) aCredentials[1];
		}

		Properties pr = new Properties();
		pr.put(USER_NAME, username);
		pr.put(PASSWORD, password);
		getAuthenticator(pr).authenticate(pr, null);
		return new Subject(true, Collections.singleton(new JMXPrincipal(username)), Collections.EMPTY_SET,
			    Collections.EMPTY_SET);
	}

	@SuppressWarnings("unchecked")
	public void authorize(ObjectName name, final String methodName, Object[] params) {
	  
    try {
      ObjectName accessControlMBean = new ObjectName(OBJECT_NAME_ACCESSCONTROL);
      if (name.equals(accessControlMBean)) {
        logger.info("Granting access to accessContorlMXBean.. name="+name);
        return;
      }
    } catch (MalformedObjectNameException e) {
      // TODO Auto-generated catch block
      // e.printStackTrace();
    }
	  
	  
	  //Only apply for gemfire domain
	  String domain = name.getDomain();
	  if(!"GemFire".equals(domain))
	    return;
	  
		// Retrieve Subject from current AccessControlContext
		AccessControlContext acc = AccessController.getContext();		
		Subject subject = Subject.getSubject(acc);
		// Allow operations performed locally on behalf of the connector server
		// itself
		if (subject == null) {
			return;
		}

		// Restrict access to "createMBean" and "unregisterMBean" to any user
		if (methodName.equals("createMBean")
				|| methodName.equals("unregisterMBean")) {
			throw new SecurityException("Access denied");
		}

		// Retrieve JMXPrincipal from Subject
		Set<JMXPrincipal> principals = subject
				.getPrincipals(JMXPrincipal.class);
		Set<Object> pubCredentials = subject.getPublicCredentials();
		
		/*System.out.println("JMXPrincipal " + principals);
		System.out.println("Principals " + subject.getPrincipals());
		System.out.println("PubCredentials " + subject.getPublicCredentials());*/
		//add condition -> check if accessor is configured
		if (principals == null || principals.isEmpty()
				/*|| pubCredentials.size() < 1 */) {
			throw new SecurityException("Access denied");
		}		
	
		Principal principal = principals.iterator().next();
		
		//Give read access globally : TODO : Need to change this to map to proper getter
		LogService.getLogger().info("Name=" + name + " methodName=" +  methodName + " principal="+ principal.getName());
		if("getAttribute".equals(methodName) || "getAttributes".equals(methodName))
			return;	
		
		//TODO : if method=getAttributes params is directly availalbe
		//TODO : if method is operation then params is array array[0] = actual params, array[1]= signature
		
		ResourceOperationContext resourceContext = buildContext(name,methodName, params);		
		boolean authorized = getAccessControl(principal).authorizeOperation(null, resourceContext);
		LogService.getLogger().info("Name=" + name + " methodName=" +  methodName 
		    + " result="+authorized + " principal="+ principal.getName());
		if(!authorized)
			throw new SecurityException("Access denied");
	}

	public MBeanServerForwarder getMBeanServerForwarder() {
		return mBeanServerForwarder;
	}

	private static Class accessControlKlass = null;
	
	public AccessControl getAccessControl(Principal principal) {
		if(accessControlKlass==null) {
			String authorizeKlass = System.getProperty(ResourceConstants.RESORUCE_AUTH_ACCESSOR);
			try {
				accessControlKlass = Class.forName(authorizeKlass);
			} catch (ClassNotFoundException e) {
			  logger.error(e);
				throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource",e);
			}
		}
		
		try {
			AccessControl accessControl = (AccessControl) accessControlKlass.newInstance();
			accessControl.init(principal, null, null); //TODO pass proper params
			LogService.getLogger().info("Returning resource accessControl");
			return accessControl;
		} catch (InstantiationException e) {
		  logger.error(e);
			throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource",e);
		} catch (IllegalAccessException e) {
		  logger.error(e);
			throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource",e);
		}
	}

	private static Class authenticatorClass = null;
	private Authenticator getAuthenticator(Properties pr) {
		if(authenticatorClass==null) {
			String authenticatorKlass = System.getProperty(ResourceConstants.RESORUCE_AUTHENTICATOR);
			try {
				authenticatorClass = Class.forName(authenticatorKlass);
			} catch (ClassNotFoundException e) {	
			  logger.error(e);
				throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource",e);
			}
		}
		
		try {
			Authenticator authenticator = (Authenticator) authenticatorClass.newInstance();
			authenticator.init(pr, null, null); //TODO pass proper params
			LogService.getLogger().info("Returning resource authenticator " + authenticator);
			return authenticator;
		} catch (InstantiationException e) {
		  logger.error(e);
			throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource",e);
		} catch (IllegalAccessException e) {
		  logger.error(e);
			throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource",e);
		}
	}

  private ResourceOperationContext buildContext(ObjectName name, String methodName, Object[] params) {
    if (params != null) {
      LogService.getLogger().info("Params length=" + params.length);
      for (int i = 0; i < params.length; i++) {
        LogService.getLogger().info("Params[" + i + "] is " + arrayString(params[i]));
      }
    }

    String service = name.getKeyProperty("service");
    // only member mbean does not have service KeyProperty
    if (service == null && "processCommand".equals(methodName)) {
      Object[] array = (Object[]) params[0];
      String command = (String) array[0];
      CLIOperationContext context = new CLIOperationContext(command);      
      LogService.getLogger().info("Returning CLIContext for " + methodName);
      return context;
    } else {
      ResourceOperationContext context = new JMXOperationContext(name, methodName);
      LogService.getLogger().info("Returning JMXOperationContext for " + methodName);
      return context;
    }
  }

  private String arrayString(Object object) {
    StringBuilder sb = new StringBuilder();
    if (object instanceof Object[]) {
      Object[] array = (Object[]) object;
      for (Object a : array)
        sb.append(a).append(" ");
    }
    return sb.toString();
  }

}
