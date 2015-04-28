package com.gemstone.gemfire.management.internal.security;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.cli.util.ClasspathScanLoadHelper;


public class JMXOperationContext  extends ResourceOperationContext {
	
	private OperationCode code = OperationCode.RESOURCE;
	private ResourceOperationCode resourceCode = null;
	
	private static Map<String,ResourceOperationCode> cacheDSResourceOps = null;
	private static Map<String,ResourceOperationCode> cacheMemberResourceOps = null;
	private static Map<String,ResourceOperationCode> cacheRegionResourceOps = null;
	private static Map<String,ResourceOperationCode> cacheDiskStoreResourceOps = null;
	
	static {
		//cache all resource annotations
		readJMXAnnotations();		
		
	}	

	private static void readJMXAnnotations() {
		try {
			Class[] klassList = ClasspathScanLoadHelper.getClasses("com.gemstone.gemfire.management");
			for(Class klass : klassList) {
				if(klass.getName().endsWith("MXBean")) {
					Method[] methods = klass.getMethods();
					for(Method method : methods) {
						String name = method.getName();
						//ResourceOperation op = method.getDeclaredAnnotations();(ResourceOperation.class);
						boolean found=false;
						Annotation ans[] = method.getDeclaredAnnotations();
						for(Annotation an : ans){
							if(an instanceof ResourceOperation) {
								cache(klass,name,(ResourceOperation)an);
								found=true;
							}
						}
						if(!found)
							cache(klass,name,null);
					}
					//TODO : Log all cached operations
				}
			}
		} catch (ClassNotFoundException e) {			
			throw new GemFireConfigException(
					"Error while configuring authorization for jmx - ", e);
		} catch (IOException e) {
			throw new GemFireConfigException(
					"Error while configuring authorization for jmx - ", e);
		}
		
	}
	
	private static void cache(Class klass, String name, ResourceOperation op) {
		ResourceOperationCode code = null;
		
		if (op != null) {
			String opString = op.operation();
			if (opString != null)
				code = ResourceOperationCode.parse(opString);
		}
		
		if(code==null){
			if(name.startsWith("list") || name.startsWith("fetch") || name.startsWith("view")
					|| name.startsWith("show")){
				code = ResourceOperationCode.LIST_DS;
			} else if (name.startsWith("get")){
				code = ResourceOperationCode.READ_DS;
			} else if (name.startsWith("is")){
				code = ResourceOperationCode.READ_DS;
			} else if (name.startsWith("set")){
				code = ResourceOperationCode.SET_DS;
			}
		}
		
		/*
		System.out.println("Klass " + klass + " mname : " + name);
		if (code != null)
			System.out.println("ResourceOperation code=" + code);
		else
			System.out.println("ResourceOperation is null");*/
		
		Resource targetedResource = null;
		
		if(op!=null){
			targetedResource = op.resource();
		} else {
			if(klass.equals(DistributedSystemMXBean.class)) {
				targetedResource = Resource.DISTRIBUTED_SYSTEM;
			}
			//TODO : Add other resource and mbeans
		}
		
		/* Comment for timebeing to avoid falling for other methods
		if(!isGetterSetter(name) && code==null){
			throw new GemFireConfigException(
					"Error while configuring authorization for jmx. No authorization defined for " 
					+ klass.getCanonicalName() + " method " + name);
		}*/
		if(targetedResource!=null) {
			switch (targetedResource) {
			case DISTRIBUTED_SYSTEM:
				if (code != null){
					if(cacheDSResourceOps==null)
						cacheDSResourceOps = new HashMap<String,ResourceOperationCode>();
					cacheDSResourceOps.put(name, code);
				}
				break;
			}
		}			
	}

	private static boolean isGetterSetter(String name) {
		if(name.startsWith("is") || name.startsWith("get") ||  name.startsWith("set") ||  name.startsWith("fetch")
			||  name.startsWith("list") ||  name.startsWith("view") ||  name.startsWith("show") ) 
		return true;
		else return false;
	}

	public JMXOperationContext(ObjectName name , String methodName){
		code = OperationCode.RESOURCE;
		if(name.equals(MBeanJMXAdapter.getDistributedSystemName())){
			resourceCode = cacheDSResourceOps.get(methodName);
		}
	}
	

	@Override
	public OperationCode getOperationCode() {		
		return code;
	}

	@Override
	public ResourceOperationCode getResourceOperationCode() {
		return resourceCode;
	}

	public static Map<String, ResourceOperationCode> getCacheDSResourceOps() {
		return cacheDSResourceOps;
	}

	public static void setCacheDSResourceOps(
			Map<String, ResourceOperationCode> cacheDSResourceOps) {
		JMXOperationContext.cacheDSResourceOps = cacheDSResourceOps;
	}
	
	

}
