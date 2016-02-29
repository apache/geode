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
package com.gemstone.gemfire.management.internal.security;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.AsyncEventQueueMXBean;
import com.gemstone.gemfire.management.CacheServerMXBean;
import com.gemstone.gemfire.management.DiskStoreMXBean;
import com.gemstone.gemfire.management.DistributedLockServiceMXBean;
import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.GatewayReceiverMXBean;
import com.gemstone.gemfire.management.GatewaySenderMXBean;
import com.gemstone.gemfire.management.LocatorMXBean;
import com.gemstone.gemfire.management.LockServiceMXBean;
import com.gemstone.gemfire.management.ManagerMXBean;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.RegionMXBean;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.cli.util.ClasspathScanLoadHelper;
import static com.gemstone.gemfire.management.internal.security.ResourceConstants.*;

/**
 * It describes current JMX MBean Method call and its parameters.
 * OpCode returned by JMXOperationContext is retrieved from ResourceOperation annotation
 * on the target methodName
 *
 * @author tushark
 * @since 9.0
 *
 */
public class JMXOperationContext  extends ResourceOperationContext {
	
	private OperationCode code = OperationCode.RESOURCE;
	private ResourceOperationCode resourceCode = null;
  private ObjectName name;
  private String methodName;

  private static Map<Class<?>,Map<String,ResourceOperationCode>> cachedResourceOpsMapping = new HashMap<Class<?>,Map<String,ResourceOperationCode>>();
  private static Map<String,ResourceOperationCode> distributedSystemMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> diskStoreMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> cacheServerMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> gatewayReceiverMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> gatewaySenderMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> lockServiceMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> managerMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> memberMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> regionMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> locatorMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> distributedLockServiceMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> distributedRegionMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> asyncEventQueueMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();
  private static Map<String,ResourceOperationCode> accessControlMXBeanResourceOps = new HashMap<String,ResourceOperationCode>();

	
	static {
		readJMXAnnotations();		
		
	}	

	private static void readJMXAnnotations() {

    cachedResourceOpsMapping.put(DistributedSystemMXBean.class, distributedSystemMXBeanResourceOps);
    cachedResourceOpsMapping.put(DiskStoreMXBean.class, diskStoreMXBeanResourceOps);
    cachedResourceOpsMapping.put(CacheServerMXBean.class, cacheServerMXBeanResourceOps);
    cachedResourceOpsMapping.put(GatewayReceiverMXBean.class, gatewayReceiverMXBeanResourceOps);
    cachedResourceOpsMapping.put(GatewaySenderMXBean.class, gatewaySenderMXBeanResourceOps);
    cachedResourceOpsMapping.put(LockServiceMXBean.class, lockServiceMXBeanResourceOps);
    cachedResourceOpsMapping.put(ManagerMXBean.class, managerMXBeanResourceOps);
    cachedResourceOpsMapping.put(MemberMXBean.class, memberMXBeanResourceOps);
    cachedResourceOpsMapping.put(RegionMXBean.class, regionMXBeanResourceOps);
    cachedResourceOpsMapping.put(LocatorMXBean.class, locatorMXBeanResourceOps);
    cachedResourceOpsMapping.put(DistributedLockServiceMXBean.class, distributedLockServiceMXBeanResourceOps);
    cachedResourceOpsMapping.put(DistributedRegionMXBean.class, distributedRegionMXBeanResourceOps);
    cachedResourceOpsMapping.put(AsyncEventQueueMXBean.class, asyncEventQueueMXBeanResourceOps);
    cachedResourceOpsMapping.put(AccessControlMXBean.class, accessControlMXBeanResourceOps);

		try {
      Class<?>[] klassList = ClasspathScanLoadHelper.getClasses(MANAGEMENT_PACKAGE);
      for(Class<?> klass : klassList) {
				if(klass.getName().endsWith("MXBean")) {
					Method[] methods = klass.getMethods();
					for(Method method : methods) {
						String name = method.getName();
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
	
  private static void cache(Class<?> klass, String name, ResourceOperation op) {
		ResourceOperationCode code = null;
		
		if (op != null) {
			String opString = op.operation();
			if (opString != null)
				code = ResourceOperationCode.parse(opString);
		}
		
    if(code==null && isGetterSetter(name)){
				code = ResourceOperationCode.LIST_DS;
		}

		
    if (code == null && cachedResourceOpsMapping.keySet().contains(klass) && !isGetterSetter(name)) {
      throw new GemFireConfigException("Error while configuring authorization for jmx. No opCode defined for "
					+ klass.getCanonicalName() + " method " + name);
				}

    final Map<String,ResourceOperationCode> resourceOpsMap = cachedResourceOpsMapping.get(klass);
    if(resourceOpsMap==null) {
      if (cachedResourceOpsMapping.keySet().contains(klass))
        throw new GemFireConfigException("Unknown MBean " + klass.getCanonicalName());
      else {
        LogService.getLogger().warn("Unsecured mbean " + klass);
			}
		}			
    else {
      resourceOpsMap.put(name, code);
    }
	}

  public static boolean isGetterSetter(String name) {
    if(name.startsWith(GETTER_IS) || name.startsWith(GETTER_GET) ||  name.startsWith(GETTER_FETCH)
      ||  name.startsWith(GETTER_LIST) ||  name.startsWith(GETTER_VIEW) ||  name.startsWith(GETTER_SHOW) ||  name.startsWith(GETTER_HAS))
		return true;
		else return false;
	}

	public JMXOperationContext(ObjectName name , String methodName){
		code = OperationCode.RESOURCE;
    Class<?> klass = getMbeanClass(name);
    Map<String,ResourceOperationCode> resourceOpsMap = cachedResourceOpsMapping.get(klass);
    resourceCode = resourceOpsMap.get(methodName);
    this.methodName = methodName;
    this.name = name;

    //If getAttr is not found try for isAttr ie. boolean getter
    if(resourceCode==null) {
      if(this.methodName.startsWith(GET_PREFIX)) {
        String methodNameBooleanGetter = GET_IS_PREFIX + this.methodName.substring(GET_PREFIX.length());
        if(resourceOpsMap.containsKey(methodNameBooleanGetter)){
          resourceCode = resourceOpsMap.get(methodNameBooleanGetter);
          this.methodName = methodNameBooleanGetter;
        }
		}
	}
	
    //If resourceCode is still null most likely its wrong method name so just allow it pass
    if(resourceCode==null) {
      resourceCode = ResourceOperationCode.LIST_DS;
    }
  }




  private Class<?> getMbeanClass(ObjectName name) {
    if (name.equals(MBeanJMXAdapter.getDistributedSystemName()))
      return DistributedSystemMXBean.class;
    else {
      String service = name.getKeyProperty(MBEAN_KEY_SERVICE);
      String mbeanType = name.getKeyProperty(MBEAN_KEY_TYPE);

      if (MBEAN_TYPE_DISTRIBUTED.equals(mbeanType)) {
        if (MBEAN_SERVICE_SYSTEM.equals(service)) {
          return DistributedSystemMXBean.class;
        } else if (MBEAN_SERVICE_REGION.equals(service)) {
          return DistributedRegionMXBean.class;
        } else if (MBEAN_SERVICE_LOCKSERVICE.equals(service)) {
          return DistributedLockServiceMXBean.class;
        } else {
          throw new RuntimeException("Unknown mbean type " + name);
        }
      } else if (MBEAN_TYPE_MEMBER.equals(mbeanType)) {
        if (service == null) {
          return MemberMXBean.class;
        } else {
          if (MBEAN_SERVICE_MANAGER.equals(service)) {
            return ManagerMXBean.class;
          } else if (MBEAN_SERVICE_CACHESERVER.equals(service)) {
            return CacheServerMXBean.class;
          } else if (MBEAN_SERVICE_REGION.equals(service)) {
            return RegionMXBean.class;
          } else if (MBEAN_SERVICE_LOCKSERVICE.equals(service)) {
            return LockServiceMXBean.class;
          } else if (MBEAN_SERVICE_DISKSTORE.equals(service)) {
            return DiskStoreMXBean.class;
          } else if (MBEAN_SERVICE_GATEWAY_RECEIVER.equals(service)) {
            return GatewayReceiverMXBean.class;
          } else if (MBEAN_SERVICE_GATEWAY_SENDER.equals(service)) {
            return GatewaySenderMXBean.class;
          } else if (MBEAN_SERVICE_ASYNCEVENTQUEUE.equals(service)) {
            return AsyncEventQueueMXBean.class;
          } else if (MBEAN_SERVICE_LOCATOR.equals(service)) {
            return LocatorMXBean.class;
          } else {
            throw new RuntimeException("Unknown mbean type " + name);
          }
        }
      } else {
        throw new RuntimeException("Unknown mbean type " + name);
      }
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

  public String toString(){
    return "JMXOpCtx(on="+name+",method="+methodName+")";
	}

	}
	
