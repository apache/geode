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

package com.gemstone.gemfire.security;

import java.util.concurrent.Callable;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import com.gemstone.gemfire.management.internal.security.ResourceOperationContext;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.ShiroException;
import org.apache.shiro.UnavailableSecurityManagerException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;

public class ShiroUtil {

  public static void login(String username, String password){
    if(!isShiroConfigured())
      return;

    Subject currentUser = SecurityUtils.getSubject();

    UsernamePasswordToken token =
        new UsernamePasswordToken(username, password);
    try {
      LogService.getLogger().info("Logging in "+username+"/"+password);
      currentUser.login(token);
    } catch (ShiroException e) {
      throw new AuthenticationFailedException(e.getMessage(), e);
    }
  }

  public static void logout(){
    if(!isShiroConfigured())
      return;

    Subject currentUser = SecurityUtils.getSubject();
    try {
      LogService.getLogger().info("Logging out "+currentUser.getPrincipal());
      currentUser.logout();
    }
    catch(ShiroException e){
      throw new AuthenticationFailedException(e.getMessage(), e);
    }
    // clean out Shiro's thread local content
    ThreadContext.remove();
  }

  public static Callable associateWith(Callable callable){
    if(!isShiroConfigured())
      return callable;

    Subject currentUser = SecurityUtils.getSubject();
    return currentUser.associateWith(callable);
  }

  public static void authorize(ResourceOperationContext context) {
    authorize(context.getResource().name(), context.getOperationCode().name(), context.getRegionName());
  }

  public static void authorize(ResourceOperation resourceOperation) {
    authorize(resourceOperation.resource().name(), resourceOperation.operation().name());
  }

  public static void authorize(String resource, String operation){
    authorize(resource, operation, null);
  }

  public static void authorize(String resource, String operation, String regionName){
    if(!isShiroConfigured())
      return;

    ResourceOperationContext permission = new ResourceOperationContext(resource, operation, regionName);
    Subject currentUser = SecurityUtils.getSubject();
    try {
      currentUser.checkPermission(permission);
    }
    catch(ShiroException e){
      LogService.getLogger().info(currentUser.getPrincipal() + " not authorized for "+resource+":"+operation+":"+regionName);
      throw new GemFireSecurityException(e.getMessage(), e);
    }
  }

  private static boolean isShiroConfigured(){
    try{
      SecurityUtils.getSecurityManager();
    }
    catch(UnavailableSecurityManagerException e){
      return false;
    }
    return true;
  }


}
