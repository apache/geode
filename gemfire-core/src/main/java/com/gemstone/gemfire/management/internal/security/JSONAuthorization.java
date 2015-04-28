package com.gemstone.gemfire.management.internal.security;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.management.remote.JMXPrincipal;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.security.ResourceOperationContext.ResourceOperationCode;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;
import com.gemstone.gemfire.security.NotAuthorizedException;

public class JSONAuthorization implements AccessControl, Authenticator {
	
	public static class Role{
		String[] permissions;
		String name;
		String regionName;
		String serverGroup;		
	}
	
	public static class User{
		String name;
		Role[] roles;
		String pwd;
	}
	
	private static Map<String,User> acl = null;
	
	public static JSONAuthorization create() throws IOException, JSONException {
	  if(acl==null){
	    readSecurityDescriptor(readDefault());
	  }
	  return new JSONAuthorization();
	}
	
  public JSONAuthorization() {
    if (acl == null) {
      try {
        readSecurityDescriptor(readDefault());
      } catch (IOException e) {
        throw new GemFireConfigException("Error creating JSONAuth", e);
      } catch (JSONException e) {
        throw new GemFireConfigException("Error creating JSONAuth", e);
      }
    }
  }
	
	public static Set<ResourceOperationCode> getAuthorizedOps(User user, ResourceOperationContext context) {
    Set<ResourceOperationCode> codeList = new HashSet<ResourceOperationCode>();
    for(Role role : user.roles) {
      for (String perm : role.permissions) {
        ResourceOperationCode code = ResourceOperationCode.parse(perm);
        if (role.regionName == null && role.serverGroup == null) {
          addPermissions(code, codeList);
        } else if (role.regionName != null) {
          LogService.getLogger().info("This role requires region=" + role.regionName);
          if (context instanceof CLIOperationContext) {
            CLIOperationContext cliContext = (CLIOperationContext) context;
            String region = cliContext.getCommandOptions().get("region");
            if (region != null && region.equals(role.regionName)) {
              addPermissions(code, codeList);
            } else {
              LogService.getLogger().info("Not adding permission " + code + " since region=" + region + " does not match");
            }
          }
        }
        // Same to be implemented for ServerGroup
      }
    }
    LogService.getLogger().info("Final set of permisions " + codeList);
    return codeList;
  }
	
	private static void addPermissions(ResourceOperationCode code, Set<ResourceOperationCode> codeList) {
	  if(code!=null) {
      if(code.getChildren()==null)
        codeList.add(code);
      else {
        for(ResourceOperationCode c : code.getChildren()){
          codeList.add(c);
        }
      }
    }    
  }

  private static String readDefault() throws IOException, JSONException {
	  String str = System.getProperty(ResourceConstants.RESORUCE_SEC_DESCRIPTOR, ResourceConstants.RESORUCE_DEFAULT_SEC_DESCRIPTOR);
		File file = new File(str);
		FileReader reader = new FileReader(file);
		char[] buffer = new char[(int) file.length()];
		reader.read(buffer);
		String json = new String(buffer);
		reader.close();
		return json;
	}

	public JSONAuthorization(String json) throws IOException, JSONException{
		readSecurityDescriptor(json);
	}
	

	private static void readSecurityDescriptor(String json) throws IOException, JSONException {		
		JSONObject jsonBean = new JSONObject(json);		
		acl = new HashMap<String,User>();		
		Map<String,Role> roleMap = readRoles(jsonBean);
		readUsers(acl,jsonBean,roleMap);		
	}

	private static void readUsers(Map<String, User> acl, JSONObject jsonBean,
			Map<String, Role> roleMap) throws JSONException {
		JSONArray array = jsonBean.getJSONArray("users");
		for(int i=0;i<array.length();i++){
			JSONObject obj = array.getJSONObject(i);
			User user = new User();
			user.name = obj.getString("name");
			if(obj.has("password"))
			  user.pwd = obj.getString("password");
			else 
			  user.pwd = user.name;
			
			JSONArray ops = obj.getJSONArray("roles");
			user.roles = new Role[ops.length()];
			for(int j=0;j<ops.length();j++){
				String roleName = ops.getString(j);
				user.roles[j] = roleMap.get(roleName);
				if(user.roles[j]==null){
					throw new RuntimeException("Role not present " + roleName);
				}
			}
			acl.put(user.name, user);
		}		
	}

	private static Map<String, Role> readRoles(JSONObject jsonBean) throws JSONException {
		Map<String,Role> roleMap = new HashMap<String,Role>();
		JSONArray array = jsonBean.getJSONArray("roles");
		for(int i=0;i<array.length();i++){
			JSONObject obj = array.getJSONObject(i);
			Role role = new Role();
			role.name = obj.getString("name");
			
			if(obj.has("operationsAllowed")){
				JSONArray ops = obj.getJSONArray("operationsAllowed");
				role.permissions = new String[ops.length()];
				for(int j=0;j<ops.length();j++){
					role.permissions[j] = ops.getString(j);
				}
			}else {
				if (!obj.has("inherit"))
					throw new RuntimeException(
							"Role "
									+ role.name
									+ " does not have any permission neither it inherits any parent role");
			}
			
			roleMap.put(role.name,role);
			
			if(obj.has("region")){
				role.regionName = obj.getString("region");
			}
			
			if(obj.has("serverGroup")){
				role.serverGroup = obj.getString("serverGroup");
			}
		}
		
		for(int i=0;i<array.length();i++){
			JSONObject obj = array.getJSONObject(i);
			String name = obj.getString("name");
			Role role = roleMap.get(name);
			if (role == null) {
				throw new RuntimeException("Role not present "
						+ role);
			}
			if(obj.has("inherit")){				
				JSONArray parentRoles = obj.getJSONArray("inherit");
				for (int m = 0; m < parentRoles.length(); m++) {
					String parentRoleName = parentRoles.getString(m);
					Role parentRole = roleMap.get(parentRoleName);
					if (parentRole == null) {
						throw new RuntimeException("Role not present "
								+ parentRoleName);
					}
					int oldLenth=0;
					if(role.permissions!=null)
						oldLenth = role.permissions.length;
					int newLength = oldLenth + parentRole.permissions.length;
					String[] str = new String[newLength];
					int k = 0;
					if(role.permissions!=null) {
						for (; k < role.permissions.length; k++) {
							str[k] = role.permissions[k];
						}
					}

					for (int l = 0; l < parentRole.permissions.length; l++) {
						str[k + l] = parentRole.permissions[l];
					}
					role.permissions = str;
				}
			}
			
		}		
		return roleMap;
	}

	public static Map<String, User> getAcl() {
		return acl;
	}
	
	private Principal principal=null;

  @Override
  public void close() {
    
  }

  @Override
  public boolean authorizeOperation(String arg0, OperationContext context) {
    
    if(principal!=null) {
      User user = acl.get(principal.getName());
      if(user!=null) {
        LogService.getLogger().info("Context received " + context);
        ResourceOperationContext ctx = (ResourceOperationContext)context;
        LogService.getLogger().info("Checking for code " + ctx.getResourceOperationCode());
        
        //TODO : This is for un-annotated commands
        if(ctx.getResourceOperationCode()==null)
          return true;        
        
        boolean found = false;
        for(ResourceOperationCode code : getAuthorizedOps(user, (ResourceOperationContext) context)) {
          if(ctx.getResourceOperationCode().equals(code)){
            found =true;
            LogService.getLogger().info("found code " + code.toString());
            break;
          }             
        }
        if(found)
          return true;
        LogService.getLogger().info("Did not find code " + ctx.getResourceOperationCode());
        return false;        
      }
    } 
    return false;
  }

  @Override
  public void init(Principal principal, DistributedMember arg1, Cache arg2) throws NotAuthorizedException {
    this.principal = principal;    
  }

  @Override
  public Principal authenticate(Properties props, DistributedMember arg1) throws AuthenticationFailedException {
    String user = props.getProperty(ManagementInterceptor.USER_NAME);
    String pwd = props.getProperty(ManagementInterceptor.PASSWORD);
    User userObj = acl.get(user);
    if(userObj==null)
      throw new AuthenticationFailedException("Wrong username/password");
    LogService.getLogger().info("User="+user + " pwd="+pwd);
    if (user!=null && !userObj.pwd.equals(pwd) && !"".equals(user))
      throw new AuthenticationFailedException("Wrong username/password");
    LogService.getLogger().info("Authentication successful!! for " + user);
    return new JMXPrincipal(user);    
  }

  @Override
  public void init(Properties arg0, LogWriter arg1, LogWriter arg2) throws AuthenticationFailedException {   
    
  }	

}
