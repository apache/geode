package com.gemstone.gemfire.management.internal.security;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import junit.framework.TestCase;

import org.json.JSONException;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.management.internal.security.JSONAuthorization.User;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.junit.UnitTest;

@Category(UnitTest.class)
public class JSONAuthorizationTest extends TestCase {
  
  public static String makePath(String[] strings) {
    StringBuilder sb = new StringBuilder();
    for(int i=0;i<strings.length;i++){
      sb.append(strings[i]);      
      sb.append(File.separator);
    }
    return sb.toString();
  }
	
	public void testSimpleUserAndRole() throws IOException, JSONException {
		String json = readFile(TestUtil.getResourcePath(getClass(), "testSimpleUserAndRole.json"));
		JSONAuthorization authorization = new JSONAuthorization(json);		
		Map<String, User> acl = authorization.getAcl();
		assertNotNull(acl);
		assertEquals(1, acl.size());		
		User user = acl.get("tushark");
		assertNotNull(user);
		assertNotNull(user.roles);
		assertEquals(1,user.roles.length);
		assertEquals("jmxReader", user.roles[0].name);
		assertEquals(1, user.roles[0].permissions.length);
		assertEquals("QUERY", user.roles[0].permissions[0]);
	}
	
	
	public void testUserAndRoleRegionServerGroup() throws IOException, JSONException {
		String json = readFile(TestUtil.getResourcePath(getClass(), "testUserAndRoleRegionServerGroup.json"));
		JSONAuthorization authorization = new JSONAuthorization(json);		
		Map<String, User> acl = authorization.getAcl();
		assertNotNull(acl);
		assertEquals(1, acl.size());		
		User user = acl.get("tushark");
		assertNotNull(user);
		assertNotNull(user.roles);
		assertEquals(1,user.roles.length);
		assertEquals("jmxReader", user.roles[0].name);
		assertEquals(1, user.roles[0].permissions.length);
		assertEquals("QUERY", user.roles[0].permissions[0]);
		
		assertEquals("secureRegion", user.roles[0].regionName);
		assertEquals("SG2", user.roles[0].serverGroup);
	}
	
	public void testUserMultipleRole()throws IOException, JSONException {
		String json = readFile(TestUtil.getResourcePath(getClass(), "testUserMultipleRole.json"));
		JSONAuthorization authorization = new JSONAuthorization(json);		
		Map<String, User> acl = authorization.getAcl();
		assertNotNull(acl);
		assertEquals(1, acl.size());		
		User user = acl.get("tushark");
		assertNotNull(user);
		assertNotNull(user.roles);
		assertEquals(2,user.roles.length);
		
		JSONAuthorization.Role role = user.roles[0];
		if(role.name.equals("jmxReader")){			
			assertEquals(1, role.permissions.length);
			assertEquals("QUERY", role.permissions[0]);
		} else {
			assertEquals(7, role.permissions.length);
			assertEquals("sysMonitors", role.name);
		}		
		
		role = user.roles[1];
		if(role.name.equals("jmxReader")){			
			assertEquals(1, role.permissions.length);
			assertEquals("QUERY", role.permissions[0]);
		} else {
			assertEquals(7, role.permissions.length);
			assertEquals("sysMonitors", role.name);
			assertTrue(contains(role.permissions, "CMD_EXORT_LOGS"));
			assertTrue(contains(role.permissions, "CMD_STACK_TRACES"));
			assertTrue(contains(role.permissions, "CMD_GC"));
			assertTrue(contains(role.permissions, "CMD_NETSTAT"));
			assertTrue(contains(role.permissions, "CMD_SHOW_DEADLOCKS")); 
			assertTrue(contains(role.permissions, "CMD_SHOW_LOG")); 
			assertTrue(contains(role.permissions, "SHOW_METRICS"));
		}		
	}
	
	private boolean contains(String[] permissions, String string) {
		for(String str : permissions)
			if(str.equals(string))
					return true;
		return false;
	}


	public void testInheritRole() throws IOException, JSONException {
		String json = readFile(TestUtil.getResourcePath(getClass(), "testInheritRole.json"));
		JSONAuthorization authorization = new JSONAuthorization(json);		
		Map<String, User> acl = authorization.getAcl();
		assertNotNull(acl);
		assertEquals(3, acl.size());		
		User user = acl.get("tushark");
		assertNotNull(user);
		assertNotNull(user.roles);
		assertEquals(1,user.roles.length);
		assertEquals("jmxReader", user.roles[0].name);
		assertEquals(1, user.roles[0].permissions.length);
		assertEquals("QUERY", user.roles[0].permissions[0]);
		
		User admin1 = acl.get("admin1");
		assertNotNull(admin1);
		assertNotNull(admin1.roles);
		assertEquals(1,admin1.roles.length);
		assertEquals("adminSG1", admin1.roles[0].name);
		assertEquals("SG1", admin1.roles[0].serverGroup);
		assertEquals(1, admin1.roles[0].permissions.length);
		assertEquals("CMD_SHUTDOWN", admin1.roles[0].permissions[0]);
		
		User admin2 = acl.get("admin2");
		assertNotNull(admin2);
		assertNotNull(admin2.roles);
		assertEquals(1,admin2.roles.length);
		assertEquals("adminSG2", admin2.roles[0].name);
		assertEquals("SG2", admin2.roles[0].serverGroup);
		assertEquals(2, admin2.roles[0].permissions.length);
		assertTrue(contains(admin2.roles[0].permissions, "CHANGE_LOG_LEVEL"));
		assertTrue(contains(admin2.roles[0].permissions, "CMD_SHUTDOWN"));
	}
	
	private String readFile(String name) throws IOException, JSONException {
		File file = new File(name);
		FileReader reader = new FileReader(file);
		char[] buffer = new char[(int) file.length()];
		reader.read(buffer);
		String json = new String(buffer);
		reader.close();
		return json;
	}

}
