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
package com.gemstone.gemfire.management.internal.cli;

import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.RegionMXBean;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * 
 * @author tushark
 *
 */
public class CliUtilDUnitTest extends CacheTestCase {

  public CliUtilDUnitTest(String name) {
    super(name);    
  }

  public static final String COMMON_REGION="region1";
  public static final String COMMON_REGION_GROUP1 = "region_group1";
  public static final String COMMON_REGION_GROUP2 = "region_group2";
  public static final String REGION_MEMBER1_GROUP1 = "region_member1_group1";
  public static final String REGION_MEMBER2_GROUP1 = "region_member2_group1";
  public static final String REGION_MEMBER1_GROUP2 = "region_member1_group2";
  public static final String REGION_MEMBER2_GROUP2 = "region_member2_group2";
  
  public static final String MEMBER_1_GROUP1 = "member1_group1";
  public static final String MEMBER_2_GROUP1 = "member2_group1";
  public static final String MEMBER_1_GROUP2 = "member1_group2";
  public static final String MEMBER_2_GROUP2 = "member2_group2";
  
  public static final String GROUP1 = "group1";
  public static final String GROUP2 = "group2";
  
  
  private static final long serialVersionUID = 1L;
  
  public void tearDown2() throws Exception {
    destroySetup();
    super.tearDown2();
  }
  
  
  protected final void destroySetup() {        
    disconnectAllFromDS();    
  }
  
  @SuppressWarnings("serial")
  void setupMembersWithIdsAndGroups(){ 
    
    final VM vm1 = Host.getHost(0).getVM(0);
    final VM vm2 = Host.getHost(0).getVM(1);
    final VM vm3 = Host.getHost(0).getVM(2);
    final VM vm4 = Host.getHost(0).getVM(3);
    
    vm1.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        createCacheWithMemberIdAndGroup(MEMBER_1_GROUP1, GROUP1);
        createRegion(REGION_MEMBER1_GROUP1);
        createRegion(COMMON_REGION_GROUP1);
        createRegion(COMMON_REGION);
        //registerFunction();       
      }
      
    });
    
    vm2.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        createCacheWithMemberIdAndGroup(MEMBER_2_GROUP1, GROUP1);
        createRegion(REGION_MEMBER2_GROUP1);
        createRegion(COMMON_REGION_GROUP1);
        createRegion(COMMON_REGION);
        //registerFunction();
      }
    });
    
    vm3.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        createCacheWithMemberIdAndGroup(MEMBER_1_GROUP2, GROUP2);
        createRegion(REGION_MEMBER1_GROUP2);
        createRegion(COMMON_REGION_GROUP2);
        createRegion(COMMON_REGION);
        //registerFunction();
      }
    });
    
    vm4.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        createCacheWithMemberIdAndGroup(MEMBER_2_GROUP2, GROUP2);
        createRegion(REGION_MEMBER2_GROUP2);
        createRegion(COMMON_REGION_GROUP2);
        createRegion(COMMON_REGION);
        //registerFunction();
      }
    });
    
    vm1.invoke(new SerializableRunnable() {      
      @Override
      public void run() {       
        startManager();
      }      
    });
    
  }
  
  private void startManager() {
    final ManagementService service = ManagementService.getManagementService(getCache());
    service.startManager();
    assertEquals(true,service.isManager());
    assertNotNull(service.getManagerMXBean());
    assertTrue(service.getManagerMXBean().isRunning());
    final WaitCriterion waitForMaangerMBean = new WaitCriterion() {
      @Override
      public boolean done() {        
        boolean flag = checkBean(COMMON_REGION,4) &&
            checkBean(COMMON_REGION_GROUP1,2) &&
            checkBean(COMMON_REGION_GROUP2,2) &&
            checkBean(REGION_MEMBER1_GROUP1,1) &&
            checkBean(REGION_MEMBER2_GROUP1,1) &&
            checkBean(REGION_MEMBER1_GROUP2,1) &&
            checkBean(REGION_MEMBER2_GROUP2,1) ;                    
        if(!flag){
          getLogWriter().info("Still probing for mbeans");
          return false; 
        }
        else{
          getLogWriter().info("All distributed region mbeans are federated to manager.");
          return true;
        }
      }
      private boolean checkBean(String string, int memberCount) {
        DistributedRegionMXBean bean2 = service.getDistributedRegionMXBean(Region.SEPARATOR+string);        
        getLogWriter().info("DistributedRegionMXBean for region=" + string + " is " + bean2);
        if(bean2==null)
          return false;
        else{
          int members = bean2.getMemberCount();
          getLogWriter().info("DistributedRegionMXBean for region=" + string + " is aggregated for " + memberCount + " expected count=" + memberCount);
          if(members<memberCount){            
            return false;
          }
          else{
            return true;
          }
        }
      }
      @Override
      public String description() {            
        return "Probing for ManagerMBean";
      }
    };
    
    DistributedTestCase.waitForCriterion(waitForMaangerMBean, 120000, 2000, true);  
    getLogWriter().info("Manager federation is complete");
  }
  
  private void registerFunction() {    
    Function funct  = new DunitFunction("DunitFunction");
    FunctionService.registerFunction(funct);
  }   
  
  @SuppressWarnings("rawtypes")
  private Region createRegion(String regionName) {
    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.REPLICATE);
    Region region = regionFactory.create(regionName);
    final ManagementService service = ManagementService.getManagementService(getCache());    
    assertNotNull(service.getMemberMXBean());
    RegionMXBean bean = service.getLocalRegionMBean(Region.SEPARATOR+regionName); 
    assertNotNull(bean);
    getLogWriter().info("Created region=" + regionName + " Bean=" + bean);
    return region;
  }
  
  public void createCacheWithMemberIdAndGroup(String memberName, String groupName){
    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, memberName);
    localProps.setProperty(DistributionConfig.GROUPS_NAME, groupName);
    localProps.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");
    localProps.setProperty(DistributionConfig.JMX_MANAGER_START_NAME, "false");    
    int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
    localProps.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME, ""+jmxPort);
    getLogWriter().info("Set jmx-port="+ jmxPort);
    getSystem(localProps);
    getCache();
    final ManagementService service = ManagementService.getManagementService(getCache());
    assertNotNull(service.getMemberMXBean());
  }
  
  public void testFileToBytes(){
    
    //CliUtil.filesToBytes(fileNames)
    
  }
  
  @SuppressWarnings("serial")
  public void testCliUtilMethods() {
    setupMembersWithIdsAndGroups();
    
    final VM vm1 = Host.getHost(0).getVM(0);
    
    getLogWriter().info("testFor - findAllMatchingMembers");
    vm1.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        verifyFindAllMatchingMembers();
      }     
    });    
       
    final String id = (String)vm1.invoke(new SerializableCallable(){      
      @Override
      public Object call() throws Exception {
        Cache cache = getCache();
        return cache.getDistributedSystem().getDistributedMember().getId();
      }
    });
    
    getLogWriter().info("testFor - getDistributedMemberByNameOrId");
    vm1.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        getDistributedMemberByNameOrId(MEMBER_1_GROUP1,id);
      }     
    });
    
    getLogWriter().info("testFor - executeFunction");
    vm1.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        verifyExecuteFunction();
      }     
    });
    
    getLogWriter().info("testFor - getRegionAssociatedMembers");
    vm1.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        getRegionAssociatedMembers();
      }     
    });    
    
  }
  
  
  public void verifyFindAllMatchingMembers(){
    try {
      Set<DistributedMember> set = CliUtil.findAllMatchingMembers(GROUP1,null);
      assertNotNull(set);
      assertEquals(2, set.size());
      assertEquals(true,containsMember(set,MEMBER_1_GROUP1));
      assertEquals(true,containsMember(set,MEMBER_2_GROUP1));
      
      
      set = CliUtil.findAllMatchingMembers("group1,group2",null);
      assertNotNull(set);
      assertEquals(4, set.size());
      assertEquals(true,containsMember(set,MEMBER_1_GROUP1));
      assertEquals(true,containsMember(set,MEMBER_2_GROUP1));
      assertEquals(true,containsMember(set,MEMBER_1_GROUP2));
      assertEquals(true,containsMember(set,MEMBER_2_GROUP2));
      
      
      set = CliUtil.findAllMatchingMembers(null,MEMBER_1_GROUP1);
      assertNotNull(set);
      assertEquals(1, set.size());
      assertEquals(true,containsMember(set,MEMBER_1_GROUP1));     
      
      
      set = CliUtil.findAllMatchingMembers(null,"member1_group1,member2_group2");
      assertNotNull(set);
      assertEquals(2, set.size());
      assertEquals(true,containsMember(set,MEMBER_1_GROUP1));
      assertEquals(true,containsMember(set,MEMBER_2_GROUP2));
      
    } catch (CommandResultException e) {     
      fail("CliUtil failed with exception",e);
    }
  }
  
  private Object containsMember(Set<DistributedMember> set, String string) {
    boolean returnValue =false;
    for(DistributedMember member : set)
      if(member.getName().equals(string))
        return true;
    return returnValue;
  }

  
  public void getDistributedMemberByNameOrId(String name,String id) { 
    
    DistributedMember member = CliUtil.getDistributedMemberByNameOrId(name);
    assertNotNull(member);
    
    member = CliUtil.getDistributedMemberByNameOrId(id);
    assertNotNull(member);    
        
  }

  
  public void verifyExecuteFunction(){
    DunitFunction function = new DunitFunction("myfunction");
    Set<DistributedMember> set;
    try {
      @SuppressWarnings("rawtypes")
      Region region1 = CacheFactory.getAnyInstance().getRegion(COMMON_REGION);
      region1.clear();
      set = CliUtil.findAllMatchingMembers(GROUP1,null);
      assertEquals(2, set.size());
      ResultCollector collector = CliUtil.executeFunction(function, "executeOnGroup", set);
      collector.getResult();
      assertEquals(2, region1.size());
      assertTrue(region1.containsKey(MEMBER_1_GROUP1));
      assertTrue(region1.containsKey(MEMBER_2_GROUP1));
      assertEquals("executeOnGroup", region1.get(MEMBER_1_GROUP1));
      assertEquals("executeOnGroup", region1.get(MEMBER_2_GROUP1));
    } catch (CommandResultException e) {
      fail("Error during querying members",e);
    }        
  }
  
  
  public void getRegionAssociatedMembers(){
    
    String region_group1 ="/region_group1";
    String region1 ="/region1";
    String region_member2_group1 ="/region_member2_group1";
    
    /* 
    String region_member1_group1 ="/region_member1_group1";    
    String region_member1_group2 ="/region_member1_group2";    
    String region_member2_group2 ="/region_member2_group2";
    String region_group2 ="/region_group2";
    */
    
    Cache cache = getCache();
    
    Set<DistributedMember> set = CliUtil.getRegionAssociatedMembers(region1, cache, true);
    assertNotNull(set);
    assertEquals(4, set.size());
    assertEquals(true,containsMember(set,MEMBER_1_GROUP1));
    assertEquals(true,containsMember(set,MEMBER_2_GROUP1));
    assertEquals(true,containsMember(set,MEMBER_1_GROUP2));
    assertEquals(true,containsMember(set,MEMBER_2_GROUP2));
    
    /* "FIXME - Abhishek" This is failing because last param is not considered in method
    set = CliUtil.getRegionAssociatedMembers(region1, cache, false);
    assertNotNull(set);
    assertEquals(1, set.size());*/
    
    set = CliUtil.getRegionAssociatedMembers(region_group1, cache, true);
    assertNotNull(set);
    assertEquals(2, set.size());
    assertEquals(true,containsMember(set,MEMBER_1_GROUP1));
    assertEquals(true,containsMember(set,MEMBER_2_GROUP1));
    
    set = CliUtil.getRegionAssociatedMembers(region_member2_group1, cache, true);
    assertNotNull(set);
    assertEquals(1, set.size());
    assertEquals(true,containsMember(set,MEMBER_2_GROUP1));    
    
  }
    
  public static class DunitFunction extends FunctionAdapter{

    private static final long serialVersionUID = 1L;
    private String id;
    
    public DunitFunction(String fid){
      this.id= fid;
    }
    
    @Override
    public void execute(FunctionContext context) {
      Object object = context.getArguments();
      Cache cache = CacheFactory.getAnyInstance();
      @SuppressWarnings("rawtypes")
      Region region = cache.getRegion(COMMON_REGION);
      String id = cache.getDistributedSystem().getDistributedMember().getName();
      region.put(id, object);
      getLogWriter().info("Completed executeFunction on member : " + id);
      context.getResultSender().lastResult(true);
    }

    @Override
    public String getId() {      
      return id;
    }    
  }

}

