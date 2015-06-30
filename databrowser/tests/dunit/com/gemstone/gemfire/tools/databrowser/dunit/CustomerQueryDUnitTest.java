package com.gemstone.gemfire.tools.databrowser.dunit;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.tools.databrowser.data.Customer;

import dunit.SerializableRunnable;

public class CustomerQueryDUnitTest extends DataBrowserDUnitTestCase {

  public CustomerQueryDUnitTest(String name) {
    super(name); 
  }
  
  @Override
  public void populateData() {
    List result = new ArrayList();
    result.add(new Customer(1, "Tom", "Pune"));
    result.add(new Customer(2, "Jack", "Bombay"));
    populateRegion(DEFAULT_REGION_PATH, result.toArray());    
  }  
  
  public void testCustomerObjectQuery() {
    browser.invoke(new SerializableRunnable() {
      public void run() {
//        GemFireMember[] members = connection.getMembers();
//        Assert.assertEquals(1, members.length);
//        Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
//        Assert.assertEquals(1, members[0].getCacheServers().length);
        
        String query1 = "SELECT * FROM "+DEFAULT_REGION_PATH;
        
        int gfe_count = executeDirectQuery(connection.getQueryService(null), query1);
        int browser_count = executeQueryThroughBrowser(connection, null, query1);
        
        Assert.assertEquals(2, gfe_count);
        Assert.assertEquals(2, browser_count);
        
        String query2 = "SELECT distinct name FROM "+DEFAULT_REGION_PATH;
        
        gfe_count = executeDirectQuery(connection.getQueryService(null), query2);
        browser_count = executeQueryThroughBrowser(connection, null, query2);
        
        Assert.assertEquals(2 , gfe_count);
        Assert.assertEquals(2 , browser_count);
        
        
        String query3 = "SELECT * FROM "+DEFAULT_REGION_PATH+" WHERE name='Tom'";
        
        gfe_count = executeDirectQuery(connection.getQueryService(null), query3);
        browser_count = executeQueryThroughBrowser(connection, null, query3);
        
        int expectedCount = 1; 
        Assert.assertEquals(expectedCount , gfe_count);
        Assert.assertEquals(expectedCount, browser_count);
        
        
        String query4 = "SELECT * FROM "+DEFAULT_REGION_PATH+" WHERE address='Bombay'";
        
        gfe_count = executeDirectQuery(connection.getQueryService(null), query4);
        browser_count = executeQueryThroughBrowser(connection, null, query4);
        
        expectedCount = 1; 
        Assert.assertEquals(expectedCount , gfe_count);
        Assert.assertEquals(expectedCount, browser_count);
      
      }
    });
  }
}
