package com.gemstone.gemfire.tools.databrowser.dunit;

import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Iterator;
import java.util.Arrays;
import junit.framework.Assert;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryExecutionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;
import com.gemstone.gemfire.tools.databrowser.data.Customer;

import dunit.SerializableRunnable;

public class ObjectTypeQueryResultDUnitTest extends DataBrowserDUnitTestCase {

  public ObjectTypeQueryResultDUnitTest(String name) {
    super(name); 
  } 
  
  @Override
  public void populateData() {
    List result = new ArrayList();
    result.add(new Customer(1, "Hrishi", "Pune"));
    populateRegion(DEFAULT_REGION_PATH, result.toArray());    
  }  
    
  public void testPrimitiveObjectQueryResult() throws Exception {
    
    browser.invoke(new SerializableRunnable() {
      public void run() {
//        GemFireMember[] members = connection.getMembers();
//        
//        Assert.assertEquals(1, members.length);
//        Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
        
        String queryString = "SELECT distinct name FROM "+DEFAULT_REGION_PATH+" where name = 'Hrishi'";
        QueryResult result = null;
        try {
          result = connection.executeQuery(queryString, null);
          Assert.assertNotNull(result); 
        }
        catch (QueryExecutionException e) {
          fail("Failed to execute query through data browser",e);
        }
        
        IntrospectionResult [] result_t = result.getIntrospectionResult();
        Assert.assertEquals(1, result_t.length);
        IntrospectionResult metaInfo = result_t[0];
                     
        verifyPrimitiveType(metaInfo, String.class);
         
        Collection<?> q_result = result.getQueryResult();
        Iterator iter = q_result.iterator();
        Object temp = iter.next();
        
        try {
          Assert.assertEquals("Hrishi", result.getColumnValue(temp, 0));
        }
        catch (Exception e) {
          fail("Failed to read the value for column 0.",e);
        }            
      }
    });       
  }
  
  public void testCompositeObjectQueryResult() throws Exception {
    
    browser.invoke(new SerializableRunnable() {
      public void run() {
//        GemFireMember[] members = connection.getMembers();
//        
//        Assert.assertEquals(1, members.length);
//        Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
        
        String queryString = "SELECT * FROM "+DEFAULT_REGION_PATH+" where name = 'Hrishi'";
        QueryResult result = null;
        try {
          result = connection.executeQuery(queryString, null);
          Assert.assertNotNull(result); 
        }
        catch (QueryExecutionException e) {
          fail("Failed to execute query through data browser",e);
        }
        
        IntrospectionResult [] result_t = result.getIntrospectionResult();
        Assert.assertEquals(1, result_t.length);
        IntrospectionResult metaInfo = result_t[0];
                     
        Assert.assertEquals(3, metaInfo.getColumnCount());
            
        List fields = Arrays.asList(new String[]{"name", "id", "address"});
        List classtypes = Arrays.asList(new String[]{"java.lang.String","java.lang.Integer", "java.lang.String"});
        List<Integer> columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                                 IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                                 IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
                   
        verifyType(metaInfo, Customer.class, fields, classtypes, columntypes);
      }
    });       
  }
  
  public void testStructQueryResult() {
    
    browser.invoke(new SerializableRunnable() {
      public void run() {
        
//    GemFireMember[] members = connection.getMembers();
//    Assert.assertEquals(1, members.length);
//    Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
    
    String queryString = "SELECT id , name FROM "+DEFAULT_REGION_PATH;
    
    QueryResult result = null;
    try {
      result = connection.executeQuery(queryString, null);
      Assert.assertNotNull(result); 
    }
    catch (QueryExecutionException e) {
      fail("Failed to execute query through data browser",e);
    }

    IntrospectionResult [] temp = result.getIntrospectionResult();
    Assert.assertEquals(1, temp.length);
    IntrospectionResult metaInfo = temp[0];
    
    Assert.assertEquals(2, metaInfo.getColumnCount());
       
    List fields = Arrays.asList(new String[]{"name", "id"});
    List classtypes = Arrays.asList(new String[]{"java.lang.Object","java.lang.Object"});
    List<Integer> columntypes = Arrays.asList(new Integer[] {IntrospectionResult.UNKNOWN_TYPE_COLUMN,
                                                             IntrospectionResult.UNKNOWN_TYPE_COLUMN });
 
    verifyType(metaInfo, StructType.class, fields, classtypes, columntypes);
    
    Collection temp1 = result.getQueryResult();        
    Assert.assertEquals(1, temp1.size());        
    
    Iterator iter = temp1.iterator();
    Object tuple = iter.next();
       
    try {
      int index = metaInfo.getColumnIndex("id");
      Class colClass = metaInfo.getColumnClass(tuple, index);
      Assert.assertTrue(colClass.getCanonicalName().equals("java.lang.Integer"));
      Assert.assertTrue(metaInfo.getColumnType(tuple,index) == IntrospectionResult.PRIMITIVE_TYPE_COLUMN);
      
      index = metaInfo.getColumnIndex("name");
      colClass = metaInfo.getColumnClass(tuple, index);
      Assert.assertTrue(colClass.getCanonicalName().equals("java.lang.String"));
      Assert.assertTrue(metaInfo.getColumnType(tuple,index) == IntrospectionResult.PRIMITIVE_TYPE_COLUMN);
    }
    catch (Exception e) {
      fail("Failed to execute test ",e);
    }   
    
  }
   });
  }
  
  public void testIntegerType() {
    browser.invoke(new SerializableRunnable() {
      public void run() {
        
//    GemFireMember[] members = connection.getMembers();
//    Assert.assertEquals(1, members.length);
//    Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
    
    String queryString = DEFAULT_REGION_PATH+".size";
    
    QueryResult result = null;
    try {
      result = connection.executeQuery(queryString, null);
      Assert.assertNotNull(result); 
    }
    catch (QueryExecutionException e) {
      fail("Failed to execute query through data browser",e);
    }

    IntrospectionResult [] temp = result.getIntrospectionResult();
    Assert.assertEquals(1, temp.length);
    IntrospectionResult metaInfo = temp[0];
    
    verifyPrimitiveType(metaInfo, Integer.class);
  }
   });
 }
 
}
