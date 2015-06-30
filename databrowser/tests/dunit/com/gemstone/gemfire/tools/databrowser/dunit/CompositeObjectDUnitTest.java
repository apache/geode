package com.gemstone.gemfire.tools.databrowser.dunit;

import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Iterator;

import junit.framework.Assert;

import java.util.Arrays;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryExecutionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ResultSet;
import com.gemstone.gemfire.tools.databrowser.data.Address;
import com.gemstone.gemfire.tools.databrowser.data.GemStoneCustomer;
import com.gemstone.gemfire.tools.databrowser.data.Product;

import dunit.SerializableRunnable;

public class CompositeObjectDUnitTest extends DataBrowserDUnitTestCase {

  public CompositeObjectDUnitTest(String name) {
    super(name); 
  }
  
  @Override
  public void populateData() {
    List result = new ArrayList();
    
    ArrayList<Product> products = new ArrayList<Product>();
    products.add(new Product(1, "GemFire Enterprise", 40));
    
    Address address = new Address("Road1","Pune",41101);
    
    result.add(new GemStoneCustomer(1, "Tom", address ,products));
    result.add(new GemStoneCustomer(2, "Harry", null ,null));
    
    products = new ArrayList<Product>();
    products.add(new Product(2, "GemFire Enterprise Monitoring", 0));
    result.add(new GemStoneCustomer(3, "Dick", address ,products));
        
    populateRegion(DEFAULT_REGION_PATH, result.toArray()); 
  }  
  
  public void testCustomerObjectQuery() {
    browser.invoke(new SerializableRunnable() {
      public void run() {
//        GemFireMember[] members = connection.getMembers();
//        
//        Assert.assertEquals(1, members.length);
//        Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
        
        String queryString = "SELECT * FROM "+DEFAULT_REGION_PATH + " WHERE id = 1";
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
        
        Assert.assertEquals(4, metaInfo.getColumnCount());
        
        List fields = Arrays.asList(new String[]{"name", "id", "address", "products"});
        List classtypes = Arrays.asList(new String[]{"java.lang.String", "int",
                                                     "com.gemstone.gemfire.tools.databrowser.data.Address",
                                                     "java.util.List"});
        List<Integer> columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                                 IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                                 IntrospectionResult.UNKNOWN_TYPE_COLUMN,
                                                                 IntrospectionResult.COLLECTION_TYPE_COLUMN});

        verifyType(metaInfo, GemStoneCustomer.class, fields, classtypes, columntypes);       
        
        Collection temp = result.getQueryResult();    
        Iterator iter = temp.iterator();
        Assert.assertEquals(1, temp.size());        
        Object tuple = iter.next();
        
        try {
          int index = metaInfo.getColumnIndex("products");
          Object columnVal = result.getColumnValue(tuple, index);
          
          Assert.assertTrue(columnVal instanceof ResultSet);
          
          ResultSet rset = (ResultSet)columnVal;
          
          IntrospectionResult[] c_result =  rset.getIntrospectionResult();
          Assert.assertEquals(1, c_result.length);
          
          IntrospectionResult metaInfo1  = c_result[0];
          
          Assert.assertEquals(3, metaInfo1.getColumnCount());
          
          fields = Arrays.asList(new String[]{"name", "id", "price" });
          classtypes = Arrays.asList(new String[]{"java.lang.String","int","float"});
          columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                   IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                   IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
          verifyType(metaInfo1, Product.class, fields, classtypes, columntypes);         
        }
        catch (Exception e) {
          fail("Failed to execute test ",e);
        }
        
        try {
          int index = metaInfo.getColumnIndex("address");
          Object columnVal = result.getColumnValue(tuple, index);
          
          Assert.assertTrue(columnVal instanceof Address);
          
          //With the new implementation, now the Composite objects are directly returned and no type
          //conversion is performed. Hence the following code is not required.
          
//          ResultSet rset = (ResultSet)columnVal;
//          
//          IntrospectionResult[] c_result =  rset.getIntrospectionResult();
//          Assert.assertEquals(1, c_result.length);
//          
//          metaInfo  = c_result[0];
//          
//          Assert.assertEquals(3, metaInfo.getColumnCount());
//          
//          fields = Arrays.asList(new String[]{"street1", "city", "postalCode" });
//          classtypes = Arrays.asList(new String[]{"java.lang.String","java.lang.String", "int"});
//          columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                   IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                   IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
//          verifyType(metaInfo, Address.class, fields, classtypes, columntypes);         
        }
        catch (Exception e) {
          fail("Failed to execute test ",e);
        }                            
      }
    });     
  }
  
  public void testCompositeObjectWithNullValues() {
    browser.invoke(new SerializableRunnable() {
      public void run() {
//        GemFireMember[] members = connection.getMembers();
//        
//        Assert.assertEquals(1, members.length);
//        Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
        
        String queryString = "SELECT * FROM "+DEFAULT_REGION_PATH + " WHERE id = 2";
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
        
        Assert.assertEquals(4, metaInfo.getColumnCount());
        
        List fields = Arrays.asList(new String[]{"name", "id", "address", "products"});
        List classtypes = Arrays.asList(new String[]{"java.lang.String", "int",
                                                     "com.gemstone.gemfire.tools.databrowser.data.Address",
                                                     "java.util.List"});
        List<Integer> columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                                 IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                                 IntrospectionResult.UNKNOWN_TYPE_COLUMN,
                                                                 IntrospectionResult.COLLECTION_TYPE_COLUMN});

        verifyType(metaInfo, GemStoneCustomer.class, fields, classtypes, columntypes);       
        
        Collection temp = result.getQueryResult(); 
        Iterator iter = temp.iterator();
        
        Assert.assertEquals(1, temp.size());        
        Object tuple = iter.next();
        
        try {
          int index = metaInfo.getColumnIndex("products");
          Object columnVal = result.getColumnValue(tuple, index);
          
          Assert.assertTrue(columnVal instanceof ResultSet);
          
          ResultSet rset = (ResultSet)columnVal;
          
          IntrospectionResult[] c_result =  rset.getIntrospectionResult();
          Assert.assertEquals(0, c_result.length);
        }
        catch (Exception e) {
          fail("Failed to execute test ",e);
        }
        
        try {
          int index = metaInfo.getColumnIndex("address");
          Object columnVal = result.getColumnValue(tuple, index);
          
          Assert.assertTrue(columnVal == null);
          
          //With the new implementation, now the Composite objects are directly returned and no type
          //conversion is performed. Hence the following code is not required.
          
//          ResultSet rset = (ResultSet)columnVal;
//          
//          Assert.assertEquals(0, rset.getQueryResult().size());
//          
//          IntrospectionResult[] c_result =  rset.getIntrospectionResult();
//          Assert.assertEquals(1, c_result.length);
//          
//          metaInfo  = c_result[0];
//          
//          Assert.assertEquals(3, metaInfo.getColumnCount());
//          
//          fields = Arrays.asList(new String[]{"street1", "city", "postalCode" });
//          classtypes = Arrays.asList(new String[]{"java.lang.String","java.lang.String", "int"});
//          columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                   IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                   IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
//          verifyType(metaInfo, Address.class, fields, classtypes, columntypes);         
        }
        catch (Exception e) {
          fail("Failed to execute test ",e);
        }                     
      }
    });     
  }
  
  public void testCollectionTypeQuery() {
    browser.invoke(new SerializableRunnable() {
      public void run() {
//        GemFireMember[] members = connection.getMembers();
//        
//        Assert.assertEquals(1, members.length);
//        Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
        
        String queryString = "SELECT products FROM "+DEFAULT_REGION_PATH +" WHERE id = 1 OR id = 3";
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
        
        Assert.assertEquals(1, metaInfo.getColumnCount());
        
        {
           List fields = Arrays.asList(new String[]{"Result"});
           List classtypes = Arrays.asList(new String[]{"java.util.ArrayList"});
           List<Integer> columntypes = Arrays.asList(new Integer[] {IntrospectionResult.COLLECTION_TYPE_COLUMN});
           verifyType(metaInfo, java.util.ArrayList.class, fields, classtypes, columntypes);
        }
        
        Collection temp = result.getQueryResult(); 
        Iterator iter = temp.iterator();
        Assert.assertEquals(2, temp.size());  
        Object tuple = iter.next();
        
                
        try {
          tuple = result.getColumnValue(tuple, 0);
          
          Assert.assertTrue(tuple instanceof ResultSet);
          ResultSet rset = (ResultSet)tuple;
          Assert.assertEquals(1, rset.getQueryResult().size());
                  
          IntrospectionResult[] c_result =  rset.getIntrospectionResult();
          Assert.assertEquals(1, c_result.length);
          IntrospectionResult metaInfo1  = c_result[0];
          
          Assert.assertEquals(3, metaInfo1.getColumnCount());
          
          {
            List fields = Arrays.asList(new String[]{"name", "id", "price" });
            List classtypes = Arrays.asList(new String[]{"java.lang.String","int","float"});
            List columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                     IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                     IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
            verifyType(metaInfo1, Product.class, fields, classtypes, columntypes);
          }
        }
        catch (Exception e) {
          fail("Failed to execute test ",e);
        }
      }
    });     
  }
}
