package com.gemstone.gemfire.tools.databrowser.dunit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;
import junit.framework.Assert;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.tools.databrowser.data.Address;
import com.gemstone.gemfire.tools.databrowser.data.GemStoneCustomer;
import com.gemstone.gemfire.tools.databrowser.data.Portfolio;
import com.gemstone.gemfire.tools.databrowser.data.Position;
import com.gemstone.gemfire.tools.databrowser.data.Product;
import com.gemstone.gemfire.mgmt.DataBrowser.query.*;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;

public class TestMapTypeDUnitTest extends DataBrowserDUnitTestCase {
  
  public TestMapTypeDUnitTest(String name) {
    super(name); 
  }
  
  @Override
  public void populateData() {
    List result = new ArrayList();
    
    Portfolio pf = new Portfolio();
    pf.setId("IBM");
    pf.setType("Equity");
    pf.setStatus("Active");
    
    HashMap<String , Position> values = new HashMap<String, Position>();    
    values.put("sec1", new Position("sec1",30.4, 1000d));
    values.put("sec2", new Position("sec2",20.6, 1700d));
    
    pf.setPositions(values);   
    
    result.add(pf);
    
    populateRegion(DEFAULT_REGION_PATH, result.toArray()); 
  }
  
  public void testPortfolioObject() {
//    browser.invoke(new SerializableRunnable() {
//      public void run() {
//        GemFireMember[] members = connection.getMembers();
//        
//        Assert.assertEquals(1, members.length);
//        Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
//        
//        String queryString = "SELECT * FROM "+DEFAULT_REGION_PATH + " pf WHERE pf.id = 'IBM'";
//        QueryResult result = null;
//        try {
//          result = connection.executeQuery(queryString, null);
//          Assert.assertNotNull(result); 
//        }
//        catch (QueryExecutionException e) {
//          fail("Failed to execute query through data browser",e);
//        }
//        
//        IntrospectionResult [] result_t = result.getIntrospectionResult();
//        Assert.assertEquals(1, result_t.length);
//        IntrospectionResult metaInfo = result_t[0];
//        
//        Assert.assertEquals(4, metaInfo.getColumnCount());
//        
//        {
//          List fields = Arrays.asList(new String[]{"id", "type", "status", "positions"});                                                   
//          List classtypes = Arrays.asList(new String[]{"java.lang.String","java.lang.String","java.lang.String","java.util.HashMap"});
//          List<Integer> columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                                   IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                                   IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                                   IntrospectionResult.MAP_TYPE_COLUMN});
//  
//          verifyType(metaInfo, Portfolio.class, fields, classtypes, columntypes);  
//        }
//        
//        Collection temp = result.getQueryResult();        
//        Assert.assertEquals(1, temp.size());        
//        Iterator iter = temp.iterator();
//        Object tuple = iter.next();
//        
//        try {
//          int index = metaInfo.getColumnIndex("positions");
//          Object columnVal = result.getColumnValue(tuple, index);
//          
//          Assert.assertTrue(columnVal instanceof MapResult);
//          MapResult rset = (MapResult)columnVal;
//          Assert.assertEquals(2, rset.getMap().size());
//          
//          //Verify Key set.
//          {
//            ResultSet temp1 = rset.getResultSetForKeys();
//            IntrospectionResult[] c_result = temp1.getIntrospectionResult();
//            Assert.assertEquals(1, c_result.length);
//            IntrospectionResult metaInfo1  = c_result[0];
//            
//            Assert.assertEquals(1, metaInfo1.getColumnCount());            
//            
//            List fields = Arrays.asList(new String[]{"Result"});
//            List classtypes = Arrays.asList(new String[]{"java.lang.String"});
//            List columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
//            verifyType(metaInfo1, java.lang.String.class, fields, classtypes, columntypes);
//          }
//                    
//          //Verify Value set. 
//          {
//            ResultSet temp1 = rset.getResultSetForValues();
//            IntrospectionResult[] c_result = temp1.getIntrospectionResult();
//            Assert.assertEquals(1, c_result.length);
//            IntrospectionResult metaInfo1  = c_result[0];
//            
//            Assert.assertEquals(3, metaInfo1.getColumnCount());            
//            
//            List fields = Arrays.asList(new String[]{"secId", "mktValue", "qty" });
//            List classtypes = Arrays.asList(new String[]{"java.lang.String","double","double"});
//            List columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                     IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                     IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
//            verifyType(metaInfo1, Position.class, fields, classtypes, columntypes);
//          }
//        }
//        catch (Exception e) {
//          fail("Failed to execute test ",e);
//        }
//      }
//    });     
  }
  
  public void testMapData() {
//    browser.invoke(new SerializableRunnable() {
//      public void run() {
//        GemFireMember[] members = connection.getMembers();
//        
//        Assert.assertEquals(1, members.length);
//        Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
//        
//        String queryString = "SELECT pf.positions FROM "+DEFAULT_REGION_PATH + " pf WHERE pf.id = 'IBM'";
//        QueryResult result = null;
//        try {
//          result = connection.executeQuery(queryString, null);
//          Assert.assertNotNull(result); 
//        }
//        catch (QueryExecutionException e) {
//          fail("Failed to execute query through data browser",e);
//        }
//        
//        IntrospectionResult [] result_t = result.getIntrospectionResult();
//        Assert.assertEquals(1, result_t.length);
//        IntrospectionResult metaInfo = result_t[0];
//        
//        Assert.assertEquals(1, metaInfo.getColumnCount());
//        
//        {
//          List fields = Arrays.asList(new String[]{IntrospectionResult.CONST_COLUMN_NAME});
//          List classtypes = Arrays.asList(new String[]{"java.util.HashMap"});
//          List<Integer> columntypes = Arrays.asList(new Integer[] {IntrospectionResult.MAP_TYPE_COLUMN});
//  
//          verifyType(metaInfo, java.util.HashMap.class, fields, classtypes, columntypes);  
//        }
//        
//        Collection temp = result.getQueryResult();        
//        Assert.assertEquals(1, temp.size());
//        
//        Iterator iter = temp.iterator();
//        Object tuple = iter.next();
//        
//        try {
//          tuple = result.getColumnValue(tuple, 0);
//          
//          Assert.assertTrue(tuple instanceof MapResult);
//          MapResult rset = (MapResult)tuple;
//          Assert.assertEquals(2, rset.getMap().size());
//          
//          //Verify Key set.
//          {
//            ResultSet temp1 = rset.getResultSetForKeys();
//            IntrospectionResult[] c_result = temp1.getIntrospectionResult();
//            Assert.assertEquals(1, c_result.length);
//            IntrospectionResult metaInfo1  = c_result[0];
//            
//            Assert.assertEquals(1, metaInfo1.getColumnCount());            
//            
//            List fields = Arrays.asList(new String[]{"Result"});
//            List classtypes = Arrays.asList(new String[]{"java.lang.String"});
//            List columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
//            verifyType(metaInfo1, java.lang.String.class, fields, classtypes, columntypes);
//          }
//                    
//          //Verify Value set. 
//          {
//            ResultSet temp1 = rset.getResultSetForValues();
//            IntrospectionResult[] c_result = temp1.getIntrospectionResult();
//            Assert.assertEquals(1, c_result.length);
//            IntrospectionResult metaInfo1  = c_result[0];
//            
//            Assert.assertEquals(3, metaInfo1.getColumnCount());            
//            
//            List fields = Arrays.asList(new String[]{"secId", "mktValue", "qty" });
//            List classtypes = Arrays.asList(new String[]{"java.lang.String","double","double"});
//            List columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                     IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                     IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
//            verifyType(metaInfo1, Position.class, fields, classtypes, columntypes);
//          }
//        }
//        catch (Exception e) {
//          fail("Failed to execute test ",e);
//        }
//      }
//    });     
  }
  
  public void testStructType() {
//    browser.invoke(new SerializableRunnable() {
//      public void run() {
//        GemFireMember[] members = connection.getMembers();
//        
//        Assert.assertEquals(1, members.length);
//        Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
//        
//        String queryString = "SELECT pf.id, pf.positions FROM "+DEFAULT_REGION_PATH + " pf WHERE pf.id = 'IBM'";
//        QueryResult result = null;
//        try {
//          result = connection.executeQuery(queryString, null);
//          Assert.assertNotNull(result); 
//        }
//        catch (QueryExecutionException e) {
//          fail("Failed to execute query through data browser",e);
//        }
//        
//        IntrospectionResult [] result_t = result.getIntrospectionResult();
//        Assert.assertEquals(1, result_t.length);
//        IntrospectionResult metaInfo = result_t[0];
//        
//        Assert.assertEquals(2, metaInfo.getColumnCount());
//        
//        {
//          List fields = Arrays.asList(new String[]{"id", "positions"});
//          List classtypes = Arrays.asList(new String[]{"java.lang.Object","java.lang.Object"});
//          List<Integer> columntypes = Arrays.asList(new Integer[] {IntrospectionResult.UNKNOWN_TYPE_COLUMN,
//                                                                   IntrospectionResult.UNKNOWN_TYPE_COLUMN});
//  
//          verifyType(metaInfo, StructType.class, fields, classtypes, columntypes);  
//        }
//        
//        Collection temp = result.getQueryResult();        
//        Assert.assertEquals(1, temp.size());
//        
//        Iterator iter = temp.iterator();
//        Object tuple = iter.next();              
//        
//        try {
//          
//          int index = metaInfo.getColumnIndex("id");
//          Class colClass = metaInfo.getColumnClass(tuple, index);
//          Assert.assertTrue(colClass.getCanonicalName().equals("java.lang.String"));
//          Assert.assertTrue(metaInfo.getColumnType(tuple,index) == IntrospectionResult.PRIMITIVE_TYPE_COLUMN);
//                    
//          index = metaInfo.getColumnIndex("positions");
//          tuple = result.getColumnValue(tuple, index);
//          
//          Assert.assertTrue(tuple instanceof MapResult);
//          MapResult rset = (MapResult)tuple;
//          Assert.assertEquals(2, rset.getMap().size());
//          
//          //Verify Key set.
//          {
//            ResultSet temp1 = rset.getResultSetForKeys();
//            IntrospectionResult[] c_result = temp1.getIntrospectionResult();
//            Assert.assertEquals(1, c_result.length);
//            IntrospectionResult metaInfo1  = c_result[0];
//            
//            Assert.assertEquals(1, metaInfo1.getColumnCount());            
//            
//            List fields = Arrays.asList(new String[]{"Result"});
//            List classtypes = Arrays.asList(new String[]{"java.lang.String"});
//            List columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
//            verifyType(metaInfo1, java.lang.String.class, fields, classtypes, columntypes);
//          }
//                    
//          //Verify Value set. 
//          {
//            ResultSet temp1 = rset.getResultSetForValues();
//            IntrospectionResult[] c_result = temp1.getIntrospectionResult();
//            Assert.assertEquals(1, c_result.length);
//            IntrospectionResult metaInfo1  = c_result[0];
//            
//            Assert.assertEquals(3, metaInfo1.getColumnCount());            
//            
//            List fields = Arrays.asList(new String[]{"secId", "mktValue", "qty" });
//            List classtypes = Arrays.asList(new String[]{"java.lang.String","double","double"});
//            List columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                     IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
//                                                     IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
//            verifyType(metaInfo1, Position.class, fields, classtypes, columntypes);
//          }
//        }
//        catch (Exception e) {
//          fail("Failed to execute test ",e);
//        }
//      }
//    });     
  }

}
