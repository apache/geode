package com.gemstone.gemfire.tools.databrowser.dunit;

import hydra.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryExecutionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;
import com.gemstone.gemfire.tools.databrowser.data.Customer;
import com.gemstone.gemfire.tools.databrowser.data.Product;

import dunit.SerializableRunnable;

public class MultipleTypeResultDUnitTest extends DataBrowserDUnitTestCase {

  public MultipleTypeResultDUnitTest(String name) {
    super(name); 
  }
  
  @Override
  public void populateData() {
    List result = new ArrayList();
    result.add(new Customer(1, "Tom", "Pune"));
    result.add(new Product(1, "GemFire Enterprise", 40.0f));
    result.add("Hello World");
    populateRegion(DEFAULT_REGION_PATH, result.toArray());    
  }  
  
  public void testMultipleTypeQueryResult() throws Exception {
    
    browser.invoke(new SerializableRunnable() {
      public void run() {
//        GemFireMember[] members = connection.getMembers();
//        
//        Assert.assertEquals(1, members.length);
//        Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
        
        String queryString = "SELECT * FROM "+DEFAULT_REGION_PATH;
        QueryResult result = null;
        try {
          result = connection.executeQuery(queryString, null);
          Assert.assertNotNull(result); 
        }
        catch (QueryExecutionException e) {
          fail("Failed to execute query through data browser",e);
        }
        
        IntrospectionResult[] metaInfo = result.getIntrospectionResult();
       
        Assert.assertEquals(3, metaInfo.length);
        
        List<String> expectedTypes = Arrays.asList(new String[] {"com.gemstone.gemfire.tools.databrowser.data.Product",
                                                                  "com.gemstone.gemfire.tools.databrowser.data.Customer",
                                                                  "java.lang.String"});
                        
        boolean product = false, customer = false, str = false;        
        for(int i = 0 ; i < metaInfo.length ; i++) {
          Assert.assertNotNull(metaInfo[i].getJavaType());         
          
          if(metaInfo[i].getJavaType().getCanonicalName().equals(expectedTypes.get(0))) {
            product = true;
            verifyProductType(metaInfo[i]);
          } else if (metaInfo[i].getJavaType().getCanonicalName().equals(expectedTypes.get(1))) {
            customer = true;
            verifyCustomerType(metaInfo[i]);
          } else if (metaInfo[i].getJavaType().getCanonicalName().equals(expectedTypes.get(2))) {
            try {
              Class t = Class.forName(expectedTypes.get(2));
              verifyPrimitiveType(metaInfo[i], t );
              str = true;
            }
            catch (ClassNotFoundException e) { fail(e.getMessage()); }            
          }           
        }        
        
        Assert.assertTrue(product);
        Assert.assertTrue(customer);
        Assert.assertTrue(str);
      }
    });       
  }
  
  protected void verifyProductType(IntrospectionResult metaInfo) {
    List fields = Arrays.asList(new String[] {"id", "name", "price"});
    List classtypes = Arrays.asList(new String[] {"int","java.lang.String", "float"});   
    List<Integer> columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                             IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                             IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
    Log.getLogWriter().info("Verifying Product type");
    verifyType(metaInfo, Product.class,  fields, classtypes, columntypes);    
  }
  
  protected void verifyCustomerType(IntrospectionResult metaInfo) {
    List fields = Arrays.asList(new String[] { "name", "id", "address" });
    List classtypes = Arrays.asList(new String[] { "java.lang.String",
        "java.lang.Integer", "java.lang.String" });
    List<Integer> columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                             IntrospectionResult.PRIMITIVE_TYPE_COLUMN,
                                                             IntrospectionResult.PRIMITIVE_TYPE_COLUMN });
    Log.getLogWriter().info("Verifying Customer type");
    verifyType(metaInfo, Customer.class, fields, classtypes, columntypes);
  }  
}
