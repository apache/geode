/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.jta.functional;

import com.gemstone.gemfire.cache.*;

import javax.sql.DataSource;
import javax.naming.*;

import java.sql.*;
import com.gemstone.gemfire.internal.jta.CacheUtils;

import javax.transaction.*;
/**
 * A <code>CacheLoader</code> used in testing.  Users should override
 * the "2" method.
 *
 * @see #wasInvoked
 * @see TestCacheWriter
 *
 * @author GemStone Systems, Inc.
 *
 * @since 3.0
 */
public class TestXACacheLoader implements CacheLoader{

	public static String tableName = "";
	
	 public final Object load(LoaderHelper helper) throws CacheLoaderException {
	    System.out.println("In Loader.load for"+helper.getKey());
	    return loadFromDatabase(helper.getKey());
	  }

	  private Object loadFromDatabase(Object ob) 
	  {  
	  	Object obj =  null;
	  	try{
		Context ctx = CacheFactory.getAnyInstance().getJNDIContext();
	  	DataSource ds = (DataSource)ctx.lookup("java:/XAPooledDataSource");
	    	Connection conn = ds.getConnection();
	  	Statement stm = conn.createStatement();
	    ResultSet rs = stm.executeQuery("select name from "+ tableName +" where id = ("+(new Integer(ob.toString())).intValue()+")");
	    rs.next();
	    obj = rs.getString(1);
	    stm.close();
	    conn.close();
	    return obj;
	  	}catch(Exception e)
		{
	  		e.printStackTrace();
		}
	  	//conn.close();
	    return obj;
	  }
	
  public static void main(String[] args) throws Exception {

    	
      Region currRegion;
  	  Cache cache = null;
//  	  DistributedSystem system = null;
//  	  HashMap regionDefaultAttrMap = new HashMap();
  	  tableName = CacheUtils.init("TestXACache");
  	  cache = CacheUtils.getCache();
      currRegion = cache.getRegion("root");
      try{	
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction)ctx.lookup("java:/UserTransaction");
      utx.begin();
      AttributesFactory fac = new AttributesFactory(currRegion.getAttributes());
      fac.setCacheLoader(new TestXACacheLoader());
      Region re = currRegion.createSubregion("employee",fac.create());
      System.out.println(re.get(args[0]));
      utx.rollback();
      System.out.println(re.get(args[0]));
      cache.close();
      System.exit(1);
    }catch(Exception e)
        {
        e.printStackTrace();
        cache.close();
        }
  }

/* (non-Javadoc)
 * @see com.gemstone.gemfire.cache.CacheCallback#close()
 */
public void close() {
	// TODO Auto-generated method stub
	
}
 
}
