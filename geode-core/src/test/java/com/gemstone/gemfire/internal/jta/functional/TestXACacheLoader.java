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
