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
package com.gemstone.gemfire.internal.jta.dunit;
                                                                                                                             
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.jta.CacheUtils;
                                                                                                                             
/**
*This is thread class
*The objective of this thread class is to implement the inserts and commit
*This thread will be called from TxnManagerMultiThreadDUnitTest.java
*This is to test the concurrent execution of the run method and see if transaction manager handles it properly
*
*
*@author Prafulla Chaudhari
*
*/


public class CommitThread implements Runnable{

/////constructor/////

public Thread thd;
public String threadName;
private LogWriter log;

public CommitThread(String name, LogWriter log){
    threadName=name;
    this.log = log;
    thd = new Thread(this, threadName);
    thd.start();
}//end of constuctor CommitThread

/////synchronized method/////
/*
*This is to make sure that key field in table is getting inserted with a unique value by every thread.
*
*/
                                                                                                                             
static int keyFld = 0;
                                                                                                                             
synchronized public static int getUniqueKey(){
    keyFld = keyFld + 5;
    return keyFld;
}

/*
*Following the the run method of this thread.
*This method is implemented to inserts the rows in the database and commit them
*
*/

public void run(){
                                                                                                                             
    //Region currRegion=null;
    Cache cache;
    int tblIDFld;
    String tblNameFld;
//    boolean to_continue = true;
    final int XA_INSERTS= 2;
                                                                                                                             
    //get the cache
    //this is used to get the context for transaction later in the same method
    cache = TxnManagerMultiThreadDUnitTest.getCache ();
                                                                                                                             
    //get the table name from CacheUtils
    String tblName = CacheUtils.getTableName();
                                                                                                                             
    tblIDFld = 1;
    tblNameFld = "thdOneCommit";
                                                                                                                             
    //initialize cache and get user transaction                                                                                                                              
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection xa_conn = null;
    try {
        ta = (UserTransaction)ctx.lookup("java:/UserTransaction");
        //ta.setTransactionTimeout(300);
        } catch (NamingException nme) {
               nme.printStackTrace();
        } catch (Exception e) {
               e.printStackTrace();
           }
                                                                                                                             
    try{
        DataSource d1 = (DataSource)ctx.lookup("java:/SimpleDataSource");
        Connection con = d1.getConnection();
        con.close();                                                                                                                     
        //Begin the user transaction
        ta.begin();
                                                                                                                             
        //Obtain XAPooledDataSource                                                                                                                              
        DataSource da = (DataSource)ctx.lookup("java:/XAPooledDataSource");
                                                                                                                             
        //obtain connection from XAPooledDataSource                                                                                                                              
        xa_conn = da.getConnection();
                                                                                                                             
        Statement xa_stmt = xa_conn.createStatement();
                                                                                                                             
        String sqlSTR;
                                                                                                                             
        //get the unique value for key to be inserted
        int uniqueKey = getUniqueKey();
                                                                                                                             
        //insert XA_INSERTS rows into timestamped table
        for (int i=0; i<XA_INSERTS;i++){
        tblIDFld= tblIDFld + uniqueKey+i;
        sqlSTR = "insert into " + tblName + " values (" + tblIDFld + "," + "'" + tblNameFld + "'" + ")" ;
        //log.info("Thread= "+Thread.currentThread()+" ... sqlStr= "+ sqlSTR + "Before  update");
        xa_stmt.executeUpdate(sqlSTR);
        //log.info("Thread= "+Thread.currentThread()+" ... sqlStr= "+ sqlSTR + "after  update");
        }
                                                                                                                             
        //close the Simple and XA statements                                                                                                                              
        xa_stmt.close();
                                                                                                                             
        //close the connections
        xa_conn.close();
                                                                                                                             
        //log.info("Thread Before Commit..."+Thread.currentThread());

         //commit the transaction
        ta.commit ();
                                                                                                                             
        }
        catch (NamingException nme){
            nme.printStackTrace();
        }
        catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        catch (Exception e){
           e.printStackTrace();
        }
        finally
        {
               if (xa_conn != null) {
                     try {
                        //close the connections
                        xa_conn.close();
                        } catch (Exception e) {
                          e.printStackTrace();
                           }
                 }
        }
                                                                                                                             
    log.info(XA_INSERTS+": Rows were inserted and committed successfully");
}//end of run method


}
