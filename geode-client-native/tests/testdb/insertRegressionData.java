/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package testdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class insertRegressionData{
  public static void main(String[] args) {
    System.out.println("Inserting values in Mysql database table!");
    System.out.println("argument is " + args[0]);
    Connection con = null;
    String url = "jdbc:mysql://nativedb.gemstone.com/nativedb";
    //String db = "jdbctutorial";
    String driver = "com.mysql.jdbc.Driver";
    //System.out.println("argument is " + args[0]);
    try{
      Class.forName(driver);
      con = DriverManager.getConnection(url,"nativedb","Foo9ahsa");
      try{
	Statement st = con.createStatement();
        int val = st.executeUpdate("LOAD DATA LOCAL INFILE '" + args[0] + "' " +
		" INTO TABLE csvtemp_table" +
	        " FIELDS TERMINATED BY ',' " +
		" LINES TERMINATED BY '\n' ;");
	System.out.println(val + " row affected");
      }
      catch (SQLException s){
	System.out.println("SQL statement is not executed!" + s);
      }
    }
    catch (Exception e){
      e.printStackTrace();
    }
  }
}
