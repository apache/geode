package io.pivotal.gemfire.spark.connector.javaapi;

import io.pivotal.gemfire.spark.connector.GemFireConnectionConf;
import io.pivotal.gemfire.spark.connector.GemFireSQLContextFunctions;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Java API wrapper over {@link org.apache.spark.sql.SQLContext} to provide GemFire
 * OQL functionality.
 *
 * <p></p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * io.pivotal.gemfire.spark.connector.javaapi.GemFireJavaUtil} class.</p>
 */
public class GemFireJavaSQLContextFunctions {

  public final GemFireSQLContextFunctions scf;

  public GemFireJavaSQLContextFunctions(SQLContext sqlContext) {
    scf = new GemFireSQLContextFunctions(sqlContext);
  }

  public <T> DataFrame gemfireOQL(String query) {
    DataFrame df = scf.gemfireOQL(query, scf.defaultConnectionConf());
    return df;
  }

  public <T> DataFrame gemfireOQL(String query, GemFireConnectionConf connConf) {
    DataFrame df = scf.gemfireOQL(query, connConf);
    return df;
  }

}
