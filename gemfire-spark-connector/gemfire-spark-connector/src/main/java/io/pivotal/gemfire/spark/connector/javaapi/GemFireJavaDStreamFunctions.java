package io.pivotal.gemfire.spark.connector.javaapi;

import io.pivotal.gemfire.spark.connector.GemFireConnectionConf;
import io.pivotal.gemfire.spark.connector.streaming.GemFireDStreamFunctions;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import java.util.Properties;

import static io.pivotal.gemfire.spark.connector.javaapi.JavaAPIHelper.*;

/**
 * A Java API wrapper over {@link org.apache.spark.streaming.api.java.JavaDStream}
 * to provide GemFire Spark Connector functionality.
 *
 * <p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * io.pivotal.gemfire.spark.connector.javaapi.GemFireJavaUtil} class.</p>
 */ 
public class GemFireJavaDStreamFunctions<T> {
  
  public final GemFireDStreamFunctions<T> dsf;

  public GemFireJavaDStreamFunctions(JavaDStream<T> ds) {
    this.dsf = new GemFireDStreamFunctions<T>(ds.dstream());
  }

  /**
   * Save the JavaDStream to GemFire key-value store.
   * @param regionPath the full path of region that the DStream is stored  
   * @param func the PairFunction that converts elements of JavaDStream to key/value pairs
   * @param connConf the GemFireConnectionConf object that provides connection to GemFire cluster
   * @param opConf the optional parameters for this operation
   */
  public <K, V> void saveToGemfire(
    String regionPath, PairFunction<T, K, V> func, GemFireConnectionConf connConf, Properties opConf) {
    dsf.saveToGemfire(regionPath, func, connConf, propertiesToScalaMap(opConf));
  }

  /**
   * Save the JavaDStream to GemFire key-value store.
   * @param regionPath the full path of region that the DStream is stored  
   * @param func the PairFunction that converts elements of JavaDStream to key/value pairs
   * @param opConf the optional  parameters for this operation
   */
  public <K, V> void saveToGemfire(
          String regionPath, PairFunction<T, K, V> func, Properties opConf) {
    dsf.saveToGemfire(regionPath, func, dsf.defaultConnectionConf(), propertiesToScalaMap(opConf));
  }

  /**
   * Save the JavaDStream to GemFire key-value store.
   * @param regionPath the full path of region that the DStream is stored
   * @param func the PairFunction that converts elements of JavaDStream to key/value pairs
   * @param connConf the GemFireConnectionConf object that provides connection to GemFire cluster
   */
  public <K, V> void saveToGemfire(
          String regionPath, PairFunction<T, K, V> func, GemFireConnectionConf connConf) {
    dsf.saveToGemfire(regionPath, func, connConf, emptyStrStrMap());
  }

  /**
   * Save the JavaDStream to GemFire key-value store.
   * @param regionPath the full path of region that the DStream is stored
   * @param func the PairFunction that converts elements of JavaDStream to key/value pairs
   */
  public <K, V> void saveToGemfire(
          String regionPath, PairFunction<T, K, V> func) {
    dsf.saveToGemfire(regionPath, func, dsf.defaultConnectionConf(), emptyStrStrMap());
  }

}
