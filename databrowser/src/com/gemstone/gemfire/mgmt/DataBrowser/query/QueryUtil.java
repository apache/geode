/*========================================================================= 
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.CollectionTypeResultImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.MapTypeResultImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.ObjectColumn;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.ObjectIntrospector;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.ObjectTypeResultImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.PdxHelper;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.PrimitiveTypeResultImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.ResultSetImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.StructTypeResultImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * This class is responsible to perform the query execution as well as conversion
 * of the query results into the Data-Browser representation. This class also provides
 * functionality to identify primitive and composite data types.
 * 
 * @author Hrishi
 **/
public class QueryUtil {
  
  /**
   * This method executes the specified query using the provided QueryService. It
   * then converts the GemFire Query execution results into the Data-Browser type.
   * 
   * @param service QueryService to be used.
   * @param query Query to be executed.
   * @return Data-Browser representation of the Query results.
   * @throws QueryExecutionException in case of error during query execution.
   */
  public static QueryResult executeQuery(QueryService service, String query, boolean enforceLimit) throws QueryExecutionException {
   
    if(null == query) {
     throw new IllegalArgumentException("query string can not be null"); 
    }
    
    if(enforceLimit) {
      LogUtil.info("QueryUtil.executeQuery ==>"+query);
      query = applyLimitClause(query);
      LogUtil.info("QueryUtil.executeQuery after enforcing LIMIT ==>"+query);
    }
    
    Query queryObj = service.newQuery(query);
    Object result = null;
    
    try {
      result = queryObj.execute();  
    }
    catch (Exception e) {
      //Exception during Query execution.
      throw new QueryExecutionException(e);
    }
    
    if(result == null) {
      return new QueryResult(new HashMap<Object, IntrospectionResult>(),
          new ArrayList(), queryObj.getStatistics());
      
    } else if(result instanceof SelectResults) {
      SelectResults temp = (SelectResults)result;
      
      ResultSet result_s = introspectCollectionObject(temp);
      QueryResult queryResult = new QueryResult(result_s, queryObj.getStatistics());
      return queryResult;     
    } else if (isPrimitiveType(result.getClass())) {  
      IntrospectionRepository repo = IntrospectionRepository.singleton();
      Map<Object, IntrospectionResult> types = new HashMap<Object, IntrospectionResult>();
      IntrospectionResult metaInfo = null;
      Class type = result.getClass();
      
      if(repo.isTypeIntrospected(type)) {
        metaInfo = repo.getIntrospectionResult(type);  
      } else {
        metaInfo = new PrimitiveTypeResultImpl(type);
        repo.addIntrospectionResult(type, metaInfo);        
      }
            
      types.put(type , metaInfo);
      
      List data = new ArrayList();
      data.add(result);
      
      QueryResult queryResult = new QueryResult(types, data , queryObj.getStatistics());
      return queryResult;      
      
    } else if(isCompositeType(result.getClass())) {
      Class type = result.getClass();
           
      ResultSet result_s = introspectCompositeObject(type, result);
      QueryResult queryResult = new QueryResult(result_s , queryObj.getStatistics());
      return queryResult;      
    }
    
    throw new QueryExecutionException("Could not parse GemFire Query result. type is "+result.getClass());
  }
  
  /**
   * This method introspect the java.util.Collection object and returns
   * the corresponding ResultSet representation.
   * 
   * @param result The java.util.Collection object which is to be introspected.
   * @return The Data-Browser representation of the input.
   * @throws QueryExecutionException in case of error during processing.
   */
  public static ResultSet introspectCollectionObject(Collection result) throws QueryExecutionException {
    Map<Object, IntrospectionResult> types = new HashMap<Object, IntrospectionResult>();    
        
    if((result == null) || (result.size() == 0) ) {
      return new ResultSetImpl(types, result);
    }
    
    //fix #830
    // for the the time being its workaround to counter  gemfire bug #40892
    ObjectType elementType = null;
    if(result instanceof SelectResults<?>) {
      elementType=((SelectResults<?>)result).getCollectionType().getElementType();
    }
    IntrospectionRepository repo = IntrospectionRepository.singleton();
    Iterator iter = result.iterator();
    
    while(iter.hasNext()) {
      Object obj = iter.next();
      
      if ( obj == null) {
        continue;
      } else if (obj instanceof Struct) {
        Struct struct = (Struct)obj;
        if(elementType == null)
          elementType =  struct.getStructType();
        IntrospectionResult metaInfo = null;
        
        if(!repo.isTypeIntrospected(elementType)) {
          metaInfo = new StructTypeResultImpl((StructType)elementType);  
          repo.addIntrospectionResult(elementType, metaInfo);
        } else {
          metaInfo = repo.getIntrospectionResult(elementType);
        }
        
        types.put(elementType, metaInfo);
                
      } else if (isPrimitiveType(obj.getClass())) {
        IntrospectionResult metaInfo = null;
        Class type = obj.getClass();
        
        if(!repo.isTypeIntrospected(type)) {
          metaInfo = new PrimitiveTypeResultImpl(type);
          repo.addIntrospectionResult(type, metaInfo);  
        } else {
          metaInfo = repo.getIntrospectionResult(type);
        }
        
        types.put(type, metaInfo);        
          
      } else if (isCompositeType(obj.getClass())) {
        IntrospectionResult metaInfo = null;
        Class type = obj.getClass();
        
        if(!repo.isTypeIntrospected(type)) {
          metaInfo = ObjectIntrospector.introspectClass(type);
          repo.addIntrospectionResult(type, metaInfo);
        } else {
          metaInfo = repo.getIntrospectionResult(type);
        }
        
        types.put(type, metaInfo);        
        
      } else if (isCollectionType(obj.getClass())) {
        IntrospectionResult metaInfo = null;
        Class type = obj.getClass();
                
        if(!repo.isTypeIntrospected(type)) {
          metaInfo = new CollectionTypeResultImpl(type);
          repo.addIntrospectionResult(type, metaInfo);
        } else {
          metaInfo = repo.getIntrospectionResult(type);
        }
        
        types.put(type, metaInfo);    
        
      } else if (isMapType(obj.getClass())) {
        IntrospectionResult metaInfo = null;
        Class type = obj.getClass();
                
        if(!repo.isTypeIntrospected(type)) {
          metaInfo = new MapTypeResultImpl(type);
          repo.addIntrospectionResult(type, metaInfo);
        } else {
          metaInfo = repo.getIntrospectionResult(type);
        }
        
        types.put(type, metaInfo);            
      } else if (isPdxInstance(obj)) {
        PdxHelper pdxHelper = PdxHelper.getInstance();
        IntrospectionResult metaInfo = null;
        
        /*PdxInfoType*/Object pdxInfoType = pdxHelper.getPdxInfoType(obj);
                
        if(!repo.isTypeIntrospected(pdxInfoType)) {
          metaInfo = pdxHelper.getPdxMetaInfo(obj);
          repo.addIntrospectionResult(pdxInfoType, metaInfo);
        } else
          metaInfo = repo.getIntrospectionResult(pdxInfoType);
        
        types.put(pdxInfoType, metaInfo);
      } else {
        LogUtil.info("Not yet implemented."+obj);
        continue;
      }  
    }
    
    return new ResultSetImpl(types, result);    
 }
  
  public static IntrospectionResult introspectJavaType(Class type) throws IntrospectionException {
    IntrospectionRepository repo = IntrospectionRepository.singleton();
    
    if(!repo.isTypeIntrospected(type)) {
     IntrospectionResult result = null;      
     if(isPrimitiveType(type)) {
       result = new PrimitiveTypeResultImpl(type);
     } else {
       result = ObjectIntrospector.introspectClass(type);
     }      
     repo.addIntrospectionResult(type, result);
     return result;
    }
    
    return repo.getIntrospectionResult(type);
  }
  
  
  public static ResultSet introspectCompositeObject(Class type, Object tuple) throws IntrospectionException {
    IntrospectionRepository repo = IntrospectionRepository.singleton();
    Map<Object, IntrospectionResult> types = new HashMap<Object, IntrospectionResult>();
    IntrospectionResult metaInfo = null;
    
    if(repo.isTypeIntrospected(type)) {
      metaInfo = repo.getIntrospectionResult(type);  
    } else {
      metaInfo = ObjectIntrospector.introspectClass(type);
      repo.addIntrospectionResult(type, metaInfo);        
    }
          
    types.put(type , metaInfo);
    
    List data = new ArrayList();
    //We will add the tuple in the result only if it is not null.
    //Otherwise we don't have any data & hence result would be empty.
    if(tuple != null)
     data.add(tuple);
    
    ResultSetImpl impl = new ResultSetImpl(types, data); 
   
    return impl;
  }
  
  /**
   * This method can be used to identify if a specified type is 'Primitive'.
   * 
   * @param type type to be checked.
   * @return true if the type is primitive
   *         false otherwise. 
   */
  public static boolean isPrimitiveType(Class type) {
    boolean result = type.isPrimitive()
                     || java.lang.Number.class.isAssignableFrom(type)
                     || java.lang.Character.class.isAssignableFrom(type)
                     || java.lang.Boolean.class.isAssignableFrom(type)
                     || java.lang.CharSequence.class.isAssignableFrom(type)
                     || java.util.Date.class.isAssignableFrom(type);    
     return result;    
  }
  
  /**
   * This method can be used to identify if a specified object is 'Composite'.
   * 
   * @param object the object to be checked.
   * @return true if the specified object is composite.
   *         false otherwise.
   */
  public static boolean isCompositeType(Class type) {
    boolean result = (!isPrimitiveType(type)
                      && !java.util.Collection.class.isAssignableFrom(type) 
                      && !type.isArray()
                      && !java.util.Map.class.isAssignableFrom(type)
                      && !isPdxInstanceType(type));    
    return result;    
  }  
  
  public static boolean isCollectionType(Class type) {
    return java.util.Collection.class.isAssignableFrom(type)
           || type.isArray() ;    
  }
  
  public static boolean isMapType(Class type) {
    return java.util.Map.class.isAssignableFrom(type);
  }

  /**
   * Checks whether give type (java.lang.Class) is same as or sub-type of
   * GemFire type PdxInstanceImpl.
   * 
   * @param type
   *          type to be checked
   * @return true if given type is same as or sub-type of GemFire type
   *         PdxInstanceImpl, false otherwise
   */
  public static boolean isPdxInstanceType(Class<?> type) {
    return PdxHelper.getInstance().isPdxInstanceType(type);
  }
  
  /**
   * Checks whether give type (java.lang.Class) of given object is same as or 
   * sub-type of GemFire type PdxInstanceImpl.
   * 
   * @param object
   *          object to be checked
   * @return true if type of given object is same as or sub-type of GemFire type
   *         PdxInstanceImpl, false otherwise
   */
  public static boolean isPdxInstance(Object object) {
    return PdxHelper.getInstance().isPdxInstance(object);
  }
  
  public static ResultSet introspectArrayType(Class type, Object array) throws IntrospectionException {
    if(type.isArray()) {
      Class component = type.getComponentType();
      //Get the type representation of array component;
      IntrospectionResult metaInfo = IntrospectionRepository.singleton().introspectType(component);
      ArrayList<Object> result = new ArrayList<Object>();    
      
      if(array != null) {
        int length = Array.getLength(array);
        for(int i = 0; i < length ; i++) {
          result.add(Array.get(array, i));
        }
      }
      
      Map<Object, IntrospectionResult> types = new HashMap<Object, IntrospectionResult>();
      types.put(component, metaInfo);
      
      return new ResultSetImpl(types, result);      
    }

    throw new IllegalArgumentException("The parameter is not of type Array. "+type.getCanonicalName());
  
  }
  
  
  public static boolean isColumnDeclaredInType(IntrospectionResult type, int index) {
    if(type.getResultType() == IntrospectionResult.COMPOSITE_TYPE_RESULT) {
      ObjectTypeResultImpl temp = (ObjectTypeResultImpl)type;
      ObjectColumn col = temp.getObjectColumn(index);
      if(col == null)
       return false; 
      return col.getDeclaringClass().equals(type.getJavaType());
    }
    
    return true;
  }
  
  static String applyLimitClause(String query) {
    Matcher matcher = SELECT_EXPR_PATTERN.matcher(query);
   
    if(matcher.matches()) {
      Matcher limit_matcher = SELECT_WITH_LIMIT_EXPR_PATTERN.matcher(query);
      boolean matchResult = limit_matcher.matches();
      
      if(!matchResult) {
        long limit = DataBrowserPreferences.getQueryLimit();
        String result = new String(query);
        result += " LIMIT "+limit; 
        return result;
      }
    }
    
    return query;
  }
  
  static final String SELECT_EXPR = "\\s*SELECT\\s+.+\\s+FROM\\s+.+";
  static Pattern SELECT_EXPR_PATTERN = Pattern.compile(SELECT_EXPR, Pattern.CASE_INSENSITIVE);
  
  static final String SELECT_WITH_LIMIT_EXPR = "\\s*SELECT\\s+.+\\s+FROM(\\s+|(.*\\s+))LIMIT\\s+[0-9]+.*";
  static Pattern SELECT_WITH_LIMIT_EXPR_PATTERN = Pattern.compile(SELECT_WITH_LIMIT_EXPR, Pattern.CASE_INSENSITIVE);
}
