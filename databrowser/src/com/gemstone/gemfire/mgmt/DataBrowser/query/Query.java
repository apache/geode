/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.query;

/**
 * Replicating the GFE query interface authored by Eric Zoerner
 * @author mghosh
 *
 */
public interface Query {

  /**
   * Return the original query string that was specified in the constructor.
   * @return the original query string
   */
  public String getQueryString();
  
  
  /**
   * Execute this query and returns an object that represent its
   * result.  If the query resolves to a primitive type, an instance
   * of the corresponding wrapper type ({@link java.lang.Integer},
   * etc.) is returned.  If the query resolves to more than one
   * object, a {@link SelectResults} is returned.
   *
   * @return The object that represents the result of the query. Note that if
   *         a query is just a select statement then it will return a result
   *         that is an instance of {@link SelectResults}. However, since a query is not
   *         necessarily just a select statement, the return type of this
   *         method is <code>Object</code>.
   *         For example, the query <code><b>(select distinct * from /rgn).size</b></code>
   *         returns an instance of <code>java.lang.Integer</code>.
   *
   * @throws FunctionDomainException
   *         A function was applied to a parameter that is improper
   *         for that function.  For example, the ELEMENT function
   *         was applied to a collection of more than one element 
   * @throws TypeMismatchException
   *         If a bound parameter is not of the expected type.
   * @throws NameResolutionException
   *         If a name in the query cannot be resolved.
   * @throws IllegalArgumentException
   *         The number of bound parameters does not match the number
   *         of placeholders
   * @throws IllegalStateException
   *         If the query is not permitted on this type of region
   */
  public Object execute();
//    throws FunctionDomainException, TypeMismatchException, NameResolutionException,
//           QueryInvocationTargetException;
  
  /**
   * Executes this query with the given parameters and returns an
   * object that represent its result.  If the query resolves to a
   * primitive type, an instance of the corresponding wrapper type
   * ({@link java.lang.Integer}, etc.) is returned.  If the query
   * resolves to more than one object, a {@link SelectResults} is
   * returned.
   *
   * @param params
   *        Values that are bound to parameters (such as
   *        <code>$1</code>) in this query. 
   *
   * @return The object that represents the result of the query. Note that if
   *         a query is just a select statement then it will return a result
   *         that is an instance of {@link SelectResults}. However, since a query is not
   *         necessarily just a select statement, the return type of this
   *         method is <code>Object</code>.
   *         For example, the query <code><b>(select distinct * from /rgn).size</b></code>
   *         returns an instance of <code>java.lang.Integer</code>.
   *
   * @throws FunctionDomainException
   *         A function was applied to a parameter that is improper
   *         for that function.  For example, the ELEMENT function
   *         was applied to a collection of more than one element 
   * @throws TypeMismatchException
   *         If a bound parameter is not of the expected type.
   * @throws NameResolutionException
   *         If a name in the query cannot be resolved.
   * @throws IllegalArgumentException
   *         The number of bound parameters does not match the number
   *         of placeholders
   * @throws IllegalStateException
   *         If the query is not permitted on this type of region
   */
  public Object execute(Object[] params);
//    throws FunctionDomainException, TypeMismatchException, NameResolutionException,
//           QueryInvocationTargetException;

  /**
   * Compiles this <code>Query</code> to achieve higher performance
   * execution.
   *
   * @throws TypeMismatchException 
   *         If the compile-time type of a name, parameter, or
   *         expression is not the expected type 
   * @throws QueryInvalidException 
   *         The syntax of the query string is not correct
   */
  public void compile(); 
 //   throws TypeMismatchException, NameResolutionException;
  
 // IPreferenceConstants


  /**
   * Return whether this query has been compiled into VM bytecodes.
   * @return <code>true</code> if this query has been compiled into bytecodes
   */
  public boolean isCompiled();

  
  /**
   * Get statistics information for this query.
   */
  public QueryStatistics getStatistics();
}
