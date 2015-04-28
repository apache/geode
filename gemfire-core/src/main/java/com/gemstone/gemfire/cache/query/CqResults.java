/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

/**
 * Represents the results of a CQ query that is executed using 
 * {@linkplain com.gemstone.gemfire.cache.query.CqQuery#executeWithInitialResults()}
 * The result will contain the instances of {@link Struct} having key and value 
 * of the region entry that satisfy the CQ query condition.
 * <pre>
 * ClientCache cache = ...
 * QueryService queryService = PoolManager.find("client").getQueryService();
 * CqAttributesFactory cqAf = new CqAttributesFactory();
 * CqAttributes cqa = cqAf.create();
 * 
 * String cqQueryStr = "SELECT * FROM /root/employees " +
 *   "WHERE salary > 50000";
 *   
 * CqQuery cq = queryService.newCq("MyCq", cqQueryStr, cqa); 
 * CqResults results = cq.executeWithInitialResults();     
 * 
 * for (Object o : results.asList()) {
 *   Struct s = (Struct)o;
 *   System.out.println("key : " + s.get("key") + " value : " + s.get("value"));
 * }
 *
 * </pre>
 * 
 * @see com.gemstone.gemfire.cache.query.Query#execute()
 * @see com.gemstone.gemfire.cache.query.CqQuery#executeWithInitialResults()
 * 
 * @author anil gingade
 * @since 6.5
 */
public interface CqResults<E> extends SelectResults<E> {
}
