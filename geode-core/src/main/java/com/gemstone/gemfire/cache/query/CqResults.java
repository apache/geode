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
 * @since GemFire 6.5
 */
public interface CqResults<E> extends SelectResults<E> {
}
