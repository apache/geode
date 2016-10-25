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
package org.apache.geode.spark.connector.internal.geodefunctions;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.*;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Query;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;
import java.util.Iterator;

public class QueryFunction implements Function {

  private static final long serialVersionUID = 4866641340803692882L;

  public final static String ID = "geode-spark-query-function";

  private final static QueryFunction instance = new QueryFunction();

  private static final Logger logger = LogService.getLogger();

  private static final int CHUNK_SIZE = 1024;

  @Override
  public String getId() {
    return ID;
  }

  public static QueryFunction getInstance() {
    return instance;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public boolean isHA() {
    return true;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public void execute(FunctionContext context) {
    try {
      String[] args = (String[]) context.getArguments();
      String queryString = args[0];
      String bucketSet = args[1];
      InternalRegionFunctionContext irfc = (InternalRegionFunctionContext) context;
      LocalRegion localRegion = (LocalRegion) irfc.getDataSet();
      boolean partitioned = localRegion.getDataPolicy().withPartitioning();
      Query query = CacheFactory.getAnyInstance().getQueryService().newQuery(queryString);
      Object result = partitioned ? query.execute((InternalRegionFunctionContext) context) : query.execute();
      ResultSender<Object> sender = context.getResultSender();
      HeapDataOutputStream buf = new HeapDataOutputStream(CHUNK_SIZE, null);
      Iterator<Object> iter = ((SelectResults) result).asList().iterator();
      while (iter.hasNext()) {
        Object row = iter.next();
        DataSerializer.writeObject(row, buf);
        if (buf.size() > CHUNK_SIZE) {
          sender.sendResult(buf.toByteArray());
          logger.debug("OQL query=" + queryString + " bucket set=" + bucketSet + " sendResult(), data size=" + buf.size());
          buf.reset();
        }
      }
      sender.lastResult(buf.toByteArray());
      logger.debug("OQL query=" + queryString + " bucket set=" + bucketSet + " lastResult(), data size=" + buf.size());
      buf.reset();
    }
    catch(Exception e) {
      throw new FunctionException(e);
    }
  }

}
