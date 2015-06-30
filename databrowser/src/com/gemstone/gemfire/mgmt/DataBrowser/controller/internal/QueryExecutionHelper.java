package com.gemstone.gemfire.mgmt.DataBrowser.controller.internal;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireConnection;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.CQConfiguarationPrms;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.IQueryExecutedEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.IQueryExecutionListener;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.QueryConfigurationPrms;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQQuery;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * 
 * Singleton Helper class to perform query fired by view also process post query
 * data
 * 
 * initialize() method has to be called after each connection to
 * initialize/reset the state of field.
 * 
 * 
 * 
 * @author mjha
 * 
 */
public class QueryExecutionHelper {
  private static QueryExecutionHelper instance;

  private GemFireConnection connection;

  private List<QueryConfigurationPrms> queriesToBeExecutedQueue;

  private List<PostQueryData> postQueryDataQueue;

  private QueryResult lastQueryResult = null;
  
  private Thread queryExecuter;

  private Thread queryResultProcessor;

  private boolean isShutDownStarted;

  private QueryExecutionHelper() {

  }

  public static QueryExecutionHelper getInstance() {
    if (instance == null)
      instance = new QueryExecutionHelper();

    return instance;
  }

  /**
   * Initialize the fields , also starts the thread to execute query and process
   * query result
   */
  public void start(GemFireConnection conn) {
    queriesToBeExecutedQueue = new ArrayList<QueryConfigurationPrms>();
    postQueryDataQueue = new ArrayList<PostQueryData>();
    connection = conn;
    isShutDownStarted = false;
  }

  public void shutDown() {
    this.isShutDownStarted = true;
    if (isShutDownStarted) {
      connection = null;

      if (queryExecuter != null){
        synchronized (queriesToBeExecutedQueue) {
          queriesToBeExecutedQueue.clear();
          queriesToBeExecutedQueue.notify();
        }
      }

      if (queryResultProcessor != null){
        synchronized (postQueryDataQueue) {
          postQueryDataQueue.clear();
          postQueryDataQueue.notify();
        }
      }

      queryResultProcessor = null;
      queryExecuter = null;
      setLastQueryResults(null);
    }
  }

  /**
   * Append query to the queue. This will return with adding query if connection
   * is no more or data browser shut down
   * 
   * @param queryPrms
   *          encapsulating parameters to perform query and process post query
   *          result
   */
  public void submitQuery(QueryConfigurationPrms queryPrms) {
    if (connection == null || isShutDownStarted)
      return;
    synchronized (queriesToBeExecutedQueue) {
      queriesToBeExecutedQueue.add(queryPrms);
      if (queryExecuter == null) {
        queryExecuter = new Thread(new QueryExecutor());
        queryExecuter.setName("Query Executer");
        queryExecuter.setDaemon(true);
        queryExecuter.start();
      }

      queriesToBeExecutedQueue.notify();
    }
  }
  

  private void processResult(PostQueryData queryData) {
    if (connection == null || isShutDownStarted)
      return;
    synchronized (postQueryDataQueue) {
      postQueryDataQueue.add(queryData);
      if (queryResultProcessor == null) {
        queryResultProcessor = new Thread(new QueryResultProcessor());
        queryResultProcessor.setName("Query Result Processor");
        queryResultProcessor.setDaemon(true);
        queryResultProcessor.start();
      }

      postQueryDataQueue.notify();
    }
  }

  /**
   * Runnable to execute query on DS
   * 
   * The runnable keeps on running as long as connection is establish with DS or
   * Data Browser is running
   * 
   * @author mjha
   */
  private class QueryExecutor implements Runnable {
    public void run() {
      while (true) {
        QueryConfigurationPrms prms = null;
        synchronized (queriesToBeExecutedQueue) {
          while (queriesToBeExecutedQueue.isEmpty()) {
            if(connection != null && !isShutDownStarted){
              try {
                queriesToBeExecutedQueue.wait();
              }
              catch (InterruptedException e) {
                 continue;
              }
            }else{
              return;
            }
          }
          prms = queriesToBeExecutedQueue.remove(0);
        }
        String queryString = prms.getQueryString();
        GemFireMember member = prms.getMember();
        boolean isCQ = prms.isCQ();
        Object result = null;
        CQQuery cQuery = null;
        if (connection != null && !isShutDownStarted) {
          try {
            if(!isCQ){
              QueryResult executeQuery = connection.executeQuery(queryString,
                  member);

              result = executeQuery;
            }else{
              CQConfiguarationPrms cqPrms= (CQConfiguarationPrms)prms;
              cQuery = cqPrms.getCQuery();
              cQuery.execute();
            }
          }
          catch (Throwable e) {
            result = e;
            LogUtil.error("Error occured while executing the query: " + prms, e);
          }
          finally {
            PostQueryData queryData = new PostQueryData(prms, result);
            processResult(queryData);
          }
        }
        else {
          return;
        }
      }
    }
  }

 public synchronized QueryResult getLastQueryResults(){
   return lastQueryResult;
 }
 
 public synchronized void setLastQueryResults(QueryResult result) {
   LogUtil.fine("QueryHelper.setLastQueryResults =>"+result);
   lastQueryResult = result;
 }
  
  /**
   * Runnable to process post query results, if query successful else it will
   * process error occurred while querying
   * 
   * The runnable keeps on running as long as connection is establish with DS or
   * Data Browser is running
   * 
   * @author mjha
   */
  private class QueryResultProcessor implements Runnable {
    public void run() {
      while (true) {
        PostQueryData data = null;
        synchronized (postQueryDataQueue) {
          while (postQueryDataQueue.isEmpty()) {
            if(connection != null && !isShutDownStarted){
              try {
                postQueryDataQueue.wait();
              }
              catch (InterruptedException e) {
                 continue;
              }
            }else{
              return;
            }
          }
          data = postQueryDataQueue.remove(0);
        }
        QueryConfigurationPrms prms = data.getQueryPrms();
        boolean isCQ = prms.isCQ();
        IQueryExecutionListener listener = prms.getQueryExecutionListener();
        String queryString = prms.getQueryString();
        GemFireMember member = prms.getMember();
        Object object = data.getQueryResult();
        if (connection != null && !isShutDownStarted) {
          if (object instanceof Throwable) {
            IQueryExecutedEvent event = new QueryExecutedEvent(queryString,
                null, member);
            listener.queryFailed(event, (Throwable)object);
          }
          else {
            if(!isCQ){
              if (object instanceof QueryResult) {
                IQueryExecutedEvent event = new QueryExecutedEvent(queryString,
                    (QueryResult)object, member);
                setLastQueryResults((QueryResult)object);
                listener.queryExecuted(event);
              }
            }else{
              IQueryExecutedEvent event = new QueryExecutedEvent(queryString, null, member);
              listener.queryExecuted(event);
            }
          }
        }
        else {
          return;
        }
      }
    }
  }

  /**
   * represents post query data along with query information
   * 
   * Post query data can be ResultSet or error occurred
   * 
   * @author mjha
   */
  private static class PostQueryData {
    private QueryConfigurationPrms queryPrms;

    private Object queryResult;

    PostQueryData(QueryConfigurationPrms prms, Object resQuery) {
      this.queryPrms = prms;
      this.queryResult = resQuery;
    }

    public QueryConfigurationPrms getQueryPrms() {
      return queryPrms;
    }

    public Object getQueryResult() {
      return queryResult;
    }

  }
 
}
