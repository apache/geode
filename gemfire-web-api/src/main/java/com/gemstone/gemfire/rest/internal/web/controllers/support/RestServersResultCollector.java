package com.gemstone.gemfire.rest.internal.web.controllers.support;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.springframework.util.StringUtils;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;

public class RestServersResultCollector<String, Object> implements ResultCollector<String, Object> {

  private ArrayList resultList = new ArrayList();
  
  public void addResult(DistributedMember memberID,
      String result) {
    if(!StringUtils.isEmpty(result)){
      this.resultList.add(result);
    }
  }
  
  public void endResults() {
  }

  public Object getResult() throws FunctionException {
    return (Object)resultList;
  }

  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException {
    return (Object)resultList;
  }
  
  public void clearResults() {
    resultList.clear();
  }

}
