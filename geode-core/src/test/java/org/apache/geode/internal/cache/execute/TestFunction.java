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
package org.apache.geode.internal.cache.execute;

import java.io.Serializable;
import java.util.Properties;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

public class TestFunction extends FunctionAdapter {

  public static final String TEST_FUNCTION1 = "TestFunction1";

  public static final String TEST_FUNCTION2 = "TestFunction2";

  public static final String TEST_FUNCTION3 = "TestFunction3";

  public static final String TEST_FUNCTION4 = "TestFunction4";

  public static final String TEST_FUNCTION5 = "TestFunction5";

  private final Properties props;

  private static final String ID = "id";

  private static final String HAVE_RESULTS = "haveResults";

  public TestFunction() {
    super();
    this.props = new Properties();
  }

  public Properties getProps() {
    return props;
  }

  public TestFunction(boolean haveResults, String id) {
    this.props = new Properties();
    this.props.setProperty(HAVE_RESULTS, Boolean.toString(haveResults));
    this.props.setProperty(ID, id);
  }

  public void execute(FunctionContext context) {
    String id = this.props.getProperty(ID);

    if (id.equals(TEST_FUNCTION1)) {
      execute1(context);
    }
    else if (id.equals(TEST_FUNCTION2)) {
      execute2(context);
    }
    else if (id.equals(TEST_FUNCTION3)) {
      execute2(context);
    }
    else if (id.equals(TEST_FUNCTION4)) {
      execute2(context);
    }
    else if (id.equals(TEST_FUNCTION5)) {
      execute5(context);
    }
  }

  public void execute1(FunctionContext context) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    logger.info("Executing executeException in TestFunction on Member : "
        + ds.getDistributedMember() + "with Context : " + context);
    context.getResultSender().lastResult((Serializable) context.getArguments());
  }

  public void execute2(FunctionContext context) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    try {
      synchronized (this) {
        this.wait(20000000);
      }
    }
    catch (InterruptedException e) {

    }
    context.getResultSender().lastResult(Boolean.TRUE);
  }

  public void execute5(FunctionContext context) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();

    if (this.props.get("TERMINATE") != null
        && this.props.get("TERMINATE").equals("YES")) {
      logger.info("Function Terminated");
    }
    else {
      try {
        synchronized (this) {
          logger.info("Function Running");
          this.wait(20000);
        }
      }
      catch (InterruptedException e) {

      }
    }
    context.getResultSender().lastResult(Boolean.TRUE);
  }

  public String getId() {
    return this.props.getProperty(ID);
  }

  public boolean hasResult() {
    return Boolean.valueOf(this.props.getProperty(HAVE_RESULTS)).booleanValue();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.xmlcache.Declarable2#getConfig()
   */
  public Properties getConfig() {
    return this.props;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.cache.Declarable#init(java.util.Properties)
   */
  public void init(Properties props) {
    this.props.putAll(props);
  }
}
