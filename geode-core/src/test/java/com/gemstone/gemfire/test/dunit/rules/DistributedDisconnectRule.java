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
package com.gemstone.gemfire.test.dunit.rules;

// TODO: import static com.gemstone.gemfire.test.dunit.DistributedTestRule.*;

import com.gemstone.gemfire.test.dunit.SerializableRunnable;

@SuppressWarnings("serial")
public class DistributedDisconnectRule extends DistributedExternalResource {

  private final boolean disconnectBefore;
  private final boolean disconnectAfter;
  private final boolean disconnectBeforeClass;
  private final boolean disconnectAfterClass;
  
  public static Builder builder() {
    return new Builder();
  }
  
  public DistributedDisconnectRule(final Builder builder) {
    this(new RemoteInvoker(), builder);
  }
   
  public DistributedDisconnectRule(final RemoteInvoker invoker, final Builder builder) {
    super(invoker);
    this.disconnectBeforeClass = builder.disconnectBeforeClass;
    this.disconnectAfterClass = builder.disconnectAfterClass;
    this.disconnectBefore = builder.disconnectBefore;
    this.disconnectAfter = builder.disconnectAfter;
  }

  @Override
  protected void before() throws Throwable {
    if (this.disconnectBefore) {
      invoker().invokeEverywhere(serializableRunnable());
    }
  }

  @Override
  protected void after() throws Throwable {
    if (this.disconnectAfter) {
      invoker().invokeEverywhere(serializableRunnable());
    }
  }

  @Override
  protected void beforeClass() throws Throwable {
    if (this.disconnectBeforeClass) {
      invoker().invokeEverywhere(serializableRunnable());
    }
  }

  @Override
  protected void afterClass() throws Throwable {
    if (this.disconnectAfterClass) {
      invoker().invokeEverywhere(serializableRunnable());
    }
  }

  private static SerializableRunnable serializableRunnable() {
    return new SerializableRunnable() {
      @Override
      public void run() {
        // TODO: disconnectFromDS();
      }
    };
  }
  
  /**
   * Builds an instance of DistributedDisconnectRule
   * 
   * @author Kirk Lund
   */
  public static class Builder {
    private boolean disconnectBeforeClass;
    private boolean disconnectAfterClass;
    private boolean disconnectBefore;
    private boolean disconnectAfter;
    
    public Builder() {}

    public Builder disconnectBeforeClass(final boolean disconnectBeforeClass) {
      this.disconnectBeforeClass = disconnectBeforeClass;
      return this;
    }
    
    public Builder disconnectBefore(final boolean disconnectBefore) {
      this.disconnectBefore = disconnectBefore;
      return this;
    }
    
    public Builder disconnectAfterClass(final boolean disconnectAfterClass) {
      this.disconnectAfterClass = disconnectAfterClass;
      return this;
    }
    
    public Builder disconnectAfter(final boolean disconnectAfter) {
      this.disconnectAfter = disconnectAfter;
      return this;
    }
    
    public DistributedDisconnectRule build() {
      return new DistributedDisconnectRule(this);
    }
  }
}
