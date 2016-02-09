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
package com.gemstone.gemfire.test.dunit;

import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * <code>IgnoredException</code> provides static utility methods that
 * will log messages to add or remove <code>IgnoredException</code>s.
 * Each <code>IgnoredException</code> allows you to specify a suspect string
 * that will be ignored by the <code>GrepLogs</code> utility which is run 
 * after each <code>DistributedTest</code> test method.
 * 
 * These methods can be used directly: 
 * <code>IgnoredException.addIgnoredException(...)</code>, 
 * however, they are intended to be referenced through static import:
 *
 * <pre>
 * import static com.gemstone.gemfire.test.dunit.IgnoredException.*;
 *    ...
 *    addIgnoredException(...);
 * </pre>
 *
 * A test should use <code>addIgnoredException(...)</code> before executing
 * the code that will potentially log the suspect string. The test should
 * then <code>remove()</code> the <code>IgnoredException</code> immediately
 * after. Note that <code>DistributedTestCase.tearDown()</code> will 
 * automatically remove all current <code>IgnoredException</code>s by
 * invoking <code>removeAllIgnoredExceptions</code>.
 *  
 * A suspect string is typically an Exception class and/or message string.
 *
 * The <code>GrepLogs</code> utility is part of Hydra which is not included
 * in Apache Geode. The Hydra class which consumes logs and reports suspect
 * strings is <code>batterytest.greplogs.GrepLogs</code>.
 * 
 * Extracted from DistributedTestCase.
 * 
 * @author Mitch Thomas
 * @since 5.7bugfix
 */
@SuppressWarnings("serial")
public class IgnoredException implements Serializable {
  
  private static final Logger logger = LogService.getLogger();

  private final String suspectString;

  private final transient VM vm;
  
  private static ConcurrentLinkedQueue<IgnoredException> ignoredExceptions = new ConcurrentLinkedQueue<IgnoredException>();

  public IgnoredException(final String suspectString) {
    this.suspectString = suspectString;
    this.vm = null;
  }

  IgnoredException(final String suspectString, final VM vm) {
    this.suspectString = suspectString;
    this.vm = vm;
  }

  String suspectString() {
    return this.suspectString;
  }
  
  VM vm() {
    return this.vm;
  }
  
  public String getRemoveMessage() {
    return "<ExpectedException action=remove>" + this.suspectString + "</ExpectedException>";
  }

  public String getAddMessage() {
    return "<ExpectedException action=add>" + this.suspectString + "</ExpectedException>";
  }

  public void remove() {
    final String removeMessage = getRemoveMessage();
    
    @SuppressWarnings("serial")
    SerializableRunnable removeRunnable = new SerializableRunnable(IgnoredException.class.getSimpleName()+" remove") {
      public void run() {
        // TODO: delete use of system.getLogWriter
        DistributedSystem system = InternalDistributedSystem.getConnectedInstance();
        if (system != null) {
          system.getLogWriter().info(removeMessage);
        }
        
        // TODO: delete use of LogWriterUtils
        try {
          LogWriterUtils.getLogWriter().info(removeMessage);
        } catch (Exception noHydraLogger) {
        }

        logger.info(removeMessage);
      }
    };

    removeRunnable.run();
    
    if (this.vm != null) {
      vm.invoke(removeRunnable);
    } else {
      Invoke.invokeInEveryVM(removeRunnable);
    }
  }

  public static void removeAllExpectedExceptions() {
    IgnoredException ignoredException;
    while ((ignoredException = ignoredExceptions.poll()) != null) {
      ignoredException.remove();
    }
  }

  /**
   * Log in all VMs, in both the test logger and the GemFire logger the
   * ignored exception string to prevent grep logs from complaining. The
   * suspect string is used by the GrepLogs utility and so can contain
   * regular expression characters.
   * 
   * @since 5.7bugfix
   * @param suspectString the exception string to expect
   * @param vm the VM on which to log the expected exception or null for all VMs
   * @return an IgnoredException instance for removal purposes
   */
  public static IgnoredException addIgnoredException(final String suspectString, final VM vm) {
    final IgnoredException ignoredException = new IgnoredException(suspectString, vm);
    final String addMessage = ignoredException.getAddMessage();
    
    @SuppressWarnings("serial")
    SerializableRunnable addRunnable = new SerializableRunnable(IgnoredException.class.getSimpleName()+" addIgnoredException") {
      public void run() {
        // TODO: delete use of system.getLogWriter
        DistributedSystem system = InternalDistributedSystem.getConnectedInstance();
        if (system != null) {
          system.getLogWriter().info(addMessage);
        }
        
        // TODO: delete use of LogWriterUtils
        try {
          LogWriterUtils.getLogWriter().info(addMessage);
        } catch (Exception noHydraLogger) {
        }
  
        logger.info(addMessage);
      }
    };
    
    addRunnable.run();
    
    if (vm != null) {
      vm.invoke(addRunnable);
    } else {
      Invoke.invokeInEveryVM(addRunnable);
    }
    
    ignoredExceptions.add(ignoredException);
    return ignoredException;
  }

  /**
   * Log in all VMs, in both the test logger and the GemFire logger the
   * ignored exception string to prevent grep logs from complaining. The
   * suspect string is used by the GrepLogs utility and so can contain
   * regular expression characters.
   * 
   * If you do not remove the ignored exception, it will be removed at the
   * end of your test case automatically.
   * 
   * @since 5.7bugfix
   * @param suspectString the exception string to expect
   * @return an IgnoredException instance for removal
   */
  public static IgnoredException addIgnoredException(final String suspectString) {
    return addIgnoredException(suspectString, null);
  }
}