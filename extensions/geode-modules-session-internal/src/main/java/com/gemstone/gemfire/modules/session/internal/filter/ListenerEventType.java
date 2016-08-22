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

package com.gemstone.gemfire.modules.session.internal.filter;

/**
 * Enumeration of all possible event types which can be listened for.
 */
public enum ListenerEventType {

  /**
   * HttpSessionAttributeListener
   */
  SESSION_ATTRIBUTE_ADDED,
  SESSION_ATTRIBUTE_REMOVED,
  SESSION_ATTRIBUTE_REPLACED,

  /**
   * HttpSessionBindingListener
   */
  SESSION_VALUE_BOUND,
  SESSION_VALUE_UNBOUND,

  /**
   * HttpSessionListener
   */
  SESSION_CREATED,
  SESSION_DESTROYED,

  /**
   * HttpSessionActivationListener
   */
  SESSION_WILL_ACTIVATE,
  SESSION_DID_PASSIVATE,

  /**
   * ServletContextListener
   */
  SERVLET_CONTEXT_INITIALIZED,
  SERVLET_CONTEXT_DESTROYED,

  /**
   * ServletContextAttributeListener
   */
  SERVLET_CONTEXT_ATTRIBUTE_ADDED,
  SERVLET_CONTEXT_ATTRIBUTE_REMOVED,
  SERVLET_CONTEXT_ATTRIBUTE_REPLACED,

  /**
   * ServletRequestListener
   */
  SERVLET_REQUEST_DESTROYED,
  SERVLET_REQUEST_INITIALIZED,

  /**
   * ServletRequestAttributeListener
   */
  SERVLET_REQUEST_ATTRIBUTE_ADDED,
  SERVLET_REQUEST_ATTRIBUTE_REMOVED,
  SERVLET_REQUEST_ATTRIBUTE_REPLACED;
}
