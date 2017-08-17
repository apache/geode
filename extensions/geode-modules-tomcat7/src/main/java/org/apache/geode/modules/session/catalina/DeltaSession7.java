/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.modules.session.catalina;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Manager;
import org.apache.catalina.realm.GenericPrincipal;
import org.apache.catalina.security.SecurityUtil;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.lru.Sizeable;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.modules.gatewaydelta.GatewayDelta;
import org.apache.geode.modules.gatewaydelta.GatewayDeltaEvent;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionAttributeEvent;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionAttributeEventBatch;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionDestroyAttributeEvent;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionUpdateAttributeEvent;

@SuppressWarnings("serial")
public class DeltaSession7 extends DeltaSession {


  /**
   * The string manager for this package.
   */
  // protected static StringManager STRING_MANAGER =
  // StringManager.getManager("org.apache.geode.modules.session.catalina");

  /**
   * Construct a new <code>Session</code> associated with no <code>Manager</code>. The
   * <code>Manager</code> will be assigned later using {@link #setOwner(Object)}.
   */
  public DeltaSession7() {
    super();
  }

  /**
   * Construct a new Session associated with the specified Manager.
   *
   * @param manager The manager with which this Session is associated
   */
  public DeltaSession7(Manager manager) {
    super(manager);
  }

}
