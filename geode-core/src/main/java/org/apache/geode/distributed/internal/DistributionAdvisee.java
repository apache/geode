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
package org.apache.geode.distributed.internal;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;

/**
 * Resource which uses a {@link DistributionAdvisor}.
 * @since GemFire 5.7
 */
public interface DistributionAdvisee {

  /**
   * Returns the underlying <code>DistributionManager</code> used for messaging.
   * @return the underlying <code>DistributionManager</code>
   */
  public DM getDistributionManager();
  
  /**
   * @return the cancellation object for the advisee
   */
  public CancelCriterion getCancelCriterion();
  
  /**
   * Returns the <code>DistributionAdvisor</code> that provides advice for
   * this advisee.
   * @return the <code>DistributionAdvisor</code>
   */
  public DistributionAdvisor getDistributionAdvisor();
  
  /**
   * Returns the <code>Profile</code> representing this member in the
   * <code>DistributionAdvisor</code>.
   * @return the <code>Profile</code> representing this member
   */
  public Profile getProfile();
  
  /**
   * Returns this advisees parent or null if no parent exists.
   * @return parent advisee
   */
  public DistributionAdvisee getParentAdvisee();
  
  /**
   * Returns the underlying <code>InternalDistributedSystem</code> connection.
   * @return the underlying <code>InternalDistributedSystem</code>
   */
  public InternalDistributedSystem getSystem();
  
  /**
   * Returns the simple name of this resource.
   * @return the simple name of this resource
   */
  public String getName();
  
  /**
   * Returns the full path of this resource.
   * @return the full path of this resource.
   */
  public String getFullPath();

  /** 
   * Fill in the <code>Profile</code> with details from the advisee.
   * @param profile the <code>Profile</code> to fill in with details
   */
  public void fillInProfile(Profile profile);

  /**
   * @return the serial number which identifies the static order in which this
   * region was created in relation to other regions or other instances of
   * this region during the life of this JVM.
   */
  public int getSerialNumber();

}
