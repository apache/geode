/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

/**
 * Common configuration for all entities that can be managed using the
 * GemFire administration API.  Note that once a managed entity has
 * been {@linkplain ManagedEntity#start started}, attempts to modify
 * its configuration will cause an {@link IllegalStateException} to be
 * thrown.
 *
 * @see ManagedEntity
 *
 * @author David Whitlock
 * @since 4.0
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface ManagedEntityConfig extends Cloneable {

  /**
   * Returns the name of the host on which the managed entity runs or
   * will run.
   */
  public String getHost();

  /**
   * Sets the name of the host on which the managed entity will run.
   */
  public void setHost(String host);

  /**
   * Returns the name of the working directory in which the managed
   * entity runs or will run.
   */
  public String getWorkingDirectory();

  /**
   * Sets the name of the working directory in which the managed
   * entity will run.
   */
  public void setWorkingDirectory(String dir);

  /**
   * Returns the name of the GemFire product directory to use when
   * administering the managed entity.
   */
  public String getProductDirectory();

  /**
   * Sets the name of the GemFire product directory to use when
   * administering the managed entity.
   */
  public void setProductDirectory(String dir);

  /**
   * Returns the command prefix used to administer a managed entity
   * that is hosted on a remote machine.  If the remote command is
   * <code>null</code> (the default value), then the remote command
   * associated with the {@linkplain
   * AdminDistributedSystem#getRemoteCommand() distributed system}
   * will be used.
   */
  public String getRemoteCommand();

  /**
   * Sets the command prefix used to administer a managed entity that
   * is hosted on a remote machine.
   */
  public void setRemoteCommand(String remoteCommand);

  /**
   * Validates this configuration.
   *
   * @throws IllegalStateException
   *         If a managed entity cannot be administered using this
   *         configuration 
   */
  public void validate();

  /**
   * Returns a new <code>ManagedEntityConfig</code> with the same
   * configuration as this <code>ManagedEntityConfig</code>.
   */
  public Object clone() throws CloneNotSupportedException; 

}
