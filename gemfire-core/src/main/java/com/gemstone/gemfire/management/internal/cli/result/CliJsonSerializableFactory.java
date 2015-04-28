/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.result;

import com.gemstone.gemfire.management.internal.cli.commands.dto.RegionAttributesInfo;
import com.gemstone.gemfire.management.internal.cli.commands.dto.RegionDetails;
import com.gemstone.gemfire.management.internal.cli.commands.dto.RegionMemberDetails;

public class CliJsonSerializableFactory implements CliJsonSerializableIds {
  
  public static CliJsonSerializable getCliJsonSerializable(int id) {
    CliJsonSerializable cliJsonSerializable = null;
    
    switch (id) {      
    case CLI_DOMAIN_OBJECT__REGION_DETAILS:
      cliJsonSerializable = new RegionDetails();
      break;
      
    case CLI_DOMAIN_OBJECT__REGION_ATTR_INFO:
      cliJsonSerializable = new RegionAttributesInfo();
      break;
      
    case CLI_DOMAIN_OBJECT__REGION_MEMBER_DETAILS:
      cliJsonSerializable = new RegionMemberDetails();
      break;      

    default:
      throw new IllegalArgumentException("Could not find type with given identifer.");
    }
    
    return cliJsonSerializable;
  }

}
