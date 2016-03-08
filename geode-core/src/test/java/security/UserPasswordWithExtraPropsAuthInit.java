/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package security;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import templates.security.UserPasswordAuthInit;

import java.util.Iterator;
import java.util.Properties;

/**
 * An {@link AuthInitialize} implementation that obtains the user name and
 * password as the credentials from the given set of properties. If 
 * keep-extra-props property exits, it will copy rest of the
 * properties provided in getCredential props argument will also be 
 * copied as new credentials.
 * 
 * @author Soubhik
 * @since 5.5
 */
public class UserPasswordWithExtraPropsAuthInit extends UserPasswordAuthInit {

  public static final String EXTRA_PROPS = "security-keep-extra-props";

  public static final String SECURITY_PREFIX = "security-";
  
  public static AuthInitialize create() {
    return new UserPasswordWithExtraPropsAuthInit();
  }

  public UserPasswordWithExtraPropsAuthInit() {
    super();
  }

  public Properties getCredentials(Properties props, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {

    Properties newProps = super.getCredentials(props, server, isPeer);
    String extraProps = props.getProperty(EXTRA_PROPS);
    if(extraProps != null) {
    	for(Iterator it = props.keySet().iterator(); it.hasNext();) {
    		String key = (String)it.next();
    		if( key.startsWith(SECURITY_PREFIX) && 
    		    key.equalsIgnoreCase(USER_NAME) == false &&
    		    key.equalsIgnoreCase(PASSWORD) == false &&
    		    key.equalsIgnoreCase(EXTRA_PROPS) == false) {
    			newProps.setProperty(key, props.getProperty(key));
    		}
    	}
    	this.securitylog.fine("got everything and now have: "
          + newProps.keySet().toString());
    }
    return newProps;
  }

}
