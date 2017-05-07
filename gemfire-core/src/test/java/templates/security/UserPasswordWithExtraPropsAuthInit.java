/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package templates.security;

import java.util.Properties;
import java.util.Iterator;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.AuthenticationFailedException;

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
