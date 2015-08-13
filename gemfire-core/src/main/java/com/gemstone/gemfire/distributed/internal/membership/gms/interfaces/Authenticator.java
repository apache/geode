package com.gemstone.gemfire.distributed.internal.membership.gms.interfaces;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.security.AuthenticationFailedException;

public interface Authenticator extends Service {

  String authenticate(InternalDistributedMember m, Object credentials) throws AuthenticationFailedException;

  Object getCredentials(InternalDistributedMember m);
}
