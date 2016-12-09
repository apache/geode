package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.security.AuthenticationRequiredException;

public class MessageIdExtractor {
  public long getUniqueIdFromMessage(Message requestMessage, HandShake handshake, long connectionId)
      throws AuthenticationRequiredException {
    AuthIds aIds = getAuthIdsFromMessage(requestMessage, handshake);
    if (connectionId != aIds.getConnectionId()) {
      throw new AuthenticationRequiredException(
          LocalizedStrings.HandShake_NO_SECURITY_CREDENTIALS_ARE_PROVIDED.toLocalizedString());
    }
    return aIds.getUniqueId();
  }

  private AuthIds getAuthIdsFromMessage(Message requestMessage, HandShake handshake)
      throws AuthenticationRequiredException {
    try {
      byte[] secureBytes = requestMessage.getSecureBytes();
      secureBytes = handshake.decryptBytes(secureBytes);
      return new AuthIds(secureBytes);
    } catch (Exception ex) {
      throw new AuthenticationRequiredException(
          LocalizedStrings.HandShake_NO_SECURITY_CREDENTIALS_ARE_PROVIDED.toLocalizedString(), ex);
    }
  }
}
