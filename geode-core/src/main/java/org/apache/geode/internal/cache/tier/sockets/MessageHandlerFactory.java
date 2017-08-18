package org.apache.geode.internal.cache.tier.sockets;

import java.util.Iterator;
import java.util.ServiceLoader;

public class MessageHandlerFactory {
  public ClientProtocolMessageHandler makeMessageHandler() {
    ServiceLoader<ClientProtocolMessageHandler> loader =
        ServiceLoader.load(ClientProtocolMessageHandler.class);
    Iterator<ClientProtocolMessageHandler> iterator = loader.iterator();

    if (!iterator.hasNext()) {
      throw new ServiceLoadingFailureException(
          "There is no ClientProtocolMessageHandler implementation found in JVM");
    }

    return iterator.next();
  }
}
