package io.atomix.copycat.client;

import io.atomix.catalyst.transport.Connection;
import io.atomix.copycat.client.balance.LoadBalancer;
import io.atomix.copycat.client.util.AddressSelector;

import java.util.Collection;

/**
 * client connection manager
 */
public class ClientConnectionManager {
    private Collection<Connection> address2Connection;
    private AddressSelector selector;
    private LoadBalancer<String> loadBalancer;

    public Connection get(String key){
        return loadBalancer.balance(address2Connection,key);
    }
}
