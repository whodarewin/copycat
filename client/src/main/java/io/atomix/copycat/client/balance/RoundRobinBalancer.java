package io.atomix.copycat.client.balance;

import io.atomix.catalyst.transport.Connection;

import java.util.Collection;

public class RoundRobinBalancer<T> implements LoadBalancer<T> {
    @Override
    public Connection balance(Collection<Connection> connections, T t) {
        return null;
    }
}
