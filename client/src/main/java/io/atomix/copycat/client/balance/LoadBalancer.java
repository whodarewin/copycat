package io.atomix.copycat.client.balance;

import io.atomix.catalyst.transport.Connection;

import java.util.Collection;

public interface LoadBalancer<T> {
    Connection balance(Collection<Connection> connections,T t);
}
