package io.atomix.copycat.client;

import io.atomix.copycat.Operation;

/**
 * get balance key from {@link Operation}
 * @param <V>
 * @param <T>
 */
public interface BalanceKeyGetter<V,T extends Operation> {
    /**
     *
     * @param t operation to balance
     * @return key to balance
     */
    V get(T t);
}
