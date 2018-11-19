package io.atomix.copycat.server.db;

import org.rocksdb.RocksDBException;

/**
 * kv db interface
 */
public interface IKeyValueDB {
    /**
     * get
     * @param key key to get
     * @return value
     */
    byte[] get(byte[] key) throws RocksDBException;

    /**
     * put
     * @param key key
     * @param value value
     * @return
     */
    boolean put(byte[] key,byte[] value) throws RocksDBException;

    /**
     * delete
     * @param key key to delete
     * @return
     */
    void delete(byte[] key) throws RocksDBException;
}
