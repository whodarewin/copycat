package io.atomix.copycat.server.db;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;

/**
 * kvdb based on rocksdb
 */
public class RocksdbKeyValueDB implements IKeyValueDB {
    private RocksDB db;

    static{
        RocksDB.loadLibrary();
    }

    public RocksdbKeyValueDB(String path) throws RocksDBException {
        File file = new File(path);
        if(!file.exists()){
            file.mkdir();
        }
        db = RocksDB.open(path);
    }

    public RocksdbKeyValueDB(RocksDB db){
        this.db = db;
    }

    @Override
    public byte[] get(byte[] key) throws RocksDBException {
        return db.get(key);
    }

    @Override
    public boolean put(byte[] key, byte[] value) throws RocksDBException {
        db.put(key,value);
        return true;
    }

    @Override
    public void delete(byte[] key) throws RocksDBException {
        db.delete(key);
    }
}
