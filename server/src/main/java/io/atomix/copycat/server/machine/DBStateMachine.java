/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.copycat.server.machine;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.db.IKeyValueDB;
import io.atomix.copycat.server.db.RocksdbKeyValueDB;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * db
 */
public class DBStateMachine extends StateMachine {
  private static final Logger LOGGER = LoggerFactory.getLogger(DBStateMachine.class);
  private IKeyValueDB db;
  static{
    RocksDB.loadLibrary();
  }

  public DBStateMachine(IKeyValueDB db) throws RocksDBException {
    this.db = db;
  }

  @Override
  protected void configure(StateMachineExecutor executor) {
    executor.register(Put.class, this::set);
    executor.register(Get.class, this::get);
    executor.register(Delete.class, this::delete);
  }

  /**
   * todo:send back the exception to client
   * Sets the value.
   */
  private Boolean set(Commit<Put> commit) {
    try{

      Put put = commit.command();
      byte[] key = put.getKey();
      byte[] value = put.getValue();
      LOGGER.info("recive commit {}", new String(key));
      this.db.put(key,value);
      return true;
    } catch (RocksDBException e) {
      e.printStackTrace();
    } finally {
      commit.close();
    }
    return false;
  }

  /**
   * todo:send back the exception to client
   * Gets the value.
   */
  private byte[] get(Commit<Get> commit) {
    try{
      Get get = commit.command();
      byte[] key = get.getKey();
      return this.db.get(key);
    } catch (RocksDBException e) {
      e.printStackTrace();
      return null;
    }finally {
      commit.close();
    }
  }

  /**
   * todo:send back the exception to client
   * Deletes the value.
   */
  private void delete(Commit<Delete> commit) {
    try{
      Delete delete = commit.command();
      byte[] key = delete.getKey();
      this.db.delete(key);
    } catch (RocksDBException e) {
      e.printStackTrace();
    }finally{
      commit.close();
    }
  }

}
