package io.atomix.copycat.db;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.db.IKeyValueDB;
import io.atomix.copycat.server.db.RocksdbKeyValueDB;
import io.atomix.copycat.server.machine.DBStateMachine;
import io.atomix.copycat.server.machine.Delete;
import io.atomix.copycat.server.machine.Get;
import io.atomix.copycat.server.machine.Put;
import io.atomix.copycat.server.storage.Storage;
import org.rocksdb.RocksDBException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class TestServer {
    public static void main(String[] argse) throws Exception {
        // Parse the address to which to bind the server.
        String[] args = new String[]{"/Users/momo/data/copycat","127.0.0.1:5002"};
        String[] mainParts = args[1].split(":");

        Address address = new Address(mainParts[0], Integer.valueOf(mainParts[1]));

        // Build a list of all member addresses to which to connect.
        List<Address> members = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            String[] parts = args[i].split(":");
            members.add(new Address(parts[0], Integer.valueOf(parts[1])));
        }

        String path = "/tmp/rocksdb";
        IKeyValueDB db = new RocksdbKeyValueDB(path);
        CopycatServer server = CopycatServer.builder(address)
                .withStateMachine(() -> {
                    try {
                        return new DBStateMachine(db);
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                    }
                    return null;
                })
                .withTransport(new NettyTransport())
                .withStorage(Storage.builder()
                        .withDirectory(args[0])
                        .withMaxSegmentSize(1024 * 1024 * 32)
                        .withMinorCompactionInterval(Duration.ofMinutes(1))
                        .withMajorCompactionInterval(Duration.ofMinutes(15))
                        .build())
                .build();

        server.serializer().register(Put.class, 1);
        server.serializer().register(Get.class, 2);
        server.serializer().register(Delete.class, 3);

        server.bootstrap(members).join();

        while (server.isRunning()) {
            Thread.sleep(1000);
        }
    }
}
