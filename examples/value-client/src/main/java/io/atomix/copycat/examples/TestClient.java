package io.atomix.copycat.examples;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.RecoveryStrategies;
import io.atomix.copycat.client.ServerSelectionStrategies;
import io.atomix.copycat.server.machine.Delete;
import io.atomix.copycat.server.machine.Get;
import io.atomix.copycat.server.machine.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class TestClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestClient.class);
    public static void main(String[] argse) throws ExecutionException, InterruptedException {
        //client ip
        String[] args = new String[]{"127.0.0.1:5002"};


        // Build a list of all member addresses to which to connect. server ip
        List<Address> members = new ArrayList<>();
        for (String arg : args) {
            String[] parts = arg.split(":");
            members.add(new Address(parts[0], Integer.valueOf(parts[1])));
        }

        CopycatClient client = CopycatClient.builder()
                //witch transporter to use
                .withTransport(new NettyTransport())
                .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
                .withRecoveryStrategy(RecoveryStrategies.RECOVER)
                .withServerSelectionStrategy(ServerSelectionStrategies.LEADER)
                .withSessionTimeout(Duration.ofSeconds(15))
                .build();

        client.serializer().register(Put.class, 1);
        client.serializer().register(Get.class, 2);
        client.serializer().register(Delete.class, 3);

        client.connect(members).join();

        for(int i = 0;i < 10000;i++){
            Put put = new Put(String.valueOf(i).getBytes(),String.valueOf(i).getBytes());
            LOGGER.info("submit {}",i);
            client.submit(put).get();
        }


    }
}
