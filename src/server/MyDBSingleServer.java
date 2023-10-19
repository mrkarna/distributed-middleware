package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.*;

import org.json.JSONException;
import org.json.JSONObject;


public class MyDBSingleServer extends SingleServer {

    private Cluster cluster;
    private Session session;

    public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
                            String keyspace) throws IOException {
        super(isa, isaDB, keyspace);
        // Initialized the connection with Cassandra Server at keyspace "demo" in the constructor. 
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("demo");
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        try {
            log.log(Level.INFO, "{0} received message from {1}",
                    new Object[] { this.clientMessenger.getListeningSocketAddress(), header.sndr });

            String ClientMsg = new String(bytes, SingleServer.DEFAULT_ENCODING);
            
            // If valid JSON request, means CallbackSend was called. 
            // Query is executed and the response is sent back with the corresponding request code (counter)
            if (checkIfValidJson(ClientMsg)) {
                JSONObject json = new JSONObject(ClientMsg);

                JSONObject jsonObject = new JSONObject();
                ResultSet result;
                result = session.execute(json.getString("request"));
                jsonObject.put("response", result.toString());
                jsonObject.put("counter", json.getString("counter"));

                this.clientMessenger.send(header.sndr, jsonObject.toString().getBytes(DEFAULT_ENCODING));
                return;
            }
            // Client.send() was called with just the query. 
            // So query is directly executed and response sent back without any request code. 
            else {
            ResultSet result;
            result = session.execute(ClientMsg);

            this.clientMessenger.send(header.sndr, result.toString().getBytes(DEFAULT_ENCODING));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        super.close(); 
        cluster.close(); // cluster is closed
    }

    public boolean checkIfValidJson(String text) {
        try {
            new JSONObject(text);
        } catch (Exception e) {
            return false;
        }
        return true;
    }
}