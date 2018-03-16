import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;


import java.util.Arrays;



public class StreamingHub {

    public static void main(String[] args) {

        PNConfiguration pnConfiguration = new PNConfiguration();
       //stock
       // pnConfiguration.setSubscribeKey("sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe");
       //weather
        pnConfiguration.setSubscribeKey("sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe");
        pnConfiguration.setSecure(false);
        PubNub pubnub = new PubNub(pnConfiguration);

        final String namespaceName = "spearfishstream";
        final String eventHubName = "stockstream";
        final String sasKeyName = "RootManageSharedAccessKey";
        final String sasKey = "sdfaasdf";
        final ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);


        System.out.println("conn str" + connStr.toString());



       pubnub.addListener(new SubscribeCallback() {
            @Override
            public void status(PubNub pubnub, PNStatus status) {
                System.out.println( status.getOrigin());
            }

            @Override
            public void message(PubNub pubnub, PNMessageResult message) {
                System.out.println(message.getMessage());
                EventHubClient ehClient = null;
                try {

                    ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    EventData sendEvent = new EventData(message.getMessage().toString().getBytes());
                    ehClient.sendSync(sendEvent);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                }
            }

            @Override
            public void presence(PubNub pubnub, PNPresenceEventResult presence) {
            }
        });

        pubnub.subscribe().channels(Arrays.asList("pubnub-market-orders")).execute();
    }
}
