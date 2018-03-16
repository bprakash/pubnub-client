import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;


public class StreamingClient {

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: StreamingClient broker_urls topic");
            System.exit(1);
        }

        final String topic = args[1];
        PNConfiguration pnConfiguration = new PNConfiguration();
       //weather
        pnConfiguration.setSubscribeKey("sub-c-b1cadece-f0fa-11e3-928e-02ee2ddab7fe");
        pnConfiguration.setUuid("my-blah");
        pnConfiguration.setOrigin("pubsub.pubnub.com");
        PubNub pubnub = new PubNub(pnConfiguration);


        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        final Producer producer = new KafkaProducer(configProperties);

       pubnub.addListener(new SubscribeCallback() {
            @Override
            public void status(PubNub pubnub, PNStatus status) {
                System.out.println( status.getOrigin());
                System.out.println(status.toString());
            }

            @Override
            public void message(PubNub pubnub, PNMessageResult message) {
                message.getMessage().getAsJsonObject().addProperty("created_time", message.getTimetoken()/1000000);
                System.out.println("hello : " + message.getMessage());
                ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, message.getMessage().getAsJsonObject().get("location").getAsString(), message.getMessage().toString());
                producer.send(rec);
            }

            @Override
            public void presence(PubNub pubnub, PNPresenceEventResult presence) {
            }
        });

        pubnub.subscribe().channels(Arrays.asList("pubnub-weather")).execute();
    }
}
