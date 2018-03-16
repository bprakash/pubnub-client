import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.Map;
import java.util.ArrayList;
import java.util.LinkedHashMap;


public class CSVClient {

    public static void main(String[] args) throws Exception {

       if (args.length < 2) {
            System.err.println("Usage: CSVClient broker_urls topic csvfile");
            System.exit(1);
        }

       final String topic = args[1];
        final String csvFile = args[2];

        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        final Producer producer = new KafkaProducer(configProperties);


        InputStream in = new FileInputStream(csvFile);
        ObjectMapper mapper = new ObjectMapper();
        CSV csv = new CSV(true, ',', in);
        List<String> fieldNames = null;
        if (csv.hasNext()) fieldNames = new ArrayList<String>(csv.next());
        while (csv.hasNext()) {
            List<String> x = csv.next();
            Map<String, String> obj = new LinkedHashMap<String, String>();
            for (int i = 0; i < fieldNames.size(); i++) {
                obj.put(fieldNames.get(i), x.get(i));
            }
            String json = mapper.writeValueAsString(obj);
            System.out.println("json" + json);
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, x.get(0), json);
            producer.send(rec);
        }
        producer.close();
    }
}
