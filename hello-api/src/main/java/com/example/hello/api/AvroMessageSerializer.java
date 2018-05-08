package com.example.hello.api;

import akka.util.ByteString;
import com.lightbend.lagom.javadsl.api.deser.DeserializationException;
import com.lightbend.lagom.javadsl.api.deser.MessageSerializer;
import com.lightbend.lagom.javadsl.api.deser.SerializationException;
import com.lightbend.lagom.javadsl.api.transport.MessageProtocol;
import com.lightbend.lagom.javadsl.api.transport.NotAcceptable;
import com.lightbend.lagom.javadsl.api.transport.UnsupportedMediaType;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.List;

public class AvroMessageSerializer implements MessageSerializer<HelloEvent, ByteString> {
    private final KafkaAvroDeserializer kafkaDeserializer;
    private final KafkaAvroSerializer kafkaSerializer;

    private final Schema schema = new Schema.Parser().parse(
            "{\n"
                    + "\"type\": \"record\",\n"
                    + "\"name\": \"GreetingMessageChanged\",\n"
                    + "\"fields\": [\n"
                    + " {\"name\": \"name\", \"type\": \"string\"},\n"
                    + " {\"name\": \"message\", \"type\": \"string\"}\n"
                    + "]}"
    );

    private final NegotiatedSerializer<HelloEvent, ByteString> lagomSerializer =
            new NegotiatedSerializer<HelloEvent, ByteString>() {
                @Override
                public MessageProtocol protocol() {
                    // This isn't a valid MIME type, but it is the one recommended by the Avro specification
                    // see https://avro.apache.org/docs/1.8.1/spec.html#HTTP+as+Transport
                    // and https://issues.apache.org/jira/browse/AVRO-488
                    return new MessageProtocol().withContentType("avro/binary");
                }

                @Override
                public ByteString serialize(HelloEvent helloEvent) throws SerializationException {
                    return ByteString.fromArray(kafkaSerializer.serialize(null, toRecord(helloEvent)));
                }
            };

    private final NegotiatedDeserializer<HelloEvent, ByteString> lagomDeserializer =
            new NegotiatedDeserializer<HelloEvent, ByteString>() {
                @Override
                public HelloEvent deserialize(ByteString bytes) throws DeserializationException {
                    return fromRecord((GenericRecord) kafkaDeserializer.deserialize(null, bytes.toArray()));
                }
            };

    AvroMessageSerializer(String schemaRegistryUrl, boolean isKey) {
        kafkaDeserializer = createDeserializer(schemaRegistryUrl, isKey);
        kafkaSerializer = createSerializer(schemaRegistryUrl, isKey);
    }

    private KafkaAvroDeserializer createDeserializer(String schemaRegistryUrl, boolean isKey) {
        KafkaAvroDeserializer s = new KafkaAvroDeserializer();
        HashMap<String, Object> configs = new HashMap<>();
        configs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");
        s.configure(configs, isKey);
        return s;
    }

    private KafkaAvroSerializer createSerializer(String schemaRegistryUrl, boolean isKey) {
        KafkaAvroSerializer s = new KafkaAvroSerializer();
        HashMap<String, Object> configs = new HashMap<>();
        configs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        s.configure(configs, isKey);
        return s;
    }

    private GenericRecord toRecord(HelloEvent helloEvent) {
        if (helloEvent instanceof HelloEvent.GreetingMessageChanged) {
            HelloEvent.GreetingMessageChanged greetingMessageChanged = (HelloEvent.GreetingMessageChanged) helloEvent;
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("name", greetingMessageChanged.getName());
            avroRecord.put("message", greetingMessageChanged.getMessage());
            return avroRecord;
        } else {
            throw new IllegalArgumentException("Cannot convert unknown event type to Avro: " + helloEvent);
        }
    }

    private HelloEvent fromRecord(GenericRecord record) {
        return new HelloEvent.GreetingMessageChanged(
                (String) record.get("name"),
                (String) record.get("message")
        );
    }

    @Override
    public NegotiatedSerializer<HelloEvent, ByteString> serializerForRequest() {
        return lagomSerializer;
    }

    @Override
    public NegotiatedDeserializer<HelloEvent, ByteString> deserializer(MessageProtocol protocol) {
        return lagomDeserializer;
    }

    @Override
    public NegotiatedSerializer<HelloEvent, ByteString> serializerForResponse(List<MessageProtocol> acceptedMessageProtocols) {
        return lagomSerializer;
    }
}
