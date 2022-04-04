package net.christophschubert.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


public abstract class FieldNameCleaner<R extends ConnectRecord<R>> implements Transformation<R> {


    static class SchemaWithMapping {
        public final Schema schema;
        public final Map<String, String> fieldMapping;

        public SchemaWithMapping(Schema schema, Map<String, String> fieldMapping) {
            this.schema = schema;
            this.fieldMapping = fieldMapping;
        }

        @Override
        public String toString() {
            return "SchemaWithMapping{" +
                    "schema=" + schema +
                    ", fieldMapping=" + fieldMapping +
                    '}';
        }
    }

    private static final Logger log = LoggerFactory.getLogger(FieldNameCleaner.class);

    // pattern for valid Avro identifies, see https://avro.apache.org/docs/current/spec.html#names.
    // Protobuf is more restricted (names cannot start with underscore), see https://developers.google.com/protocol-buffers/docs/reference/proto3-spec#identifiers
    private static final Pattern pattern = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private Cache<Schema, SchemaWithMapping> schemaUpdateCache;

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return record; //only deal with records with a schema
        } else {
            return applyWithSchema(record);
        }
    }

    //visible for testing
    static boolean hasFieldsToBeRenamed(Schema schema) {
        return schema.fields().stream().anyMatch(field -> !hasValidName(field.name()));
    }

    //visible for testing
    static boolean hasValidName(String name) {
        return pattern.matcher(name).matches();
    }

    private R applyWithSchema(R record) {
        final var schema = operatingSchema(record);
        final var struct = operatingValue(record);
        requireStruct(struct, "field renaming");
        if (!hasFieldsToBeRenamed(schema)) {
            return record;
        }
        SchemaWithMapping updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            updatedSchema = remapSchema(schema);
            schemaUpdateCache.put(schema, updatedSchema);
        } else {
            log.trace("Found schema in cache");
        }
        return newRecord(record, updatedSchema.schema, remap((Struct)struct, updatedSchema));
    }


    Struct remap(Struct struct, SchemaWithMapping mapping) {
        final var remapped = new Struct(mapping.schema);
        for (Field field : mapping.schema.fields()) {
            if (mapping.fieldMapping.containsKey(field.name())) {
                remapped.put(mapping.fieldMapping.get(field.name()), struct.get(field));
            } else {
                remapped.put(field, struct.get(field));
            }
        }
        return remapped;
    }

    static String remapFieldName(String name) {
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    static SchemaWithMapping remapSchema(Schema schema) {
        final var builder = SchemaBuilder.struct();
        final var mappedFields = new HashMap<String, String>();
        for (Field field : schema.fields()) {
            if (hasValidName(field.name())) {
                builder.field(field.name(), field.schema());
            } else {
                final var newFieldName = remapFieldName(field.name());
                mappedFields.put(field.name(), newFieldName);
                builder.field(newFieldName, field.schema());
            }
        }
        return new SchemaWithMapping(builder.build(), mappedFields);
    }


    @Override
    public void configure(Map<String, ?> props) {
        //TODO: consider making cache-size configurable
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(128));
    }

    @Override
    public org.apache.kafka.common.config.ConfigDef config() {
        return new ConfigDef(); // SMT does not offer any options
    }

    @Override
    public void close() {
        //nothing to do
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends FieldNameCleaner<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends FieldNameCleaner<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }

}
