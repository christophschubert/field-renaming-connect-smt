package net.christophschubert.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class FieldNameCleanerTest {

    @Test
    void namePattern() {
        assertTrue(FieldNameCleaner.hasValidName("aA99_"));
        assertTrue(FieldNameCleaner.hasValidName("_aaBB99"));
        assertFalse(FieldNameCleaner.hasValidName("a.b"));
    }

    @Test
    void schemaTest() {
        final var validOneField = SchemaBuilder.struct().field("a_b_c", Schema.STRING_SCHEMA).build();
        final var validThreeFields = SchemaBuilder.struct().field("a_b_c", Schema.STRING_SCHEMA)
                .field("AVC11", Schema.STRING_SCHEMA).
                field("_aaa", Schema.STRING_SCHEMA).build();
        final var invalidOneField = SchemaBuilder.struct().field("a.b", Schema.STRING_SCHEMA).build();

        final var invalidThreeFields = SchemaBuilder.struct().field("a_b", Schema.STRING_SCHEMA)
                .field("aa.bb", Schema.STRING_SCHEMA)
                .field("acb", Schema.STRING_SCHEMA).build();


        assertFalse(FieldNameCleaner.hasFieldsToBeRenamed(validOneField));
        assertFalse(FieldNameCleaner.hasFieldsToBeRenamed(validThreeFields));
        assertTrue(FieldNameCleaner.hasFieldsToBeRenamed(invalidOneField));
        assertTrue(FieldNameCleaner.hasFieldsToBeRenamed(invalidThreeFields));
    }


    @Test
    void nothingToRemapTest() {
        SchemaBuilder builder = new SchemaBuilder(SchemaBuilder.struct().type());
        builder.field("a_string", Schema.STRING_SCHEMA);
        builder.field("b_long", Schema.INT64_SCHEMA);
        builder.field("bool", Schema.BOOLEAN_SCHEMA);
        final var schema = builder.build();

        Struct value = new Struct(schema);
        value.put("a_string", "string_value");
        value.put("b_long", 12345L);
        value.put("bool", true);

        SinkRecord record = new SinkRecord("topic", 0, schema, value, schema, value, 0);

        final FieldNameCleaner<SinkRecord> regexFieldRenamer = new FieldNameCleaner.Value<>();

        assertEquals(record, regexFieldRenamer.apply(record));
    }

    @Test
    void shouldRemapFieldsTest() {
        SchemaBuilder builder = new SchemaBuilder(SchemaBuilder.struct().type());
        builder.field("a.string", Schema.STRING_SCHEMA);
        builder.field("b.long", Schema.INT64_SCHEMA);
        builder.field("c_bool", Schema.BOOLEAN_SCHEMA);
        final var schema = builder.build();

        Struct value = new Struct(schema);
        value.put("a.string", "string_value");
        value.put("b.long", 12345L);
        value.put("c_bool", true);

        SchemaBuilder builder2 = new SchemaBuilder(SchemaBuilder.struct().type());
        builder2.field("a_string", Schema.STRING_SCHEMA);
        builder2.field("b_long", Schema.INT64_SCHEMA);
        builder2.field("c_bool", Schema.BOOLEAN_SCHEMA);
        final var reMappedSchema = builder2.build();

        Struct remappedValue = new Struct(reMappedSchema);
        remappedValue.put("a_string", "string_value");
        remappedValue.put("b_long", 12345L);
        remappedValue.put("c_bool", true);

        SinkRecord record = new SinkRecord("topic", 0, null, null, schema, value, 0);
        SinkRecord remappedRecord = new SinkRecord("topic", 0, null, null, reMappedSchema, remappedValue, 0);


        final FieldNameCleaner<SinkRecord> regexFieldRenamer = new FieldNameCleaner.Value<>();
        regexFieldRenamer.configure(Collections.emptyMap());

        assertEquals(remappedRecord, regexFieldRenamer.apply(record));
    }
}