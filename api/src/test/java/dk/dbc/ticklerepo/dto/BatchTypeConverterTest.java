package dk.dbc.ticklerepo.dto;

import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BatchTypeConverterTest {

    private final BatchTypeConverter converter = new BatchTypeConverter();

    @Test
    public void convertToDatabaseColumn_typeArgIsNull_returnsNullValuedDatabaseObject() {
        final Object pgObject = converter.convertToDatabaseColumn(null);
        assertThat("PGobject", pgObject, is(notNullValue()));
        assertThat("PGobject type", ((PGobject) pgObject).getType(), is("batch_type"));
        assertThat("PGobject value", ((PGobject) pgObject).getValue(), is(nullValue()));
    }

    @Test
    public void convertToDatabaseColumn() {
        final Object pgObject = converter.convertToDatabaseColumn(Batch.Type.TOTAL);
        assertThat("PGobject", pgObject, is(notNullValue()));
        assertThat("PGobject type", ((PGobject) pgObject).getType(), is("batch_type"));
        assertThat("PGobject value", ((PGobject) pgObject).getValue(), is(Batch.Type.TOTAL.name()));
    }

    @Test
    public void convertToEntityAttribute_dbValueArgIsNull_throws() {
        assertThrows(IllegalArgumentException.class, () -> converter.convertToEntityAttribute(null));
    }

    @Test
    public void convertToEntityAttribute() {
        assertThat("TOTAL", converter.convertToEntityAttribute("TOTAL"), is(Batch.Type.TOTAL));
        assertThat("INCREMENTAL", converter.convertToEntityAttribute("INCREMENTAL"), is(Batch.Type.INCREMENTAL));
        assertThat("UNKNOWN", converter.convertToEntityAttribute("UNKNOWN"), is(nullValue()));
    }
}
