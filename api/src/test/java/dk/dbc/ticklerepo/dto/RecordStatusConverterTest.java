package dk.dbc.ticklerepo.dto;

import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RecordStatusConverterTest {
    private final RecordStatusConverter converter = new RecordStatusConverter();

    @Test
    public void convertToDatabaseColumn_statusArgIsNull_returnsNullValuedDatabaseObject() {
        final Object pgObject = converter.convertToDatabaseColumn(null);
        assertThat("PGobject", pgObject, is(notNullValue()));
        assertThat("PGobject type", ((PGobject) pgObject).getType(), is("record_status"));
        assertThat("PGobject value", ((PGobject) pgObject).getValue(), is(nullValue()));
    }

    @Test
    public void convertToDatabaseColumn() {
        final Object pgObject = converter.convertToDatabaseColumn(Record.Status.ACTIVE);
        assertThat("PGobject", pgObject, is(notNullValue()));
        assertThat("PGobject type", ((PGobject) pgObject).getType(), is("record_status"));
        assertThat("PGobject value", ((PGobject) pgObject).getValue(), is(Record.Status.ACTIVE.name()));
    }

    @Test
    public void toEntityAttribute_dbValueArgIsNull_throws() {
        assertThrows(IllegalArgumentException.class, () -> converter.convertToEntityAttribute(null));
    }

    @Test
    public void toEntityAttribute() {
        assertThat("ACTIVE", converter.convertToEntityAttribute("ACTIVE"), is(Record.Status.ACTIVE));
        assertThat("DELETED", converter.convertToEntityAttribute("DELETED"), is(Record.Status.DELETED));
        assertThat("RESET", converter.convertToEntityAttribute("RESET"), is(Record.Status.RESET));
        assertThat("UNKNOWN", converter.convertToEntityAttribute("UNKNOWN"), is(nullValue()));
    }
}
