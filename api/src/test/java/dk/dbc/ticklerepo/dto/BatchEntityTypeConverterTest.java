package dk.dbc.ticklerepo.dto;

import dk.dbc.commons.testutil.Assert;
import org.junit.Test;
import org.postgresql.util.PGobject;

import static dk.dbc.commons.testutil.Assert.isThrowing;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class BatchEntityTypeConverterTest {

    private final BatchEntityTypeConverter converter = new BatchEntityTypeConverter();

    @Test
    public void convertToDatabaseColumn_typeArgIsNull_throws() {
        Assert.assertThat(() -> converter.convertToDatabaseColumn(null), isThrowing(IllegalArgumentException.class));
    }

    @Test
    public void convertToDatabaseColumn() {
        final Object pgObject = converter.convertToDatabaseColumn(BatchEntity.Type.ACTIVE);
        assertThat("PGobject", pgObject, is(notNullValue()));
        assertThat("PGobject type", ((PGobject) pgObject).getType(), is("batch_type"));
        assertThat("PGobject value", ((PGobject) pgObject).getValue(), is(BatchEntity.Type.ACTIVE.name()));
    }

    @Test
    public void convertToEntityAttribute_dbValueArgIsNull_throws() {
        Assert.assertThat(() -> converter.convertToEntityAttribute(null), isThrowing(IllegalArgumentException.class));
    }

    @Test
    public void convertToEntityAttribute() {
        assertThat("ACTIVE", converter.convertToEntityAttribute("ACTIVE"), is(BatchEntity.Type.ACTIVE));
        assertThat("RESET", converter.convertToEntityAttribute("RESET"), is(BatchEntity.Type.RESET));
        assertThat("DELETED", converter.convertToEntityAttribute("DELETED"), is(BatchEntity.Type.DELETED));
        assertThat("UNKNOWN", converter.convertToEntityAttribute("UNKNOWN"), is(nullValue()));
    }
}
