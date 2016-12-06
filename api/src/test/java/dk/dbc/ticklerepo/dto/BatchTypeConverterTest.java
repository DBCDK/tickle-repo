/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo.dto;

import dk.dbc.commons.testutil.Assert;
import org.junit.Test;
import org.postgresql.util.PGobject;

import static dk.dbc.commons.testutil.Assert.isThrowing;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

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
        Assert.assertThat(() -> converter.convertToEntityAttribute(null), isThrowing(IllegalArgumentException.class));
    }

    @Test
    public void convertToEntityAttribute() {
        assertThat("TOTAL", converter.convertToEntityAttribute("TOTAL"), is(Batch.Type.TOTAL));
        assertThat("INCREMENTAL", converter.convertToEntityAttribute("INCREMENTAL"), is(Batch.Type.INCREMENTAL));
        assertThat("UNKNOWN", converter.convertToEntityAttribute("UNKNOWN"), is(nullValue()));
    }
}
