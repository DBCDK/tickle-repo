/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo.dto;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RecordTest {
    @Test
    public void recordBatchUpdatedWhenChecksumIndicatesChange() {
        final Batch expectedBatch = new Batch()
                .withId(2);
        final String expectedChecksum = "chksum2";

        final Record record = new Record()
                .withBatch(1)
                .withChecksum("chksum1");

        record.updateBatchIfModified(expectedBatch, expectedChecksum);

        assertThat("Record batch", record.getBatch(), is(expectedBatch.getId()));
        assertThat("Record checksum", record.getChecksum(), is(expectedChecksum));
    }

    @Test
    public void recordBatchUpdatedWhenCurrentChecksumIsNull() {
        final Batch expectedBatch = new Batch()
                .withId(2);
        final String expectedChecksum = "chksum2";

        final Record record = new Record()
                .withBatch(1)
                .withChecksum(null);

        record.updateBatchIfModified(expectedBatch, expectedChecksum);

        assertThat("Record batch", record.getBatch(), is(expectedBatch.getId()));
        assertThat("Record checksum", record.getChecksum(), is(expectedChecksum));
    }

    @Test
    public void recordBatchNotUpdatedWhenChecksumMatches() {
        final Batch batch = new Batch()
                .withId(2);

        final Record record = new Record()
                .withBatch(1)
                .withChecksum("chksum1");

        record.updateBatchIfModified(batch, record.getChecksum());

        assertThat("Record batch", record.getBatch(), is(1));
        assertThat("Record checksum", record.getChecksum(), is("chksum1"));
    }
}