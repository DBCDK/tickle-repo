/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo.dto;

import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.NamedNativeQueries;
import jakarta.persistence.NamedNativeQuery;
import jakarta.persistence.NamedQueries;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.SequenceGenerator;
import org.eclipse.persistence.annotations.Cache;
import org.eclipse.persistence.annotations.CacheType;

import java.sql.Timestamp;
import java.util.Date;


@Entity
@NamedQueries({
        @NamedQuery(name = Record.GET_RECORD_BY_LOCALID_QUERY_NAME, query = Record.GET_RECORD_BY_LOCALID_QUERY),
        @NamedQuery(name = Record.GET_RECORDS_BY_LOCALIDS_QUERY_NAME, query = Record.GET_RECORDS_BY_LOCALIDS_QUERY),
        @NamedQuery(name = Record.GET_RECORDS_IN_BATCH_QUERY_NAME, query = Record.GET_RECORDS_IN_BATCH_QUERY),
        @NamedQuery(name = Record.GET_RECORDS_IN_DATASET_QUERY_NAME, query = Record.GET_RECORDS_IN_DATASET_QUERY),
        @NamedQuery(name = Record.MARK_QUERY_NAME, query = Record.MARK_QUERY),
        @NamedQuery(name = Record.UNDO_MARK_QUERY_NAME, query = Record.UNDO_MARK_QUERY),
        @NamedQuery(name = Record.SWEEP_QUERY_NAME, query = Record.SWEEP_QUERY),
        @NamedQuery(name = Record.SWEEP_OUTDATED_QUERY_NAME, query = Record.SWEEP_OUTDATED_QUERY),
})
@NamedNativeQueries({
        @NamedNativeQuery(name = Record.NUMBER_OF_RECORDS_IN_DATASET_QUERY_NAME,
                query = Record.NUMBER_OF_RECORDS_IN_DATASET_QUERY),
        @NamedNativeQuery(name = Record.ESTIMATED_NUMBER_OF_RECORDS_IN_DATASET_QUERY_NAME,
                query = Record.ESTIMATED_NUMBER_OF_RECORDS_IN_DATASET_QUERY)
})
@Cache(type = CacheType.WEAK, disableHits = true)
public class Record {
    public static final String GET_RECORD_BY_LOCALID_QUERY =
            "SELECT record FROM Record record WHERE record.dataset = :dataset AND record.localId = :localId";
    public static final String GET_RECORD_BY_LOCALID_QUERY_NAME = "Record.getRecordByLocalId";
    public static final String GET_RECORDS_BY_LOCALIDS_QUERY =
            "SELECT record FROM Record record WHERE record.dataset = :dataset AND record.localId in :localIds";
    public static final String GET_RECORDS_BY_LOCALIDS_QUERY_NAME = "Record.getRecordsByLocalIds";

    public static final String GET_RECORDS_IN_BATCH_QUERY =
            "SELECT record FROM Record record WHERE record.batch = ?1 ORDER BY record.id ASC";
    public static final String GET_RECORDS_IN_BATCH_QUERY_NAME = "Record.getRecordsInBatch";

    public static final String GET_RECORDS_IN_DATASET_QUERY =
            "SELECT record FROM Record record WHERE record.dataset = ?1 ORDER BY record.id ASC";
    public static final String GET_RECORDS_IN_DATASET_QUERY_NAME = "Record.getRecordsInDataSet";

    public static final String MARK_QUERY =
            "UPDATE Record record SET record.status = dk.dbc.ticklerepo.dto.Record.Status.RESET " +
                    "WHERE record.dataset = :dataset AND record.status = dk.dbc.ticklerepo.dto.Record.Status.ACTIVE";
    public static final String MARK_QUERY_NAME = "Record.mark";

    public static final String UNDO_MARK_QUERY =
            "UPDATE Record record SET record.status = dk.dbc.ticklerepo.dto.Record.Status.ACTIVE " +
                    "WHERE record.dataset = :dataset AND record.status = dk.dbc.ticklerepo.dto.Record.Status.RESET";
    public static final String UNDO_MARK_QUERY_NAME = "Record.undoMark";

    public static final String SWEEP_QUERY =
            "UPDATE Record record SET record.batch = :batch, record.status = dk.dbc.ticklerepo.dto.Record.Status.DELETED, record.timeOfLastModification = :now, record.checksum = '' " +
                    "WHERE record.dataset = :dataset AND record.status = dk.dbc.ticklerepo.dto.Record.Status.RESET";
    public static final String SWEEP_QUERY_NAME = "Record.sweep";

    public static final String SWEEP_OUTDATED_QUERY =
            "UPDATE Record record SET record.batch = :batch, record.status = dk.dbc.ticklerepo.dto.Record.Status.DELETED, record.timeOfLastModification = :now, record.checksum = '' " +
                    "WHERE record.dataset = :dataset AND record.timeOfLastModification < :cutOffTime";
    public static final String SWEEP_OUTDATED_QUERY_NAME = "Record.sweepOutdated";

    public static final String NUMBER_OF_RECORDS_IN_DATASET_QUERY =
            "SELECT count(*) FROM record WHERE dataset = ?";
    public static final String NUMBER_OF_RECORDS_IN_DATASET_QUERY_NAME =
            "Record.numberOfRecordsInDataSet";

    public static final String ESTIMATED_NUMBER_OF_RECORDS_IN_DATASET_QUERY =
            "EXPLAIN (ANALYZE OFF, FORMAT TEXT) SELECT count(*) FROM record WHERE dataset = ?";
    public static final String ESTIMATED_NUMBER_OF_RECORDS_IN_DATASET_QUERY_NAME =
            "Record.estimatedNumberOfRecordsInDataSet";

    public enum Status {
        ACTIVE,
        DELETED,
        RESET
    }

    @Id
    @SequenceGenerator(
            name = "record_id_seq",
            sequenceName = "record_id_seq",
            allocationSize = 1)
    @GeneratedValue(
            strategy = GenerationType.SEQUENCE,
            generator = "record_id_seq")
    @Column(updatable = false)
    private int id;

    private int batch;
    private int dataset;
    private String localId;
    private String trackingId;

    @Convert(converter = RecordStatusConverter.class)
    private Status status;

    @Column(insertable = false, updatable = false)
    private Timestamp timeOfCreation;

    private Timestamp timeOfLastModification;

    private byte[] content;
    private String checksum;

    public Record() {
    }

    public int getId() {
        return id;
    }

    public Record withId(int id) {
        this.id = id;
        return this;
    }

    public int getBatch() {
        return batch;
    }

    public Record withBatch(int batch) {
        this.batch = batch;
        return this;
    }

    public int getDataset() {
        return dataset;
    }

    public Record withDataset(int dataset) {
        this.dataset = dataset;
        return this;
    }

    public String getLocalId() {
        return localId;
    }

    public Record withLocalId(String localId) {
        this.localId = localId;
        return this;
    }

    public String getTrackingId() {
        return trackingId;
    }

    public Record withTrackingId(String trackingId) {
        this.trackingId = trackingId;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Record withStatus(Status status) {
        this.status = status;
        return this;
    }

    public Timestamp getTimeOfCreation() {
        return timeOfCreation;
    }

    public Record withTimeOfCreation(Timestamp timeOfCreation) {
        if (timeOfCreation != null) {
            this.timeOfCreation = new Timestamp(timeOfCreation.getTime());
        }
        return this;
    }

    public Timestamp getTimeOfLastModification() {
        return timeOfLastModification;
    }

    public Record withTimeOfLastModification(Timestamp timeOfLastModification) {
        if (timeOfLastModification != null) {
            this.timeOfLastModification = new Timestamp(timeOfLastModification.getTime());
        }
        return this;
    }

    public byte[] getContent() {
        return content;
    }

    public Record withContent(byte[] content) {
        this.content = content;
        return this;
    }

    public String getChecksum() {
        return checksum;
    }

    public Record withChecksum(String checksum) {
        this.checksum = checksum;
        return this;
    }

    /**
     * Updates batch for this record if given checksum indicates a change when compared to current checksum
     *
     * @param batch    record batch
     * @param checksum new checksum
     * @return this record
     */
    public Record updateBatchIfModified(Batch batch, String checksum) {
        if (this.checksum == null || !this.checksum.equals(checksum)) {
            this.batch = batch.getId();
            this.checksum = checksum;
        }
        return this;
    }

    @Override
    public String toString() {
        return "Record{" +
                "id=" + id +
                ", batch=" + batch +
                ", dataset=" + dataset +
                ", localId='" + localId + '\'' +
                ", trackingId='" + trackingId + '\'' +
                ", status=" + status +
                ", timeOfCreation=" + timeOfCreation +
                ", timeOfLastModification=" + timeOfLastModification +
                '}';
    }

    @PrePersist
    @PreUpdate
    void onDatabaseCommit() {
        this.timeOfLastModification = new Timestamp(new Date().getTime());
    }
}
