/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo.dto;

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.PostPersist;
import javax.persistence.PostUpdate;
import javax.persistence.SequenceGenerator;
import java.sql.Timestamp;
import java.util.Date;

@Entity
public class Record {
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
    private int agencyId;
    private String localId;
    private String trackingId;

    @Convert(converter = RecordStatusConverter.class)
    private Status status;

    @Column(insertable = false, updatable = false)
    private Timestamp timeOfCreation;

    private Timestamp timeOfLastModification;

    private byte[] content;
    private String chksum;

    public Record() {}

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

    public int getAgencyId() {
        return agencyId;
    }

    public Record withAgencyId(int agencyId) {
        this.agencyId = agencyId;
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

    public Timestamp getTimeOfLastModification() {
        return timeOfLastModification;
    }

    public byte[] getContent() {
        return content;
    }

    public Record withContent(byte[] content) {
        this.content = content;
        return this;
    }

    public String getChksum() {
        return chksum;
    }

    public Record withChksum(String chksum) {
        this.chksum = chksum;
        return this;
    }

    @Override
    public String toString() {
        return "Record{" +
                "id=" + id +
                ", batch=" + batch +
                ", dataset=" + dataset +
                ", agencyId=" + agencyId +
                ", localId='" + localId + '\'' +
                ", trackingId='" + trackingId + '\'' +
                ", status=" + status +
                ", timeOfCreation=" + timeOfCreation +
                ", timeOfLastModification=" + timeOfLastModification +
                '}';
    }

    @PostPersist
    @PostUpdate
    void onDatabaseCommit() {
        this.timeOfLastModification = new Timestamp(new Date().getTime());
    }
}
