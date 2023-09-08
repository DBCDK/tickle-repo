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
import jakarta.persistence.NamedQueries;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.SequenceGenerator;

import java.sql.Timestamp;

@Entity
@NamedQueries({
        @NamedQuery(name = Batch.GET_NEXT_BATCH_QUERY_NAME, query = Batch.GET_NEXT_BATCH_QUERY),
        @NamedQuery(name = Batch.GET_BATCH_BY_KEY_QUERY_NAME, query = Batch.GET_BATCH_BY_KEY_QUERY)
})
public class Batch {
    public static final String GET_BATCH_BY_KEY_QUERY =
            "SELECT batch FROM Batch batch WHERE batch.batchKey = :key";
    public static final String GET_BATCH_BY_KEY_QUERY_NAME = "Batch.getBatchByKey";

    public static final String GET_NEXT_BATCH_QUERY =
            "SELECT batch FROM Batch batch WHERE batch.id > :lastSeenId AND batch.dataset = :dataset ORDER BY batch.id ASC";
    public static final String GET_NEXT_BATCH_QUERY_NAME = "Batch.getNextBatch";

    public enum Type {
        TOTAL,
        INCREMENTAL
    }

    @Id
    @SequenceGenerator(
            name = "batch_id_seq",
            sequenceName = "batch_id_seq",
            allocationSize = 1)
    @GeneratedValue(
            strategy = GenerationType.SEQUENCE,
            generator = "batch_id_seq")
    @Column(updatable = false)
    private int id;

    private int dataset;
    private int batchKey;

    @Convert(converter = BatchTypeConverter.class)
    private Type type;

    @Column(insertable = false, updatable = false)
    private Timestamp timeOfCreation;

    private Timestamp timeOfCompletion;

    @Column(columnDefinition = "jsonb")
    @Convert(converter = JSonBConverter.class)
    private String metadata;

    public int getId() {
        return id;
    }

    public Batch withId(int id) {
        this.id = id;
        return this;
    }

    public int getDataset() {
        return dataset;
    }

    public Batch withDataset(int dataset) {
        this.dataset = dataset;
        return this;
    }

    public int getBatchKey() {
        return batchKey;
    }

    public Batch withBatchKey(int batchKey) {
        this.batchKey = batchKey;
        return this;
    }

    public Type getType() {
        return type;
    }

    public Batch withType(Type type) {
        this.type = type;
        return this;
    }

    public Timestamp getTimeOfCreation() {
        return timeOfCreation;
    }

    public Batch withTimeOfCreation(Timestamp timeOfCreation) {
        this.timeOfCreation = timeOfCreation;
        return this;
    }

    public Timestamp getTimeOfCompletion() {
        return timeOfCompletion;
    }

    public Batch withTimeOfCompletion(Timestamp timeOfCompletion) {
        this.timeOfCompletion = timeOfCompletion;
        return this;
    }

    public String getMetadata() {
        return metadata;
    }

    public Batch withMetadata(String metadata) {
        this.metadata = metadata;
        return this;
    }

    @Override
    public String toString() {
        return "Batch{" +
                "id=" + id +
                ", dataset=" + dataset +
                ", batchKey=" + batchKey +
                ", type=" + type +
                ", timeOfCreation=" + timeOfCreation +
                ", timeOfCompletion=" + timeOfCompletion +
                ", metadata='" + metadata + '\'' +
                '}';
    }

}
