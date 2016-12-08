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
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.SequenceGenerator;
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

    @Column(nullable = false)
    private int dataset;

    @Column(nullable = false)
    private int batchKey;

    @Convert(converter = BatchTypeConverter.class)
    private Type type;

    @Column(insertable = false, updatable = false)
    private Timestamp timeOfCreation;

    private Timestamp timeOfCompletion;

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

    @Override
    public String toString() {
        return "Batch{" +
                "id=" + id +
                ", dataset=" + dataset +
                ", batchKey=" + batchKey +
                ", type=" + type +
                ", timeOfCreation=" + timeOfCreation +
                ", timeOfCompletion=" + timeOfCompletion +
                '}';
    }
}
