/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo.dto;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.NamedQueries;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.SequenceGenerator;

@Entity
@NamedQueries({
        @NamedQuery(name = DataSet.GET_DATASET_BY_NAME_QUERY_NAME, query = DataSet.GET_DATASET_BY_NAME_QUERY),
        @NamedQuery(name = DataSet.GET_DATASET_BY_RECORD_LOCALID_NAME, query = DataSet.GET_DATASET_BY_RECORD_LOCALID_QUERY)
})
public class DataSet {

    public static final String GET_DATASET_BY_NAME_QUERY_NAME = "DataSet.getDataSetByName";
    public static final String GET_DATASET_BY_NAME_QUERY =
            "SELECT dataset FROM DataSet dataSet WHERE dataset.name = :name";

    public static final String GET_DATASET_BY_RECORD_LOCALID_NAME = "DataSet.getDataSetByRecordLocalId";
    public static final String GET_DATASET_BY_RECORD_LOCALID_QUERY =
            "SELECT dataset FROM DataSet dataset WHERE dataset.id IN (" +
                    "SELECT record.dataset FROM Record record WHERE record.localId = :localId)";

    @Id
    @SequenceGenerator(
            name = "dataset_id_seq",
            sequenceName = "dataset_id_seq",
            allocationSize = 1)
    @GeneratedValue(
            strategy = GenerationType.SEQUENCE,
            generator = "dataset_id_seq")
    @Column(updatable = false)
    private int id;

    private String name;
    private String displayName;
    private int agencyId;

    public int getId() {
        return id;
    }

    public DataSet withId(int id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public DataSet withName(String name) {
        this.name = name;
        return this;
    }

    public String getDisplayName() {
        return displayName;
    }

    public DataSet withDisplayName(String displayName) {
        this.displayName = displayName;
        return this;
    }

    public int getAgencyId() {
        return agencyId;
    }

    public DataSet withAgencyId(int agencyId) {
        this.agencyId = agencyId;
        return this;
    }

    @Override
    public String toString() {
        return "DataSet{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", displayName='" + displayName + '\'' +
                ", agencyId=" + agencyId +
                '}';
    }
}
