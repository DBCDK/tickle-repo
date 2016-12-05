/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo.dto;

import org.postgresql.util.PGobject;

import java.sql.SQLException;

public class BatchEntityTypeConversion {
    public Object toDatabaseColumn(BatchEntity.Type type) {
        if (type == null) {
            throw new IllegalArgumentException("type can not be null");
        }
        final PGobject pgObject = new PGobject();
        pgObject.setType("batch_type");
        try {
            pgObject.setValue(type.name());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return pgObject;
    }

    public BatchEntity.Type toEntityAttribute(Object dbValue) {
        if (dbValue == null) {
            throw new IllegalArgumentException("dbValue can not be null");
        }
        switch ((String) dbValue) {
            case "ACTIVE": return BatchEntity.Type.ACTIVE;
            case "DELETED": return BatchEntity.Type.DELETED;
            case "RESET": return BatchEntity.Type.RESET;
            default: return null;
        }
    }
}
