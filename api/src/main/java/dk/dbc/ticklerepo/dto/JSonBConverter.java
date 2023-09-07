package dk.dbc.ticklerepo.dto;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import org.postgresql.util.PGobject;

import java.sql.SQLException;

@Converter
public class JSonBConverter implements AttributeConverter<String, PGobject> {

    @Override
    public PGobject convertToDatabaseColumn(String s) {
        try {
            PGobject object = new PGobject();
            object.setType("jsonb");
            object.setValue(s);
            return object;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String convertToEntityAttribute(PGobject pGobject) {
        if(pGobject == null) return null;
        return pGobject.getValue();
    }
}
