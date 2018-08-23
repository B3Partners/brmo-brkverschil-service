package nl.b3p.brmo.verschil.util;

import com.fasterxml.jackson.core.JsonProcessingException;

public class ResultSetSerializerException extends JsonProcessingException {
    public ResultSetSerializerException(Throwable t) {
        super(t);
    }
}
