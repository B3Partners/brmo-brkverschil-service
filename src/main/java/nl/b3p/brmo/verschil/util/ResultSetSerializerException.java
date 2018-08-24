package nl.b3p.brmo.verschil.util;

import com.fasterxml.jackson.core.JsonProcessingException;

public class ResultSetSerializerException extends JsonProcessingException {
    private static final long serialVersionUID = 1L;
    public ResultSetSerializerException(Throwable t) {
        super(t);
    }
}
