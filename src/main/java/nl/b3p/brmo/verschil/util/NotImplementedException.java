package nl.b3p.brmo.verschil.util;

public class NotImplementedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NotImplementedException(){}

    public NotImplementedException(Throwable t){
        super(t);
    }

    public NotImplementedException(String error){
        super(error);
    }
}