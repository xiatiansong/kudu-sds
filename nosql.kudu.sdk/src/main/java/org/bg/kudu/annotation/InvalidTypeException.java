package org.bg.kudu.annotation;

/**
 *
 * @author: xiatiansong
 * @create: 2019-01-02 14:53
 **/
public class InvalidTypeException extends RuntimeException {

    private static final long serialVersionUID = 0;

    public InvalidTypeException(String msg) {
        super(msg);
    }

    public InvalidTypeException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public InvalidTypeException copy() {
        return new InvalidTypeException(getMessage(), this);
    }
}
