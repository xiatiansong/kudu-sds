package org.bg.kudu.annotation;

/**
 * 自定义的缺少相关注解的异常
 * @author: xiatiansong
 * @create: 2018-12-27 17:37
 **/
public class MissingAnnotationException extends RuntimeException {

    private String msgDes;

    public MissingAnnotationException() {
        super();
    }

    public MissingAnnotationException(String message) {
        super(message);
        msgDes = message;
    }

    public MissingAnnotationException(String retCd, String msgDes) {
        super();
        this.msgDes = msgDes;
    }

    public String getMsgDes() {
        return msgDes;
    }
}
