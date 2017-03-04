package org.fulin.chestnut;

/**
 * chestnut
 *
 * @author tangfulin
 * @since 17/3/3
 */
public class Response<T> {

    public static final Response<String> CLIENT_ERROR_RESPONSE = new Response<>(400, "Bad Request");
    public static final Response<String> SERVER_ERROR_RESPONSE = new Response<>(500, "Server Internal Error");
    public static final Response<long[]> ALREADY_LIKE_ERROR_RESPONSE = new Response<>(499, "User Already Liked this Object");
    public int retCode;
    public String message;
    public String action;
    public long uid;
    public long oid;
    public T result;

    public Response(int code) {
        this(code, "Server Internal Error");
    }

    public Response(int code, String msg) {
        this(code, msg, null);
    }

    public Response(int code, String msg, T result) {
        this.retCode = code;
        this.message = msg;
        this.result = result;
    }

    public static <T> Response of(T result) {
        return new Response(200, "ok", result);
    }

    public static <T> Response of(String action, long uid, long oid, T result) {
        Response response = new Response(200, "ok", result);
        response.action = action;
        response.uid = uid;
        response.oid = oid;
        return response;
    }

}
