package org.fulin.chestnut;

import java.util.List;

/**
 * chestnut
 *
 * @author tangfulin
 * @since 17/3/3
 */
public class Response<T> {

    public static final Response<String> CLIENT_ERROR_RESPONSE = new Response<>(400, "Bad Request");
    public static final Response<String> SERVER_ERROR_RESPONSE = new Response<>(500, "Server Internal Error");
    public static final Response<List<User>> ALREADY_LIKE_ERROR_RESPONSE = new Response<>(499, "User Already Liked this Object");
    public int retCode;
    public String message;
    public String action = "like";
    public long uid;
    public long oid;
    public long nextCursor;
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

    public Response<T> with(long uid, long oid) {
        this.uid = uid;
        this.oid = oid;
        return this;
    }

    public Response<T> withCursor(long cursor) {
        this.nextCursor = cursor;
        return this;
    }

    public static <T> Response<T> of(T result) {
        return new Response<T>(200, "ok", result);
    }

    public static <T> Response<T> of(String action, long uid, long oid, T result) {
        Response<T> response = new Response<T>(200, "ok", result);
        response.action = action;
        response.uid = uid;
        response.oid = oid;
        return response;
    }

    public static <T> Response<T> of(String action, long uid, long oid, long cursor, T result) {
        return of(action, uid, oid, result).withCursor(cursor);
    }

}
