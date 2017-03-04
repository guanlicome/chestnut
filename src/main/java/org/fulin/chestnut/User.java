package org.fulin.chestnut;

/**
 * chestnut
 *
 * @author tangfulin
 * @since 17/3/4
 */
public class User {
    long uid;
    String nickname;

    public User(long uid, String nickname) {
        this.uid = uid;
        this.nickname = nickname;
    }

    public long getUid() {
        return uid;
    }

    public void setUid(long uid) {
        this.uid = uid;
    }

    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        User user = (User) o;

        if (uid != user.uid) return false;
        return nickname.equals(user.nickname);
    }

    @Override
    public int hashCode() {
        int result = (int) (uid ^ (uid >>> 32));
        result = 31 * result + nickname.hashCode();
        return result;
    }
}
