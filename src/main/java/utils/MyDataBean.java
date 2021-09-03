package utils;

import java.io.Serializable;

public class MyDataBean implements Serializable {
    private static final long serialVersionUID = -6275514108819129601L;

    private String value;

    public MyDataBean() {
    }

    public MyDataBean(String s) {
        value = s;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MyDataBean{" +
                "value='" + value + '\'' +
                '}';
    }
}
