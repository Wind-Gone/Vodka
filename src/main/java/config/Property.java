package config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public abstract class Property {

    protected String path;
    protected Properties props;

    public Property(String path) throws IOException {
        this.path = path;
        props = new Properties();
        props.load(new FileInputStream(path));
    }

    public void addProperty(String key, String value) {
        props.setProperty(key, value);
    }

    public abstract void loadProperty() throws Exception;
}
