package erreesse.utils.configuration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfiguration {

    private Properties prop;
    private static AppConfiguration instance;

    private AppConfiguration(String fileConf) {
        try {
            InputStream input = this.getClass().getResourceAsStream("/"+fileConf);
            prop = new Properties();
            prop.load(input);
        } catch (FileNotFoundException e) {
            System.err.println("Config file not found: "+fileConf);
        }
        catch (IOException e) {
            System.err.println("Error reading conf file: "+fileConf);
        }
    }

    public static AppConfiguration getInstance() {
        if (instance == null) {
            instance = new AppConfiguration("configuration.properties");
        }
        return instance;
    }

    public static String getProperty(String confKey) {
        return getInstance().prop.getProperty(confKey);
    }


}
