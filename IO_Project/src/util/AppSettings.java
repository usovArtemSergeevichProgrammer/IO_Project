package util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class AppSettings {
	private Properties props;
	private static AppSettings instance;
	private AppSettings() {
		props=new Properties();
		try {
			props.load(new FileInputStream("resourses/config.properties"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static AppSettings getInstance(){
		if(instance==null) {
			instance=new AppSettings();
		}
		return instance;
	}
	
	public Properties getProps() {
		return props;
	}
	
	public String getValue(String key) {
		if(!props.containsKey(key)) {
			System.out.println("No such key in props: "+key);
		}
		return props.getProperty(key);

	}

}
