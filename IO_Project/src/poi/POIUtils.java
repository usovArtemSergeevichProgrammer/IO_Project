package poi;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.naming.ldap.SortControl;

import java.util.TreeMap;

import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;

import model.Client;

public class POIUtils {

	public static final String DATE_FORMAT_PATTERN = "yyyyMMdd";

	public static Map<String, Integer> sortColumns(List<Client> clients, String key) {
		Map<String, Integer> data = new TreeMap<String, Integer>();
		for (Client client : clients) {
			switch (key) {
			case "Country":
				String keyCountry=client.getCountry();
				if (!data.containsKey(keyCountry)) {
					data.put(keyCountry, 1);
				} else {
					data.put(keyCountry, data.get(keyCountry) + 1);
				}
				break;
			case "Gender":
				String keyGender=client.getGender();
				if (!data.containsKey(client.getGender())) {
					data.put(keyGender, 1);
				} else {
					data.put(keyGender, data.get(keyGender) + 1);
				}
				break;

			}
		}
		return data;
	}

	public static void generateClientTemplate(List<Client> clients, String path) {
		XWPFDocument template = new XWPFDocument();
		OutputStream os = null;

		try {
			os = new FileOutputStream(path);
			XWPFParagraph paragraph = template.createParagraph();
			XWPFRun run = paragraph.createRun();
			XWPFRun run2 = paragraph.createRun();
			run.setBold(true);
			SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT_PATTERN);
			String dateStr = formatter.format(new Date());
			run.setText(String.format("CLIENTS REPORT SINCE - %s ", dateStr));
			run.addBreak();
			run.setBold(true);
			run2.setBold(false);
			run.setText("Some ");
			run.addBreak();

			Map<String, Integer> dataYear = new TreeMap<String, Integer>();
			for (Client client : clients) {
				LocalDate date = LocalDate.parse(new SimpleDateFormat("yyyy-MM-dd").format(client.getDate()));
				String key = String.valueOf(date.getYear());
				if (!dataYear.containsKey(key)) {
					dataYear.put(key, 1);
				} else {
					dataYear.put(key, dataYear.get(key) + 1);
				}
			}
			for (Entry<String, Integer> entry : dataYear.entrySet()) {
				

				run2.setText(entry.getKey() + " " + entry.getValue());
				run2.addBreak();
			}
			run.addBreak();

			run.setText("By gender");
			run.addBreak();
			for (Entry<String, Integer> entry : sortColumns(clients, "Gender").entrySet()) {


				run2.setText(entry.getKey() + " " + entry.getValue());
				run2.addBreak();
			}
			run.addBreak();
			run.setText("By Country");
			run.addBreak();
			for (Entry<String, Integer> entry : sortColumns(clients, "Country").entrySet()) {
				run2.setText(entry.getKey() + " " + entry.getValue());
				run2.addBreak();
			}

			template.write(os);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				os.close();
				template.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	public static void main(String[] args) {
		POIThread thread=new POIThread();
		thread.start();
		System.out.println("ALL STARTED OK");
		
	}

}
