package poi;

import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import model.Client;

public class XLSXReader{

	private String filePath;

	public XLSXReader(String filePath) {
		this.filePath = filePath;
	}

	public List<Client> getClients() throws Exception {
		File excelFile = new File(filePath);
		FileInputStream fis = new FileInputStream(excelFile);

		// we create an XSSF Workbook object for our XLSX Excel File
		XSSFWorkbook workbook = new XSSFWorkbook(fis);
		// we get first sheet
		XSSFSheet sheet = workbook.getSheetAt(0);

		// we iterate on rows
		Iterator<Row> rowIt = sheet.iterator();
		List<Client> clients = new ArrayList<Client>();
		rowIt.next();
		while (rowIt.hasNext()) {
			
			Row row = rowIt.next();
			if(row.getRowNum()==0) continue;
			Client client = new Client();

			// iterate on cells for the current row
			Iterator<Cell> cellIterator = row.cellIterator();
			

			while (cellIterator.hasNext()) {
				Cell cell = cellIterator.next();
				int columnIndex = cell.getColumnIndex();
				switch (columnIndex) {
				case 0:
					client.setId((int) cell.getNumericCellValue());
					//client.setNumber((int) cell.getNumericCellValue());
					break;
				case 1:
					client.setFirstName(cell.getStringCellValue());
					break;
				case 2:
					client.setLastName(cell.getStringCellValue());
					break;
				case 3:
					client.setGender(cell.getStringCellValue());
					break;
				case 4:
					client.setCountry(cell.getStringCellValue());
					break;
				case 5:
					client.setAge((int) cell.getNumericCellValue());
					break;
				case 6:
					
//					client.setDate(new Date(cell.getDateCellValue().getTime()));
					client.setDate(DateUtil.getJavaDate((long)cell.getNumericCellValue()));
					break;
				case 7:
					//client.setId((int) cell.getNumericCellValue());
					break;
				default:
					break;
				}
			}
			clients.add(client);

		}

		workbook.close();
		fis.close();
		return clients;
	}

}
