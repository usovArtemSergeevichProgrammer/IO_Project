package poi;

import util.HTMLTableGenerator;
import java.util.Collections;
import java.util.List;

import model.Client;
import util.MailUtils;

public class POITester {

	public static void main(String[] args) {
		XLSXReader reader = new XLSXReader("resourses/file_example.xlsx");
		try {

			List<Client> clients=reader.getClients();
			HTMLTableGenerator table=new HTMLTableGenerator(clients.size()+1,8);
			table.addHeaders("Number","First Name","Last Name","Gender","Country","Age","Date","Id");
			for (Client client : clients) {
				table.addRowsData(client.getNumber()+"",client.getFirstName(),
						client.getLastName(),client.getGender(),client.getCountry(),
						client.getAge()+"",client.getDate().toString(),client.getId()+"");
			}
			MailUtils.send("j1819sender@gmail.com", 
					"vadriarh@gmail.com", 
					"resourses/Sorted_Clients.txt",
					table.build(),
					"No SPam");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		
	}

}
