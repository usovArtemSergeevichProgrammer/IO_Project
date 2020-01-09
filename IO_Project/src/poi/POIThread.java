package poi;

import java.util.List;

import model.Client;
import util.HTMLTableGenerator;
import util.MailUtils;


public class POIThread extends Thread{
@Override
public void run() {
	PerformanceAnalyzer.start("POI CLIENTS JOB");
    
    List<Client> clients = null;
     
    try {
        clients = new XLSXReader(
                "resourses/file_example.xlsx").getClients();
        POIUtils.generateClientTemplate( clients, "resourses/Output_Clients_template.docx");
    } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
    
    HTMLTableGenerator table = new HTMLTableGenerator(clients.size(), 8);

    table.addHeaders("id", "number", "firstName", "lastName", "gender", "country", "age", "date");
    
    for (Client client : clients) {
        table.addRowsData(client.getId() + "",
                client.getNumber() + "",
                client.getFirstName(),
                client.getLastName(),
                client.getGender(),
                client.getCountry(),
                client.getAge() + "",
                client.getDate().toString());
    }
    
    MailUtils.send("j1819sender@gmail.com", 
            "vadriarh@gmail.com", 
            "resourses/Output_Clients_template.docx",
            table.build(),
            "NO SPAM 2");
    
    
    PerformanceAnalyzer.end();

}
}
