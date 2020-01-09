package dao;

import java.util.Date;
import java.util.List;

import model.Client;

public interface ClientsDAO {
	Client getClientById(Client client);
	
	List<Client> getAllClients();
	void deleateAllClients();
	boolean insertClient(Client client);
	boolean updateClient(Client client);
	boolean deleteClient(Client client);
	
	boolean deleteClientsByCountry(String country);
	List<Client> getAllClientsRegistredBefore(Date date);
	boolean updateOrInsertClient(Client client);
}
