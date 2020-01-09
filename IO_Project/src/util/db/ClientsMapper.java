package util.db;

import java.util.List;

import model.Client;

public interface ClientsMapper {
	void save(Client client);
	void update(Client client);
	void delete(Client client);
	void saveAll(List<Client> clients);
	void deleteAll(List<Client> clients);
	void updateAll(List<Client> clients);
	Client load(Client client);
	List<Client> loadAll(List<Client> clients);
	List<Client> loadAll();
}
