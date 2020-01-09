package buff;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Example {

	public static void main(String[] args) {
		String sourcePath = "resourses/source.txt";
		String targetpath = "resourses/target.txt";
		BufferedReader reader = null;
		BufferedWriter writer = null;
		
		try {
			reader = new BufferedReader(new FileReader(sourcePath));
			writer = new BufferedWriter(new FileWriter(targetpath));
			String line = null;
			while ((line = reader.readLine()) != null) {
				writer.write(line.toUpperCase());
				writer.newLine();

			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			
			try {
				reader.close();
				writer.close();
			} catch (IOException e) {

				e.printStackTrace();
			}
			
		}
		
	}
}
