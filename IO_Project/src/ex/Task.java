package ex;

import java.io.File;

public class Task {

	public static void transform(String path, String path2) {
		IOStream stream = new IOStream(path);
		stream.read();
		File file = new File(path2);
		if (file.exists()) {
			System.out.println("the file already exists");
		} else {
			System.out.printf("The file %s was created", path2);
		}
		IOStream stream2 = new IOStream(file.getAbsolutePath());
		stream2.write(stream.getData());
	}

	public static int searchKey(String sourcePath, String key) {
		IOStream stream = new IOStream(sourcePath);
		stream.read();
		int count = 0;
		String text = stream.getData();
		int index = text.indexOf(key);

		while (index != -1) {
			count++;
			text = text.substring(index + key.length());
			index = text.indexOf(key);
		}

		return count;
	}

}
