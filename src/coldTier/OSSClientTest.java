import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

public class OSSClientTest {

    private static final String HOST = "localhost";
    private static final int PORT = 1234;

    public boolean uploadFileToOSS(String targetFilePath, byte[] content) {
        try (Socket socket = new Socket(HOST, PORT);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream())) {

            objectOutputStream.writeUTF("UPLOAD");
            objectOutputStream.writeUTF(targetFilePath);
            objectOutputStream.writeObject(content);

            return objectInputStream.readBoolean(); // Wait and read the response
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean uploadFileToOSS(String targetFilePath, String content) {
        return uploadFileToOSS(targetFilePath, content.getBytes());
    }

    public boolean uploadFileToOSS(String targetFilePath) {
        try {
            byte[] content = Files.readAllBytes(Paths.get(targetFilePath));
            return uploadFileToOSS(targetFilePath, content);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean downloadFileFromOSS(String originalFilePath, String targetStorePath) {
        System.out.println("Try to download from server: " + originalFilePath);
        try (Socket socket = new Socket(HOST, PORT);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream())) {

            objectOutputStream.writeUTF("DOWNLOAD");
            objectOutputStream.writeUTF(originalFilePath);
            objectOutputStream.flush();
            System.out.println("Try to download from server: " + originalFilePath + ", request sent");
            boolean fileExists = objectInputStream.readBoolean();
            System.out.println("Recv the file exist flag");
            if (fileExists) {
                byte[] content = (byte[]) objectInputStream.readObject();
                Files.write(Paths.get(targetStorePath), content);
                return true;
            } else {
                System.out.println("File not found on server: " + originalFilePath);
                return false;
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) {
        OSSClientTest client = new OSSClientTest();
        boolean uploadSuccess = client.uploadFileToOSS("server/file.txt", "Hello World".getBytes());
        System.out.println("Upload successful: " + uploadSuccess);

        boolean downloadSuccess = client.downloadFileFromOSS("server/file.txt", "local/file.txt");
        System.out.println("Download successful: " + downloadSuccess);
    }
}
