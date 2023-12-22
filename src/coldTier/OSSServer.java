import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.nio.file.Files;

public class OSSServer {

    private static int PORT;
    private ExecutorService executorService = Executors.newFixedThreadPool(500);

    public void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server started on port: " + PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                executorService.submit(new ClientHandler(clientSocket));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class ClientHandler implements Runnable {
        private Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream())) {

                String operation = objectInputStream.readUTF();

                if ("UPLOAD".equals(operation)) {
                    System.out.println("Server received upload request");
                    handleUpload(objectInputStream, objectOutputStream);
                } else if ("DOWNLOAD".equals(operation)) {
                    System.out.println("Server received download request");
                    handleDownload(objectInputStream, objectOutputStream);
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    clientSocket.close(); // Close the client socket
                    System.out.println("Server disconnected the client");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private void handleUpload(ObjectInputStream objectInputStream, ObjectOutputStream objectOutputStream)
                throws IOException {
            try {
                String targetFilePath = objectInputStream.readUTF();
                byte[] content = (byte[]) objectInputStream.readObject();
                System.out.println("Upload file path: " + targetFilePath + ", content length: " + content.length);
                File targetFile = new File("data/" + targetFilePath);
                try (FileOutputStream fileOutputStream = new FileOutputStream(targetFile)) {
                    fileOutputStream.write(content);
                }
                objectOutputStream.writeBoolean(true);
                objectOutputStream.flush(); // Ensure the response is sent
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                objectOutputStream.writeBoolean(false);
                objectOutputStream.flush(); // Ensure the response is sent
            }
        }

        private void handleDownload(ObjectInputStream objectInputStream, ObjectOutputStream objectOutputStream)
                throws IOException {
            String originalFilePath = objectInputStream.readUTF();
            File file = new File("data/" + originalFilePath);
            System.out.println("Download file path: " + originalFilePath + ", file exists: " + file.exists());
            if (file.exists()) {
                byte[] content = Files.readAllBytes(file.toPath());
                objectOutputStream.writeBoolean(true); // Indicate that the file was found
                objectOutputStream.writeObject(content);
                objectOutputStream.flush();
            } else {
                objectOutputStream.writeBoolean(false); // Indicate that the file was not found
                objectOutputStream.flush();
            }
        }

    }

    public void setServerPort(int port) {
        PORT = port;
    }

    public static void main(String[] args) {
        OSSServer server = new OSSServer();
        int port = Integer.parseInt(args[0]);
        server.setServerPort(port);
        server.startServer();
    }
}
