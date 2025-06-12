import java.io.*;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class UserLogsMaker {
    private static final Path sourceDir = Paths.get("").toAbsolutePath().resolve("transactions");
    private static final Path targetDir = sourceDir.resolve("transactions_by_users");
    private static final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public UserLogsMaker() {}

    public static void processLogs() throws IOException {
        deleteLogFiles(targetDir);
        List<String> lines = readLogs();
        createLogsFiles(lines);
        writeLogs(lines);
        calculateBalance();
    }

    private static void deleteLogFiles(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }

        boolean hasFiles = Files.list(dir)
                .filter(Files::isRegularFile)
                .findAny()
                .isPresent();

        if(!hasFiles) {
            return;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.log")) {
            for (Path file : stream) {
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    throw new IOException("не удалось удалить .log файл");
                }
            }
        }
    }

    private static List<String> readLogs() throws IOException {
        List<String> lines = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourceDir, "*.log")) {
            for (Path file : stream) {
                lines.addAll(Files.readAllLines(file, StandardCharsets.UTF_8));
            }
        } catch (IOException e){
            throw new IOException("произошла ошибка в чтении .log файлов");
        }
        lines.sort(Comparator.comparing(line -> line.substring(1, 20)));
        return lines;
    }

    private static void createLogsFiles(List<String> lines) throws IOException {
        Files.createDirectories(targetDir);
        for(String s : lines){
            String[] log = s.split(" ");
            Path filePath = Paths.get(targetDir + "\\" + log[2] + ".log");
            if(!Files.exists(filePath)){
                Files.createFile(filePath);
            }
        }
    }

    private static void writeLogs(List<String> lines) throws IOException {
        for(String s : lines){
            String[] log = s.split(" ");
            Path filePath = Paths.get(targetDir + "\\" + log[2] + ".log");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath.toFile(), StandardCharsets.UTF_8,true))) {
                if(log[3].equals("transferred")) {
                    Path filePath2 = Paths.get(targetDir + "\\" + log[6] + ".log");
                    try (BufferedWriter writer2 = new BufferedWriter(new FileWriter(filePath2.toFile(), StandardCharsets.UTF_8,true))) {
                        String forReceiver = String.format("[%s] %s received %s from %s",
                                s.substring(1,20), log[6] , log[4] ,log[2]
                        );
                        writer2.write(forReceiver);
                        writer2.newLine();
                    } catch (IOException e) {
                        throw new IOException("произошла ошибка в записи в .log файл для получателя");
                    }
                }
                writer.write(s);
                writer.newLine();
            } catch (IOException e) {
                throw new IOException("произошла ошибка в записи в .log файл для отправляющего");
            }
        }
    }

    private static void calculateBalance() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(targetDir, "*.log")) {
            for (Path path : stream) {
                List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8)
                        .stream()
                        .filter(line -> !line.contains("final balance"))
                        .toList();

                double balance = 0.0;
                boolean hasValidOperations = false;

                for (String line : lines) {
                    String[] parts = line.split(" ");
                    if (parts.length < 5) continue;

                    String operation;
                    double amount;
                    int amountIndex;

                    if (parts.length >= 6 && parts[3].equals("balance") && parts[4].equals("inquiry")) {
                        operation = "balance inquiry";
                        amountIndex = 5;
                    } else {
                        operation = parts[3];
                        amountIndex = 4;
                    }

                    try {
                        amount = Double.parseDouble(parts[amountIndex]);
                        switch (operation) {
                            case "balance inquiry":
                                balance = amount;
                                hasValidOperations = true;
                                break;
                            case "transferred":
                            case "withdrew":
                                balance -= amount;
                                hasValidOperations = true;
                                break;
                            case "received":
                                balance += amount;
                                hasValidOperations = true;
                                break;
                        }
                    } catch (NumberFormatException e) {
                        continue;
                    }
                }

                if (hasValidOperations) {
                    String username = path.getFileName().toString().replace(".log", "");
                    String balanceLine = String.format(
                            "[%s] %s final balance %.2f",
                            LocalDateTime.now().format(dateFormat),
                            username,
                            balance
                    );
                    try (FileWriter writer = new FileWriter(path.toFile(), StandardCharsets.UTF_8,true)) {
                        writer.write(balanceLine);
                    } catch (IOException e) {
                        throw new IOException("произошла ошибка в записи баланса в .log файл");
                    }
                }
            }
        }
    }
}