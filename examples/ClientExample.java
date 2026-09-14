import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MCP 数据收发服务 —— Java 客户端例程（仅用 JDK 11+ 标准库，无需第三方依赖）
 *
 * 功能：
 * 1. sendText(String text)                    推送文本
 * 2. sendImage(String path)                   推送图片（自动 base64）
 * 3. sendTextImage(String text, String path)  图 + 文一次推送
 * 4. receiveResults(long afterId, int waitSeconds)  接收 AI 返回的结果（游标 + 长轮询）
 *
 * 编译：javac ClientExample.java
 * 运行：java ClientExample
 */
public class ClientExample {

    // ===== 按需修改这三项 =====
    static final String BASE_URL = "http://127.0.0.1:8765";
    static final String API_KEY = "dk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"; // 运行 mcp_service.py 后从 config.json 读取

    static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    /** 最小化 JSON 字符串转义（够本例程使用） */
    static String esc(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t");
    }

    /** 发送 HTTP 请求并返回响应体字符串 */
    static String http(String method, String path, String jsonBody, Duration timeout)
            throws IOException, InterruptedException {
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + path))
                .timeout(timeout)
                .header("X-API-Key", API_KEY);
        if (jsonBody != null) {
            b.header("Content-Type", "application/json; charset=utf-8");
            b.POST(HttpRequest.BodyPublishers.ofString(jsonBody));
        } else {
            b.GET();
        }
        HttpResponse<String> resp = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IOException("HTTP " + resp.statusCode() + ": " + resp.body());
        }
        return resp.body();
    }

    // ---------------------------------------------------------------- 发送数据

    /** 推送文本 */
    static String sendText(String text) throws IOException, InterruptedException {
        String body = "{\"text\":\"" + esc(text) + "\"}";
        return http("POST", "/receive", body, Duration.ofSeconds(15));
    }

    /** 推送图片（自动转 base64） */
    static String sendImage(String imagePath) throws IOException, InterruptedException {
        String mime = imagePath.toLowerCase().endsWith(".png") ? "image/png" : "image/jpeg";
        String b64 = Base64.getEncoder().encodeToString(Files.readAllBytes(Path.of(imagePath)));
        String body = "{\"images\":[{\"data\":\"" + b64 + "\",\"mime\":\"" + mime + "\"}]}";
        return http("POST", "/receive", body, Duration.ofSeconds(30));
    }

    /** 图 + 文一次推送（AI 一次 get_new_data 就能全部读到） */
    static String sendTextImage(String text, String imagePath) throws IOException, InterruptedException {
        String mime = imagePath.toLowerCase().endsWith(".png") ? "image/png" : "image/jpeg";
        String b64 = Base64.getEncoder().encodeToString(Files.readAllBytes(Path.of(imagePath)));
        String body = "{\"text\":\"" + esc(text) + "\","
                + "\"images\":[{\"data\":\"" + b64 + "\",\"mime\":\"" + mime + "\"}]}";
        return http("POST", "/receive", body, Duration.ofSeconds(30));
    }

    // ---------------------------------------------------------------- 接收 AI 结果

    /**
     * 游标方式接收 AI 通过 send_result 返回的结果。
     *
     * @param afterId     上次读到的最大结果编号（首次传 0）
     * @param waitSeconds 0-60，没有新结果时服务端等待多久再返回（长轮询）
     * @return 响应 JSON 字符串，形如 {"ok":true,"count":1,"data":[{"id":1,"time":"...","type":"text","content":"..."}]}
     */
    static String receiveResults(long afterId, int waitSeconds) throws IOException, InterruptedException {
        waitSeconds = Math.max(0, Math.min(60, waitSeconds));
        String path = "/responses?after_id=" + afterId + "&wait_seconds=" + waitSeconds;
        return http("GET", path, null, Duration.ofSeconds(waitSeconds + 20));
    }

    /** 从结果 JSON 中提取 content 字段的简易方法（例程用，生产建议用 Jackson/Gson） */
    static String extractContent(String json) {
        Matcher m = Pattern.compile("\"content\":\"(.*?)\"\\s*[},]").matcher(json);
        return m.find() ? m.group(1).replace("\\n", "\n").replace("\\\"", "\"") : "(无 content)";
    }

    /** 从响应中提取最大 id，用于更新游标 */
    static long extractMaxId(String json) {
        long max = 0;
        Matcher m = Pattern.compile("\"id\":(\\d+)").matcher(json);
        while (m.find()) {
            max = Math.max(max, Long.parseLong(m.group(1)));
        }
        return max;
    }

    // ---------------------------------------------------------------- 使用示例
    public static void main(String[] args) throws Exception {
        // 1. 推送文本
        System.out.println("发送文本: " + sendText("你好 AI，请告诉我今天的日期"));

        // 2. 推送图片（有图片时取消注释）
        // System.out.println("发送图片: " + sendImage("D:/test/demo.png"));

        // 3. 图 + 文一起推（有需要时取消注释）
        // System.out.println("发送图+文: " + sendTextImage("这张图里有什么？", "D:/test/demo.png"));

        // 4. 循环接收 AI 的返回结果（游标 + 10 秒长轮询）
        long afterId = 0;
        System.out.println("开始监听 AI 返回结果（Ctrl+C 退出）...");
        while (true) {
            try {
                String resp = receiveResults(afterId, 10);
                long maxId = extractMaxId(resp);
                if (maxId > afterId) {
                    afterId = maxId;                       // 更新游标
                    System.out.println("[AI返回] " + extractContent(resp));
                }
            } catch (Exception e) {
                System.out.println("请求失败（服务未启动？）: " + e.getMessage());
                Thread.sleep(3000);                        // 出错后等 3 秒重试
            }
        }
    }
}
