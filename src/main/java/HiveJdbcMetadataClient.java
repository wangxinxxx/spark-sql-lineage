import org.apache.hive.service.cli.thrift.TOpenSessionReq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.net.URL;


/**
 * Minimal Hive JDBC client for metadata-only operations such as SHOW CREATE TABLE.
 */
public final class HiveJdbcMetadataClient {
    public static final String DEFAULT_JDBC_URL =
            "jdbc:hive2://hiveserver.58dns.org:10000";
    public static final String DEFAULT_DATABASE_JDBC_URL =
            "jdbc:hive2://hiveserver.58dns.org:10000/hdp_zhuanzhuan_dw_global";
    public static final String DEFAULT_USERNAME = "hdp_ubu_zhuanzhuan";
    public static final String DEFAULT_PASSWORD = "";
    public static final String DEFAULT_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    public static final List<String> DEFAULT_SESSION_STATEMENTS = Collections.unmodifiableList(Arrays.asList(
            "set mapreduce.job.queuename=root.offline.hdp_ubu_zhuanzhuan.platform",
            "set wbdp.job.name=zhuanzhuan_zeye_hive_zhangyecheng",
            "set 58.department=转转技术部",
            "set 58.user=zhangyecheng",
            "set hive.merge.smallfiles.avgsize=32000000",
            "set hive.exec.parallel=true"));

    public static final class Config {
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private final String driverClassName;
        private final List<String> sessionStatements;

        public Config(
                String jdbcUrl,
                String username,
                String password,
                String driverClassName,
                List<String> sessionStatements) {
            this.jdbcUrl = Objects.requireNonNull(jdbcUrl, "jdbcUrl");
            this.username = username == null ? "" : username;
            this.password = password == null ? "" : password;
            this.driverClassName = Objects.requireNonNull(driverClassName, "driverClassName");
            this.sessionStatements = Collections.unmodifiableList(
                    new ArrayList<>(sessionStatements == null ? Collections.<String>emptyList() : sessionStatements));
        }

        public static Config fromLegacyHiveJdbc() {
            return new Config(
                    DEFAULT_JDBC_URL,
                    DEFAULT_USERNAME,
                    DEFAULT_PASSWORD,
                    DEFAULT_DRIVER,
                    DEFAULT_SESSION_STATEMENTS);
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getDriverClassName() {
            return driverClassName;
        }

        public List<String> getSessionStatements() {
            return sessionStatements;
        }
    }

    private final Config config;

    public HiveJdbcMetadataClient(Config config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    public String showCreateTable(String qualifiedTableName) throws SQLException {
        String sql = "SHOW CREATE TABLE " + qualifiedTableName;
        try (Connection connection = openConnection();
             Statement statement = connection.createStatement()) {
            applySessionStatements(statement);
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                String ddl = readAllRows(resultSet);
                if (ddl.trim().isEmpty()) {
                    throw new SQLException("Empty SHOW CREATE TABLE result for " + qualifiedTableName);
                }
                return ddl;
            }
        }
    }

    private Connection openConnection() throws SQLException {
        try {
            Class.forName(config.getDriverClassName());
        } catch (ClassNotFoundException err) {
            throw new SQLException(
                    "Failed to load Hive JDBC driver " + config.getDriverClassName()
                            + ". Add Maven dependency org.apache.hive:hive-jdbc:"
                            + "2.3.7 or put the Hive JDBC jar on the runtime classpath and reimport the project.",
                    err);
        }
        logHiveJdbcRuntimeDiagnostics();
        return DriverManager.getConnection(
                config.getJdbcUrl(),
                config.getUsername(),
                config.getPassword());
    }

    private void logHiveJdbcRuntimeDiagnostics() {
        logClassOrigin("org.apache.hive.jdbc.HiveDriver");
        logClassOrigin("org.apache.hive.jdbc.HiveConnection");
        logClassOrigin("org.apache.hive.service.rpc.thrift.TOpenSessionReq");
        try {
            TOpenSessionReq request = new TOpenSessionReq();
            System.out.println("[HIVE-JDBC-DIAG] TOpenSessionReq.defaultClientProtocol="
                    + request.getClient_protocol());
        } catch (Throwable err) {
            System.out.println("[HIVE-JDBC-DIAG] failed to instantiate TOpenSessionReq: " + err);
        }
    }

    private void logClassOrigin(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            URL codeSource = clazz.getProtectionDomain() == null
                    || clazz.getProtectionDomain().getCodeSource() == null
                    ? null
                    : clazz.getProtectionDomain().getCodeSource().getLocation();
            System.out.println("[HIVE-JDBC-DIAG] class=" + className + ", codeSource=" + codeSource);

            String resourceName = className.replace('.', '/') + ".class";
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Enumeration<URL> resources = classLoader == null
                    ? ClassLoader.getSystemResources(resourceName)
                    : classLoader.getResources(resourceName);
            int index = 0;
            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                System.out.println("[HIVE-JDBC-DIAG] classResource[" + index + "]=" + resource);
                index += 1;
            }
        } catch (Throwable err) {
            System.out.println("[HIVE-JDBC-DIAG] failed to inspect class " + className + ": " + err);
        }
    }

    private void applySessionStatements(Statement statement) throws SQLException {
        for (String sessionStatement : config.getSessionStatements()) {
            String trimmed = sessionStatement == null ? "" : sessionStatement.trim();
            if (!trimmed.isEmpty()) {
                statement.execute(trimmed);
            }
        }
    }

    private String readAllRows(ResultSet resultSet) throws SQLException {
        StringBuilder builder = new StringBuilder();
        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnCount = metadata.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                String value = resultSet.getString(i);
                if (value != null && !value.trim().isEmpty()) {
                    if (builder.length() > 0) {
                        builder.append('\n');
                    }
                    builder.append(value);
                }
            }
        }
        return builder.toString();
    }
}
