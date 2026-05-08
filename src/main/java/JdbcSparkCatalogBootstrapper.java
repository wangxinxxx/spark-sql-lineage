import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Bootstraps a local Spark catalog from Hive JDBC metadata without enabling Hive support.
 */
public final class JdbcSparkCatalogBootstrapper implements AutoCloseable {
    private static final Pattern BLOCK_COMMENT_PATTERN = Pattern.compile("(?s)/\\*.*?\\*/");
    private static final Pattern LINE_COMMENT_PATTERN = Pattern.compile("(?m)--[^\\r\\n]*$");
    private static final Pattern HASH_COMMENT_PATTERN = Pattern.compile("(?m)#[^\\r\\n]*$");
    private static final Pattern SOURCE_TABLE_PATTERN =
            Pattern.compile("(?is)\\b(?:from|join)\\s+([`A-Za-z0-9_.]+)");
    private static final Pattern TARGET_TABLE_PATTERN =
            Pattern.compile("(?is)\\binsert\\s+(?:overwrite|into)\\s+table\\s+([`A-Za-z0-9_.]+)");
    private static final Pattern CREATE_TABLE_HEADER_PATTERN = Pattern.compile(
            "(?is)create\\s+(?:external\\s+)?table\\s+(?:if\\s+not\\s+exists\\s+)?([`A-Za-z0-9_.]+)\\s*\\(");

    private final HiveJdbcMetadataClient metadataClient;
    private final SparkSession sparkSession;
    private final boolean ownsSparkSession;

    public JdbcSparkCatalogBootstrapper(
            HiveJdbcMetadataClient metadataClient,
            SparkSession sparkSession) {
        this.metadataClient = Objects.requireNonNull(metadataClient, "metadataClient");
        this.sparkSession = Objects.requireNonNull(sparkSession, "sparkSession");
        this.ownsSparkSession = false;
    }

    public JdbcSparkCatalogBootstrapper(
            HiveJdbcMetadataClient metadataClient,
            LocalSparkSessionFactoryConfig sparkConfig) {
        this.metadataClient = Objects.requireNonNull(metadataClient, "metadataClient");
        this.sparkSession = new LocalSparkSessionFactory().create(
                Objects.requireNonNull(sparkConfig, "sparkConfig"));
        this.ownsSparkSession = true;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public BootstrapResult bootstrap(String sqlText) throws SQLException {
        List<String> tableNames = extractTableNames(sqlText);
        List<BootstrappedTable> bootstrappedTables = new ArrayList<>();
        for (String qualifiedTableName : tableNames) {
            String showCreateTableDdl = metadataClient.showCreateTable(qualifiedTableName);
            ParsedTableDdl parsedTableDdl = parseShowCreateTable(qualifiedTableName, showCreateTableDdl);
            createLocalDatabase(parsedTableDdl.qualifiedName.databaseName);
            dropLocalTable(parsedTableDdl.qualifiedName);
            String localCreateTableDdl = buildLocalCreateTableDdl(parsedTableDdl);
            sparkSession.sql(localCreateTableDdl);
            bootstrappedTables.add(new BootstrappedTable(
                    parsedTableDdl.qualifiedName.asUnquotedString(),
                    showCreateTableDdl,
                    localCreateTableDdl));
        }
        return new BootstrapResult(tableNames, bootstrappedTables);
    }

    public List<String> extractTableNames(String sqlText) {
        if (sqlText == null || sqlText.trim().isEmpty()) {
            return Collections.emptyList();
        }
        String sanitizedSql = stripComments(sqlText);
        Set<String> tableNames = new LinkedHashSet<>();
        collectMatches(tableNames, TARGET_TABLE_PATTERN, sanitizedSql);
        collectMatches(tableNames, SOURCE_TABLE_PATTERN, sanitizedSql);
        return new ArrayList<>(tableNames);
    }

    private void collectMatches(Set<String> tableNames, Pattern pattern, String sqlText) {
        Matcher matcher = pattern.matcher(sqlText);
        while (matcher.find()) {
            String raw = matcher.group(1);
            QualifiedName qualifiedName = QualifiedName.parse(raw);
            if (qualifiedName != null) {
                tableNames.add(qualifiedName.asUnquotedString());
            }
        }
    }

    private String stripComments(String sqlText) {
        String withoutBlockComments = BLOCK_COMMENT_PATTERN.matcher(sqlText).replaceAll(" ");
        String withoutLineComments = LINE_COMMENT_PATTERN.matcher(withoutBlockComments).replaceAll(" ");
        return HASH_COMMENT_PATTERN.matcher(withoutLineComments).replaceAll(" ");
    }

    private ParsedTableDdl parseShowCreateTable(String fallbackQualifiedTableName, String ddl) {
        String trimmedDdl = ddl == null ? "" : ddl.trim();
        if (trimmedDdl.isEmpty()) {
            throw new IllegalArgumentException("SHOW CREATE TABLE returned empty DDL for " + fallbackQualifiedTableName);
        }
        String upper = trimmedDdl.toUpperCase(Locale.ROOT);
        if (upper.startsWith("CREATE VIEW") || upper.startsWith("CREATE MATERIALIZED VIEW")) {
            throw new IllegalArgumentException("Views are not supported for local table bootstrap: " + fallbackQualifiedTableName);
        }

        Matcher headerMatcher = CREATE_TABLE_HEADER_PATTERN.matcher(trimmedDdl);
        QualifiedName qualifiedName = QualifiedName.parse(fallbackQualifiedTableName);
        int openParenthesisIndex;
        if (headerMatcher.find()) {
            QualifiedName parsedName = QualifiedName.parse(headerMatcher.group(1));
            if (parsedName != null) {
                qualifiedName = parsedName;
            }
            openParenthesisIndex = headerMatcher.end() - 1;
        } else {
            throw new IllegalArgumentException("Failed to parse CREATE TABLE header for " + fallbackQualifiedTableName);
        }

        int closeParenthesisIndex = findMatchingParenthesis(trimmedDdl, openParenthesisIndex);
        String columnBlock = trimmedDdl.substring(openParenthesisIndex + 1, closeParenthesisIndex);
        String remainder = trimmedDdl.substring(closeParenthesisIndex + 1);

        List<ColumnSpec> columns = parseColumnBlock(columnBlock);
        List<ColumnSpec> partitionColumns = parsePartitionBlock(remainder);
        String tableComment = extractTableComment(remainder);

        return new ParsedTableDdl(qualifiedName, columns, partitionColumns, tableComment);
    }

    private List<ColumnSpec> parsePartitionBlock(String remainder) {
        int partitionedByIndex = findTopLevelPhrase(remainder, "PARTITIONED BY");
        if (partitionedByIndex < 0) {
            return Collections.emptyList();
        }
        int openParenthesisIndex = remainder.indexOf('(', partitionedByIndex);
        if (openParenthesisIndex < 0) {
            return Collections.emptyList();
        }
        int closeParenthesisIndex = findMatchingParenthesis(remainder, openParenthesisIndex);
        String partitionBlock = remainder.substring(openParenthesisIndex + 1, closeParenthesisIndex);
        return parseColumnBlock(partitionBlock);
    }

    private String extractTableComment(String remainder) {
        int partitionedByIndex = findTopLevelPhrase(remainder, "PARTITIONED BY");
        String searchScope = partitionedByIndex >= 0 ? remainder.substring(0, partitionedByIndex) : remainder;
        int commentIndex = findTopLevelKeyword(searchScope, "COMMENT");
        if (commentIndex < 0) {
            return null;
        }
        return readSingleQuotedLiteral(searchScope, commentIndex + "COMMENT".length());
    }

    private List<ColumnSpec> parseColumnBlock(String block) {
        List<String> definitions = splitTopLevel(block);
        List<ColumnSpec> columns = new ArrayList<>();
        for (String definition : definitions) {
            String trimmed = definition.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            columns.add(parseColumnSpec(trimmed));
        }
        return columns;
    }

    private ColumnSpec parseColumnSpec(String definition) {
        String trimmed = definition.trim();
        String name;
        String remainder;
        if (trimmed.startsWith("`")) {
            int closingBacktick = trimmed.indexOf('`', 1);
            if (closingBacktick < 0) {
                throw new IllegalArgumentException("Malformed column definition: " + definition);
            }
            name = trimmed.substring(1, closingBacktick);
            remainder = trimmed.substring(closingBacktick + 1).trim();
        } else {
            int firstWhitespace = findFirstWhitespace(trimmed);
            if (firstWhitespace < 0) {
                throw new IllegalArgumentException("Malformed column definition: " + definition);
            }
            name = trimmed.substring(0, firstWhitespace);
            remainder = trimmed.substring(firstWhitespace + 1).trim();
        }

        int commentIndex = findTopLevelKeyword(remainder, "COMMENT");
        String type = commentIndex >= 0 ? remainder.substring(0, commentIndex).trim() : remainder.trim();
        String comment = commentIndex >= 0
                ? readSingleQuotedLiteral(remainder, commentIndex + "COMMENT".length())
                : null;

        return new ColumnSpec(stripQuotes(name), type, comment);
    }

    private String buildLocalCreateTableDdl(ParsedTableDdl parsedTableDdl) {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE ")
                .append(parsedTableDdl.qualifiedName.asQuotedString())
                .append(" (\n");
        appendColumnDefinitions(builder, parsedTableDdl.columns);
        builder.append("\n)\nUSING PARQUET");
        if (parsedTableDdl.tableComment != null && !parsedTableDdl.tableComment.isEmpty()) {
            builder.append("\nCOMMENT '")
                    .append(escapeSqlString(parsedTableDdl.tableComment))
                    .append("'");
        }
        if (!parsedTableDdl.partitionColumns.isEmpty()) {
            builder.append("\nPARTITIONED BY (\n");
            appendColumnDefinitions(builder, parsedTableDdl.partitionColumns);
            builder.append("\n)");
        }
        return builder.toString();
    }

    private void appendColumnDefinitions(StringBuilder builder, List<ColumnSpec> columns) {
        for (int i = 0; i < columns.size(); i++) {
            ColumnSpec column = columns.get(i);
            if (i > 0) {
                builder.append(",\n");
            }
            builder.append("  ").append(column.toSparkSql());
        }
    }

    private void createLocalDatabase(String databaseName) {
        if (databaseName == null || databaseName.isEmpty() || "default".equalsIgnoreCase(databaseName)) {
            return;
        }
        sparkSession.sql("CREATE DATABASE IF NOT EXISTS " + quoteIdentifier(databaseName));
    }

    private void dropLocalTable(QualifiedName qualifiedName) {
        sparkSession.sql("DROP TABLE IF EXISTS " + qualifiedName.asQuotedString());
    }

    private int findMatchingParenthesis(String text, int openParenthesisIndex) {
        int roundDepth = 0;
        int angleDepth = 0;
        int squareDepth = 0;
        boolean inSingleQuote = false;
        boolean inBacktick = false;
        for (int i = openParenthesisIndex; i < text.length(); i++) {
            char current = text.charAt(i);
            char next = i + 1 < text.length() ? text.charAt(i + 1) : '\0';

            if (inBacktick) {
                if (current == '`') {
                    inBacktick = false;
                }
                continue;
            }

            if (inSingleQuote) {
                if (current == '\'' && next == '\'') {
                    i++;
                    continue;
                }
                if (current == '\'' && (i == 0 || text.charAt(i - 1) != '\\')) {
                    inSingleQuote = false;
                }
                continue;
            }

            if (current == '`') {
                inBacktick = true;
                continue;
            }
            if (current == '\'') {
                inSingleQuote = true;
                continue;
            }

            if (current == '<') {
                angleDepth++;
                continue;
            }
            if (current == '>') {
                angleDepth = Math.max(0, angleDepth - 1);
                continue;
            }
            if (current == '[') {
                squareDepth++;
                continue;
            }
            if (current == ']') {
                squareDepth = Math.max(0, squareDepth - 1);
                continue;
            }

            if (angleDepth > 0 || squareDepth > 0) {
                continue;
            }

            if (current == '(') {
                roundDepth++;
                continue;
            }
            if (current == ')') {
                roundDepth--;
                if (roundDepth == 0) {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("Failed to find matching ')' in DDL text");
    }

    private List<String> splitTopLevel(String text) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int roundDepth = 0;
        int angleDepth = 0;
        int squareDepth = 0;
        boolean inSingleQuote = false;
        boolean inBacktick = false;

        for (int i = 0; i < text.length(); i++) {
            char currentChar = text.charAt(i);
            char next = i + 1 < text.length() ? text.charAt(i + 1) : '\0';

            if (inBacktick) {
                current.append(currentChar);
                if (currentChar == '`') {
                    inBacktick = false;
                }
                continue;
            }

            if (inSingleQuote) {
                current.append(currentChar);
                if (currentChar == '\'' && next == '\'') {
                    current.append(next);
                    i++;
                    continue;
                }
                if (currentChar == '\'' && (i == 0 || text.charAt(i - 1) != '\\')) {
                    inSingleQuote = false;
                }
                continue;
            }

            if (currentChar == '`') {
                inBacktick = true;
                current.append(currentChar);
                continue;
            }
            if (currentChar == '\'') {
                inSingleQuote = true;
                current.append(currentChar);
                continue;
            }

            if (currentChar == '(') {
                roundDepth++;
                current.append(currentChar);
                continue;
            }
            if (currentChar == ')') {
                roundDepth--;
                current.append(currentChar);
                continue;
            }
            if (currentChar == '<') {
                angleDepth++;
                current.append(currentChar);
                continue;
            }
            if (currentChar == '>') {
                angleDepth = Math.max(0, angleDepth - 1);
                current.append(currentChar);
                continue;
            }
            if (currentChar == '[') {
                squareDepth++;
                current.append(currentChar);
                continue;
            }
            if (currentChar == ']') {
                squareDepth = Math.max(0, squareDepth - 1);
                current.append(currentChar);
                continue;
            }

            if (currentChar == ',' && roundDepth == 0 && angleDepth == 0 && squareDepth == 0) {
                parts.add(current.toString());
                current.setLength(0);
                continue;
            }

            current.append(currentChar);
        }

        if (current.length() > 0) {
            parts.add(current.toString());
        }
        return parts;
    }

    private int findTopLevelPhrase(String text, String phrase) {
        return findTopLevelKeyword(text, phrase);
    }

    private int findTopLevelKeyword(String text, String keyword) {
        int roundDepth = 0;
        int angleDepth = 0;
        int squareDepth = 0;
        boolean inSingleQuote = false;
        boolean inBacktick = false;

        for (int i = 0; i <= text.length() - keyword.length(); i++) {
            char current = text.charAt(i);
            char next = i + 1 < text.length() ? text.charAt(i + 1) : '\0';

            if (inBacktick) {
                if (current == '`') {
                    inBacktick = false;
                }
                continue;
            }

            if (inSingleQuote) {
                if (current == '\'' && next == '\'') {
                    i++;
                    continue;
                }
                if (current == '\'' && (i == 0 || text.charAt(i - 1) != '\\')) {
                    inSingleQuote = false;
                }
                continue;
            }

            if (current == '`') {
                inBacktick = true;
                continue;
            }
            if (current == '\'') {
                inSingleQuote = true;
                continue;
            }
            if (current == '(') {
                roundDepth++;
                continue;
            }
            if (current == ')') {
                roundDepth = Math.max(0, roundDepth - 1);
                continue;
            }
            if (current == '<') {
                angleDepth++;
                continue;
            }
            if (current == '>') {
                angleDepth = Math.max(0, angleDepth - 1);
                continue;
            }
            if (current == '[') {
                squareDepth++;
                continue;
            }
            if (current == ']') {
                squareDepth = Math.max(0, squareDepth - 1);
                continue;
            }

            if (roundDepth == 0 && angleDepth == 0 && squareDepth == 0
                    && text.regionMatches(true, i, keyword, 0, keyword.length())
                    && isKeywordBoundary(text, i - 1)
                    && isKeywordBoundary(text, i + keyword.length())) {
                return i;
            }
        }
        return -1;
    }

    private String readSingleQuotedLiteral(String text, int offset) {
        int index = offset;
        while (index < text.length() && Character.isWhitespace(text.charAt(index))) {
            index++;
        }
        if (index >= text.length() || text.charAt(index) != '\'') {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (int i = index + 1; i < text.length(); i++) {
            char current = text.charAt(i);
            char next = i + 1 < text.length() ? text.charAt(i + 1) : '\0';
            if (current == '\'' && next == '\'') {
                builder.append('\'');
                i++;
                continue;
            }
            if (current == '\'' && text.charAt(i - 1) != '\\') {
                return builder.toString().replace("\\'", "'");
            }
            builder.append(current);
        }
        throw new IllegalArgumentException("Unterminated single-quoted literal in DDL");
    }

    private int findFirstWhitespace(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isWhitespace(value.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private boolean isKeywordBoundary(String text, int index) {
        if (index < 0 || index >= text.length()) {
            return true;
        }
        char value = text.charAt(index);
        return !Character.isLetterOrDigit(value) && value != '_' && value != '`';
    }

    private static String stripQuotes(String value) {
        return value == null ? null : value.replace("`", "").trim();
    }

    private static String quoteIdentifier(String value) {
        return "`" + value.replace("`", "``") + "`";
    }

    private static String escapeSqlString(String value) {
        return value.replace("'", "''");
    }

    @Override
    public void close() {
        if (ownsSparkSession) {
            sparkSession.stop();
        }
    }

    public static final class BootstrapResult {
        private final List<String> extractedTableNames;
        private final List<BootstrappedTable> bootstrappedTables;

        private BootstrapResult(List<String> extractedTableNames, List<BootstrappedTable> bootstrappedTables) {
            this.extractedTableNames = Collections.unmodifiableList(new ArrayList<>(extractedTableNames));
            this.bootstrappedTables = Collections.unmodifiableList(new ArrayList<>(bootstrappedTables));
        }

        public List<String> getExtractedTableNames() {
            return extractedTableNames;
        }

        public List<BootstrappedTable> getBootstrappedTables() {
            return bootstrappedTables;
        }
    }

    public static final class BootstrappedTable {
        private final String qualifiedTableName;
        private final String showCreateTableDdl;
        private final String localCreateTableDdl;

        private BootstrappedTable(String qualifiedTableName, String showCreateTableDdl, String localCreateTableDdl) {
            this.qualifiedTableName = qualifiedTableName;
            this.showCreateTableDdl = showCreateTableDdl;
            this.localCreateTableDdl = localCreateTableDdl;
        }

        public String getQualifiedTableName() {
            return qualifiedTableName;
        }

        public String getShowCreateTableDdl() {
            return showCreateTableDdl;
        }

        public String getLocalCreateTableDdl() {
            return localCreateTableDdl;
        }
    }

    private static final class ParsedTableDdl {
        private final QualifiedName qualifiedName;
        private final List<ColumnSpec> columns;
        private final List<ColumnSpec> partitionColumns;
        private final String tableComment;

        private ParsedTableDdl(
                QualifiedName qualifiedName,
                List<ColumnSpec> columns,
                List<ColumnSpec> partitionColumns,
                String tableComment) {
            this.qualifiedName = qualifiedName;
            this.columns = columns;
            this.partitionColumns = partitionColumns;
            this.tableComment = tableComment;
        }
    }

    private static final class ColumnSpec {
        private final String name;
        private final String type;
        private final String comment;

        private ColumnSpec(String name, String type, String comment) {
            this.name = name;
            this.type = type;
            this.comment = comment;
        }

        private String toSparkSql() {
            StringBuilder builder = new StringBuilder();
            builder.append(quoteIdentifier(name)).append(" ").append(type);
            if (comment != null && !comment.isEmpty()) {
                builder.append(" COMMENT '").append(escapeSqlString(comment)).append("'");
            }
            return builder.toString();
        }
    }

    private static final class QualifiedName {
        private final String databaseName;
        private final String tableName;

        private QualifiedName(String databaseName, String tableName) {
            this.databaseName = databaseName == null || databaseName.trim().isEmpty()
                    ? "default"
                    : databaseName.trim();
            this.tableName = Objects.requireNonNull(tableName, "tableName").trim();
        }

        private static QualifiedName parse(String rawIdentifier) {
            if (rawIdentifier == null) {
                return null;
            }
            String cleaned = stripQuotes(rawIdentifier).trim();
            if (cleaned.isEmpty()) {
                return null;
            }
            String[] parts = cleaned.split("\\.");
            if (parts.length == 1) {
                return new QualifiedName("default", parts[0]);
            }
            StringBuilder database = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                if (i > 0) {
                    database.append('.');
                }
                database.append(parts[i]);
            }
            return new QualifiedName(database.toString(), parts[parts.length - 1]);
        }

        private String asQuotedString() {
            return quoteIdentifier(databaseName) + "." + quoteIdentifier(tableName);
        }

        private String asUnquotedString() {
            return databaseName + "." + tableName;
        }
    }
}
