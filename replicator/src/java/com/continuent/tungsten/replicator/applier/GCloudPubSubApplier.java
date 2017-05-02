package com.continuent.tungsten.replicator.applier;


import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.datasource.CommitSeqno;
import com.continuent.tungsten.replicator.datasource.CommitSeqnoAccessor;
import com.continuent.tungsten.replicator.datasource.UniversalConnection;
import com.continuent.tungsten.replicator.datasource.UniversalDataSource;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.gcloud.GCloudService;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.Joiner;
import com.google.common.net.MediaType;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class GCloudPubSubApplier implements RawApplier {

    private static Logger logger = Logger.getLogger(GCloudPubSubApplier.class);

    protected int taskId = 0;
    protected ReplicatorRuntime runtime = null;
    protected String dataSource = null;

    protected String metadataSchema = null;
    protected String consistencyTable = null;
    protected String consistencySelect = null;
    protected Pattern ignoreSessionPattern = null;

    protected UniversalConnection conn = null;
    protected CommitSeqno commitSeqno = null;
    protected CommitSeqnoAccessor commitSeqnoAccessor = null;

    private ReplDBMSHeader lastProcessedEvent = null;


    private String credentialsFile = "";
    private String topicUrl = "";
    private URI publishUri;


    private ObjectMapper objectMapper = null;

    private GCloudService gCloudService;

    private ArrayNode rowChangeData;

    private String directory;

    public String getTopicUrl() {
        return topicUrl;
    }

    public void setTopicUrl(String topicUrl) {
        this.topicUrl = topicUrl;
        try {
            this.publishUri = new URI(topicUrl + ":publish");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public String getCredentialsFile() {
        return credentialsFile;
    }

    public void setCredentialsFile(String credentialsFile) {
        this.credentialsFile = credentialsFile;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.applier.RawApplier#setTaskId(int)
     */
    public void setTaskId(int id) {
        this.taskId = id;
        if (logger.isDebugEnabled())
            logger.debug("Set task id: id=" + taskId);
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit,
                      boolean doRollback) throws ReplicatorException,
            ConsistencyException, InterruptedException {

        // Ensure we are not trying to apply a previously applied event. This
        // case can arise during restart.
        if (lastProcessedEvent != null && lastProcessedEvent.getLastFrag()
                && lastProcessedEvent.getSeqno() >= header.getSeqno()
                && !(event instanceof DBMSEmptyEvent)) {
            logger.info("Skipping over previously applied event: seqno="
                    + header.getSeqno() + " fragno=" + header.getFragno());
            return;
        }
        if (logger.isDebugEnabled())
            logger.debug("Applying event: seqno=" + header.getSeqno()
                    + " fragno=" + header.getFragno() + " commit=" + doCommit);

        long seqNo = header.getSeqno();

        ArrayList<DBMSData> dbmsDataValues = event.getData();

        if (rowChangeData == null) {
            rowChangeData = objectMapper.createArrayNode();
        }

        // Iterate through values inferring the database name.
        for (DBMSData dbmsData : dbmsDataValues) {
            if (dbmsData instanceof StatementData) {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring statement");
            } else if (dbmsData instanceof RowChangeData) {
                RowChangeData rd = (RowChangeData) dbmsData;

                for (OneRowChange orc : rd.getRowChanges()) {
                    // Get the action as well as the schema & table name.
                    RowChangeData.ActionType action = orc.getAction();
                    String schema = orc.getSchemaName();
                    String table = orc.getTableName();
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing row : action=" + action
                                + " schema=" + schema + " table=" + table);
                    }


                    if (action.equals(RowChangeData.ActionType.INSERT)) {
                        // Fetch column names.
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();

                        // Make a document and insert for each row.
                        Iterator<ArrayList<ColumnVal>> colValues = orc
                                .getColumnValues().iterator();
                        while (colValues.hasNext()) {
                            ObjectNode doc = objectMapper.createObjectNode();
                            ArrayList<ColumnVal> row = colValues.next();
                            for (int i = 0; i < row.size(); i++) {
                                Object value = row.get(i).getValue();
                                setValue(doc, colSpecs.get(i), value);
                            }
                            if (logger.isDebugEnabled()) {
                                logger.debug("Adding document: doc="
                                        + doc.toString());
                            }
                            ObjectNode oneRowChangeJson = objectMapper.createObjectNode();
                            oneRowChangeJson.put("database", orc.getSchemaName());
                            oneRowChangeJson.put("table", orc.getTableName());
                            oneRowChangeJson.put("ts", seqNo);
                            oneRowChangeJson.put("type", action.toString());
                            oneRowChangeJson.put("table_id", orc.getTableId());
                            oneRowChangeJson.put("data", doc);
                            rowChangeData.add(oneRowChangeJson);
                        }
                    } else if (action.equals(RowChangeData.ActionType.UPDATE)) {

                        // Fetch key and column names.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc
                                .getKeyValues();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc
                                .getColumnValues();

                        // Iterate across the rows.
                        for (int row = 0; row < columnValues.size()
                                || row < keyValues.size(); row++) {
                            List<ColumnVal> keyValuesOfRow = keyValues.get(row);
                            List<ColumnVal> colValuesOfRow = columnValues
                                    .get(row);

                            // Prepare key values query to search for rows.
                            ObjectNode old = objectMapper.createObjectNode();
                            for (int i = 0; i < keyValuesOfRow.size(); i++) {
                                setValue(old, keySpecs.get(i), keyValuesOfRow
                                        .get(i).getValue());
                            }

                            ObjectNode doc = objectMapper.createObjectNode();
                            for (int i = 0; i < colValuesOfRow.size(); i++) {
                                setValue(doc, colSpecs.get(i), colValuesOfRow
                                        .get(i).getValue());
                            }
                            if (logger.isDebugEnabled()) {
                                logger.debug("Updating document: query="
                                        + old + " doc=" + doc);
                            }
                            ObjectNode oneRowChangeJson = objectMapper.createObjectNode();
                            oneRowChangeJson.put("database", orc.getSchemaName());
                            oneRowChangeJson.put("table", orc.getTableName());
                            oneRowChangeJson.put("ts", seqNo);
                            oneRowChangeJson.put("type", action.toString());
                            oneRowChangeJson.put("table_id", orc.getTableId());
                            oneRowChangeJson.put("data", doc);
                            oneRowChangeJson.put("old", old);
                            rowChangeData.add(oneRowChangeJson);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Documented updated: doc="
                                        + doc);
                            }
                        }
                    } else if (action.equals(RowChangeData.ActionType.DELETE)) {

                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc
                                .getKeyValues();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc
                                .getColumnValues();

                        // Iterate across the rows.
                        for (int row = 0; row < columnValues.size()
                                || row < keyValues.size(); row++) {
                            List<ColumnVal> keyValuesOfRow = keyValues.get(row);

                            // Prepare key values query to search for rows.
                            ObjectNode old = objectMapper.createObjectNode();
                            for (int i = 0; i < keyValuesOfRow.size(); i++) {
                                setValue(old, keySpecs.get(i), keyValuesOfRow
                                        .get(i).getValue());
                            }

                            ObjectNode oneRowChangeJson = objectMapper.createObjectNode();
                            oneRowChangeJson.put("database", orc.getSchemaName());
                            oneRowChangeJson.put("table", orc.getTableName());
                            oneRowChangeJson.put("ts", seqNo);
                            oneRowChangeJson.put("type", action.toString());
                            oneRowChangeJson.put("table_id", orc.getTableId());
                            oneRowChangeJson.put("old", old);
                            rowChangeData.add(oneRowChangeJson);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Deleting document: query="
                                        + old);
                            }
                        }
                    } else {
                        logger.warn("Unrecognized action type: " + action);
                        return;
                    }

                }

            } else if (dbmsData instanceof LoadDataFileFragment) {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring load data file fragment");
            } else if (dbmsData instanceof RowIdData) {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring row ID data");
            } else {
                logger.warn("Unsupported DbmsData class: "
                        + dbmsData.getClass().getName());
            }
        }

        lastProcessedEvent = header;
        if (doCommit) {
            ArrayNode r = rowChangeData;
            rowChangeData = null;
            if (logger.isDebugEnabled()) {
                try {
                    logger.debug("commit " + (r != null ? objectMapper.writeValueAsString(r) : "null"));
                } catch (IOException e) {
                    throw new ReplicatorException(e);
                }
            }
            publish(r);
            commit();
        } else if (doRollback) {
            ArrayNode r = rowChangeData;
            rowChangeData = null;
            if (logger.isDebugEnabled()) {
                try {
                    logger.debug("rollback " + (r != null ? objectMapper.writeValueAsString(r) : "null"));
                } catch (IOException e) {
                    throw new ReplicatorException(e);
                }
            }
            commit();
        }
    }

    private void setValue(ObjectNode doc, ColumnSpec columnSpec, Object value)
            throws ReplicatorException {
        String name = columnSpec.getName();

        if (value == null) {
            doc.putNull(name);

        } else if (value instanceof Blob) {
            doc.put(name, deserializeBlob(name, (Blob) value));

        } else if (value instanceof Clob) {
            doc.put(name, deserializeClob(name, (Clob) value));

        } else if (value instanceof Timestamp) {
            doc.put(name, FastDateFormat.getInstance("yyyyMMdd'T'HHmmss.SSSZ").format((Timestamp) value));

        } else if (value instanceof Time) {
            doc.put(name, FastDateFormat.getInstance("HHmmss.SSSZ").format((Time) value));

        } else if (value instanceof java.sql.Date) {
            doc.put(name, FastDateFormat.getInstance("yyyyMMdd'T'HHmmss.SSSZ").format((java.sql.Date) value));

        } else if (value instanceof Boolean) {
            doc.put(name, (Boolean) value);

        } else if (value instanceof String) {
            doc.put(name, (String) value);

        } else if (value instanceof byte[]) {
            doc.put(name, (byte[]) value);

        } else if (value instanceof BigDecimal) {
            doc.put(name, (BigDecimal) value);

        } else if (value instanceof BigInteger) {
            doc.put(name, ((BigInteger) value).doubleValue());

        } else if (value instanceof Double) {
            doc.put(name, (Double) value);

        } else if (value instanceof Float) {
            doc.put(name, (Float) value);

        } else if (value instanceof Integer) {
            doc.put(name, (Integer) value);

        } else if (value instanceof Long) {
            doc.put(name, (Long) value);

        } else {
            doc.put(name, value.toString());
        }
    }

    private byte[] deserializeBlob(String name, Blob blob)
            throws ReplicatorException {
        try {
            long length = blob.length();
            if (length > 0) {
                // Try to deserialize.
                return blob.getBytes(1, (int) length);
            } else {
                // The blob is empty, so just return an empty string.
                return new byte[0];
            }
        } catch (SQLException e) {
            throw new ReplicatorException(
                    "Unable to deserialize blob value: column=" + name, e);
        }
    }

    private String deserializeClob(String name, Clob blob)
            throws ReplicatorException {
        try {
            long length = blob.length();
            if (length > 0) {
                // Try to deserialize.
                return blob.getSubString(1, (int) length);
            } else {
                // The blob is empty, so just return an empty string.
                return "";
            }
        } catch (SQLException e) {
            throw new ReplicatorException(
                    "Unable to deserialize blob value: column=" + name, e);
        }
    }

    @Override
    public void commit() throws ReplicatorException, InterruptedException {
        // If there's nothing to commit, go back.
        if (this.lastProcessedEvent == null)
            return;

        // Add applied latency so that we can easily track how far back each
        // partition is. If we don't have data we just put in zero.
        long appliedLatency;
        if (lastProcessedEvent instanceof ReplDBMSEvent) {
            appliedLatency = (System.currentTimeMillis() - lastProcessedEvent.getExtractedTstamp().getTime()) / 1000;
        } else {
            appliedLatency = 0;
        }

        updateCommitSeqno(lastProcessedEvent, appliedLatency);
    }

    private void updateCommitSeqno(ReplDBMSHeader header, long appliedLatency)
            throws ReplicatorException, InterruptedException {
        if (commitSeqnoAccessor == null)
            return;
        else {
            if (logger.isDebugEnabled())
                logger.debug("Updating commit seqno to " + header.getSeqno());
            commitSeqnoAccessor.updateLastCommitSeqno(header, appliedLatency);
        }
    }


    private void publish(ArrayNode rowChangeData) throws ReplicatorException {

        try {
            ObjectNode root = objectMapper.createObjectNode();

            ObjectNode messageJson = objectMapper.createObjectNode();
            messageJson.put("data", objectMapper.writeValueAsBytes(rowChangeData));

            ArrayNode messagesJson = objectMapper.createArrayNode();
            messagesJson.add(messageJson);
            root.put("messages", messagesJson);

            String toSend = objectMapper.writeValueAsString(root);
            if (logger.isDebugEnabled()) {
                logger.debug("POST to " + publishUri.toString() + " Body " + toSend);
            }
            HttpResponse response = gCloudService.executePost(publishUri, MediaType.JSON_UTF_8.toString(), toSend.getBytes(StandardCharsets.UTF_8), 3, 30000, 30000);
            String entity = response.parseAsString();
            int responseCode = response.getStatusCode();
            if (responseCode < 200 || responseCode >= 300) {
                String dump = toString(response, entity);
                throw new ReplicatorException(publishUri.toString() + " returned " + dump);
            }
        } catch (Throwable e) {
            if (e instanceof ReplicatorException) {
                throw (ReplicatorException) e;
            } else {
                throw new ReplicatorException(e);
            }
        }
    }

    @Override
    public ReplDBMSHeader getLastEvent()
            throws ReplicatorException, InterruptedException {
        if (commitSeqnoAccessor == null)
            return null;
        else
            return commitSeqnoAccessor.lastCommitSeqno();
    }

    @Override
    public void rollback() throws InterruptedException {
    }


    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException {
        this.runtime = (ReplicatorRuntime) context;
    }

    @Override
    public void prepare(PluginContext context)
            throws ReplicatorException, InterruptedException {

        objectMapper = new ObjectMapper();

        try {

            // Establish a connection to the data source.
            logger.info("Connecting to data source");
            UniversalDataSource dataSourceImpl = context
                    .getDataSource(dataSource);
            if (dataSourceImpl == null) {
                throw new ReplicatorException(
                        "Unable to locate data source: name=" + dataSource);
            }

            // Create accessor that can update the trep_commit_seqno table.
            conn = dataSourceImpl.getConnection();
            commitSeqno = dataSourceImpl.getCommitSeqno();
            commitSeqnoAccessor = commitSeqno.createAccessor(taskId, conn);

            // Fetch the last processed event.
            lastProcessedEvent = commitSeqnoAccessor.lastCommitSeqno();

        } catch (Exception e) {
            String message = String.format(
                    "Unable to initialize applier: data source=" + dataSource);
            throw new ReplicatorException(message, e);
        }

        try {
            publishUri = new URI(topicUrl + ":publish");
            gCloudService = new GCloudService();
            gCloudService.setServiceAccountCredentialsFile(credentialsFile);
            gCloudService.start();
        } catch (Exception e) {
            throw new ReplicatorException(
                    "Unable to connect to GCloud: url="
                            + topicUrl
                            + ",credentials=" + credentialsFile, e);
        }
    }

    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException {
        if (commitSeqno != null) {
            commitSeqno.release();
            commitSeqnoAccessor = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }

        if (gCloudService != null) {
            gCloudService.stop();
            gCloudService = null;
        }
    }

    public static String toString(HttpResponse httpClientResponse, String response) {
        StringBuilder sb = new StringBuilder();
        sb.append("\r\nHttp Header Dump <<<<<\r\n\r\n");
        sb.append(String.format("HTTP/1.1 %d %s\r\n", httpClientResponse.getStatusCode(), httpClientResponse.getStatusMessage()));
        HttpHeaders headers = httpClientResponse.getHeaders();
        for (String headerName : headers.keySet()) {
            List<String> values = headers.getHeaderStringValues(headerName);
            sb.append(String.format("%s: %s\r\n", headerName, Joiner.on(',').join(values)));
        }
        sb.append("\r\n");
        sb.append("Http Header Dump <<<<<\r\n");
        sb.append("\r\nHttp Body Dump <<<<<\r\n\r\n");
        sb.append(response);
        sb.append("\r\n\r\nHttp Body Dump <<<<<\r\n");
        return sb.toString();
    }
}