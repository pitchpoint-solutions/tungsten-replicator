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
import com.google.api.client.util.PemReader;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.net.MediaType;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.log4j.Logger;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.io.CipherOutputStream;
import org.bouncycastle.crypto.modes.GCMBlockCipher;
import org.bouncycastle.crypto.params.AEADParameters;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import javax.crypto.Cipher;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.spec.X509EncodedKeySpec;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

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
    private boolean compress = true;
    private String publicKeyFile;
    private String publicKeyId;
    private SecureRandom secureRandom;
    private PublicKey publicKey;
    private byte[] nonce;
    private String nonceAsString;
    private static final String KEY_ENC_ALG = "RSA/ECB/PKCS1Padding";
    private static final String DATA_ENC_ALG = "AES/GCM/NoPadding";


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

    public boolean isCompress() {
        return compress;
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    public String getPublicKeyFile() {
        return publicKeyFile;
    }

    public void setPublicKeyFile(String publicKeyFile) {
        this.publicKeyFile = publicKeyFile;
    }

    public String getPublicKeyId() {
        return publicKeyId;
    }

    public void setPublicKeyId(String publicKeyId) {
        this.publicKeyId = publicKeyId;
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


                    if (RowChangeData.ActionType.INSERT.equals(action)
                            || RowChangeData.ActionType.UPDATE.equals(action)
                            || RowChangeData.ActionType.DELETE.equals(action)) {

                        ArrayList<ColumnSpec> keySpecs = orc.getKeySpec();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc.getKeyValues();

                        ArrayNode keyRowColumnsJson = objectMapper.createArrayNode();
                        for (ArrayList<OneRowChange.ColumnVal> key : keyValues) {
                            int size = key.size();
                            for (int i = 0; i < size; i++) {
                                ColumnSpec spec = keySpecs.get(i);
                                ColumnVal columnVal = key.get(i);

                                ObjectNode rowColumnJson = objectMapper.createObjectNode();
                                rowColumnJson.put("index", spec.getIndex());
                                rowColumnJson.put("length", spec.getLength());
                                rowColumnJson.put("name", spec.getName());
                                rowColumnJson.put("type", spec.getType());
                                rowColumnJson.put("type_description", spec.getTypeDescription());
                                rowColumnJson.put("is_blob", spec.isBlob());
                                rowColumnJson.put("is_not_null", spec.isNotNull());
                                rowColumnJson.put("is_unsigned", spec.isUnsigned());
                                setValue(rowColumnJson, "value", columnVal.getValue());
                                keyRowColumnsJson.add(rowColumnJson);
                            }
                        }


                        ArrayList<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc.getColumnValues();

                        ArrayNode dataRowColumnsJson = objectMapper.createArrayNode();
                        for (ArrayList<OneRowChange.ColumnVal> changes : columnValues) {
                            int size = changes.size();
                            for (int i = 0; i < size; i++) {
                                ColumnSpec spec = colSpecs.get(i);
                                ColumnVal columnVal = changes.get(i);

                                ObjectNode rowColumnJson = objectMapper.createObjectNode();
                                rowColumnJson.put("index", spec.getIndex());
                                rowColumnJson.put("length", spec.getLength());
                                rowColumnJson.put("name", spec.getName());
                                rowColumnJson.put("type", spec.getType());
                                rowColumnJson.put("type_description", spec.getTypeDescription());
                                rowColumnJson.put("is_blob", spec.isBlob());
                                rowColumnJson.put("is_not_null", spec.isNotNull());
                                rowColumnJson.put("is_unsigned", spec.isUnsigned());
                                setValue(rowColumnJson, "value", columnVal.getValue());
                                dataRowColumnsJson.add(rowColumnJson);
                            }
                        }

                        ObjectNode oneRowChangeJson = objectMapper.createObjectNode();
                        oneRowChangeJson.put("database_name", orc.getSchemaName());
                        oneRowChangeJson.put("table_name", orc.getTableName());
                        oneRowChangeJson.put("seq_no", seqNo);
                        oneRowChangeJson.put("action", action.toString());
                        oneRowChangeJson.put("table_id", orc.getTableId());
                        oneRowChangeJson.put("key_data", keyRowColumnsJson);
                        oneRowChangeJson.put("column_data", dataRowColumnsJson);
                        rowChangeData.add(oneRowChangeJson);

                        if (logger.isDebugEnabled()) {
                            logger.debug("document: " + oneRowChangeJson);
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

    private void setValue(ObjectNode doc, String name, Object value)
            throws ReplicatorException {

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

            ObjectNode attributesJson = objectMapper.createObjectNode();

            byte[] data = objectMapper.writeValueAsBytes(rowChangeData);
            if (compress) {
                data = gzip(data);
                attributesJson.put("compressed", "true");
                attributesJson.put("compression_format", "gzip");
            }

            if (publicKeyFile != null) {
                try {
                    byte[] key = new byte[32];
                    secureRandom.nextBytes(key);

                    Cipher cipher = Cipher.getInstance(KEY_ENC_ALG, "BC");
                    cipher.init(Cipher.ENCRYPT_MODE, publicKey);
                    byte[] encryptedKey = cipher.doFinal(key);

                    Arrays.fill(key, (byte) 0);

                    GCMBlockCipher gcmBlockCipher = new GCMBlockCipher(new AESEngine());
                    gcmBlockCipher.init(true, new AEADParameters(new KeyParameter(key), 128, nonce));

                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    try (CipherOutputStream cipherOutputStream = new CipherOutputStream(byteArrayOutputStream, gcmBlockCipher)) {
                        cipherOutputStream.write(data);
                    }

                    data = byteArrayOutputStream.toByteArray();

                    attributesJson.put("encrypted", "true");
                    attributesJson.put("cipher_key_alg", KEY_ENC_ALG);
                    attributesJson.put("cipher_key_id", publicKeyId);
                    attributesJson.put("cipher_data_key", BaseEncoding.base64().encode(encryptedKey));
                    attributesJson.put("cipher_data_iv", nonceAsString);
                    attributesJson.put("cipher_data_alg", DATA_ENC_ALG);

                } catch (Throwable e) {
                    throw new ReplicatorException(
                            "Unable to encrypt using RSA: pem=" + publicKeyFile, e);
                }
            }

            ObjectNode messageJson = objectMapper.createObjectNode();
            messageJson.put("data", data);
            messageJson.put("attributes", attributesJson);

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
                throw new ReplicatorException(publishUri.toString() + " request " + toSend + " returned " + dump);
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

        Security.addProvider(new BouncyCastleProvider());

        if (!StringUtils.isBlank(publicKeyFile)) {
            publicKey = loadPublicKey();
            if (StringUtils.isBlank(publicKeyId)) {
                throw new ReplicatorException(
                        "Unable to initialize cipher. publicKeyId is required");
            }
            secureRandom = new SecureRandom();
            nonce = new byte[32];
            secureRandom.nextBytes(nonce);
            nonceAsString = BaseEncoding.base64().encode(nonce);
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

    private byte[] gzip(byte[] data) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
            gzipOutputStream.write(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteArrayOutputStream.toByteArray();
    }

    private PublicKey loadPublicKey() throws ReplicatorException {
        try {
            try (FileReader reader = new FileReader(publicKeyFile)) {
                PemReader pemReader = new PemReader(reader);
                PemReader.Section section = pemReader.readNextSection("PUBLIC KEY");
                Preconditions.checkNotNull(section, "PUBLIC KEY section not found in " + publicKeyFile);
                byte[] bytes = section.getBase64DecodedBytes();
                X509EncodedKeySpec keySpec = new X509EncodedKeySpec(bytes);
                KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                return keyFactory.generatePublic(keySpec);
            }
        } catch (Throwable e) {
            throw new ReplicatorException(
                    "Unable to load pem: file=" + publicKeyFile, e);
        }
    }
}