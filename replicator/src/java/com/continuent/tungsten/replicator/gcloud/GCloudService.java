package com.continuent.tungsten.replicator.gcloud;


import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;

public class GCloudService {

    private static Logger logger = Logger.getLogger(GCloudService.class);
    private HttpTransport httpTransport;
    private JsonFactory jsonFactory;
    private String serviceAccountCredentialsFile;
    private GoogleCredential credential;
    private HttpRequestFactory httpRequestFactory;

    public String getServiceAccountCredentialsFile() {
        return serviceAccountCredentialsFile;
    }

    public void setServiceAccountCredentialsFile(String serviceAccountCredentialsFile) {
        this.serviceAccountCredentialsFile = serviceAccountCredentialsFile;
    }

    public void start() {
        try {
            Preconditions.checkState(!StringUtils.isBlank(serviceAccountCredentialsFile), "serviceAccountCredentialsFile is required");
            Preconditions.checkState(Files.exists(Paths.get(serviceAccountCredentialsFile)), "Credentials file " + serviceAccountCredentialsFile + " not found");
            httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            jsonFactory = JacksonFactory.getDefaultInstance();
            try (FileInputStream fileInputStream = new FileInputStream(serviceAccountCredentialsFile)) {
                credential = GoogleCredential.fromStream(fileInputStream, httpTransport, jsonFactory);
            }
            httpRequestFactory = httpTransport.createRequestFactory(credential);
        } catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        if (httpTransport != null) {
            try {
                httpTransport.shutdown();
            } catch (IOException e) {
                logger.warn("Handling Exception", e);
            }
        }
    }

    public HttpResponse executePost(URI uri, String type, byte[] data, int retries, int connectTimeout, int readTimeout) throws IOException {
        ByteArrayContent byteArrayContent = new ByteArrayContent(type, data);
        GenericUrl url = new GenericUrl(uri);
        HttpRequest request =
                httpRequestFactory.buildPostRequest(url, byteArrayContent)
                        .setConnectTimeout(connectTimeout)
                        .setReadTimeout(readTimeout)
                        .setNumberOfRetries(retries);
        return request.execute();
    }
}
