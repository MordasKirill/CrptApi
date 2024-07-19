package com.сrpt.turnover;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.*;

@Slf4j
public class CrptApi {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Semaphore semaphore;
    private final int requestLimit;
    private final String apiUrl;

    /**
     * Constructor
     * Initializes HttpClient to make HTTP requests.
     * Initializes ObjectMapper to serialize objects to JSON.
     * Initializes the ScheduledExecutorService to reset the request counter.
     * Sets a limit on the number of requests in a specified time interval.
     * Initializes the Semaphore with num of permits
     *
     * @param config Crpt Api Config
     */
    public CrptApi(CrptApiConfig config) {
        try(ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1)) {
            this.httpClient = HttpClient.newHttpClient();
            this.objectMapper = new ObjectMapper();
            this.requestLimit = config.requestLimit();
            long timeUnitMillis = config.timeUnit().toMillis(1);
            this.apiUrl = config.apiUrl();
            this.semaphore = new Semaphore(requestLimit, true);

            scheduler.scheduleAtFixedRate(this::resetSemaphore, timeUnitMillis, timeUnitMillis, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Restores the number of available permissions to the
     * requestLimit value at regular intervals.
     */
    private void resetSemaphore() {
        int permitsToRelease = requestLimit - semaphore.availablePermits();
        semaphore.release(permitsToRelease);
        log.debug("Semaphore reset: released " + permitsToRelease + " permits");
    }

    /**
     * Creating a document for putting into circulation goods produced
     * in the Russian Federation, by calling ismp.crpt.ru API
     *
     * @param document the document to be sent to API
     * @param signature signature of the document
     * @throws InterruptedException in case thread is interrupted
     * @throws IOException in case of document creation failure (response code != 200)
     */
    public void createDocument(Document document, String signature) throws InterruptedException, IOException {
        semaphore.acquire();
        log.debug("Acquired permit. Available permits: " + semaphore.availablePermits());

        try {
            HttpResponse<String> response =
                    httpClient.send(getHttpRequest(document, signature), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IOException("Failed to create document: " + response.body());
            }
            log.info("Document created successfully: " + response.body());
        } catch (IOException | InterruptedException e) {
            log.error("Failed to create document", e);
            throw e;
        } finally {
            semaphore.release();
            log.debug("Released permit. Available permits: " + semaphore.availablePermits());
        }
    }

    /**
     * Get http request with json body and headers
     *
     * @param object object to be converted to json and set as body
     * @param signature signature to be added as a header
     * @return HttpRequest with body and Http headers
     * @throws JsonProcessingException in case of issue while converting object to json
     */
    private HttpRequest getHttpRequest(Object object, String signature) throws JsonProcessingException {
        String jsonBody = objectMapper.writeValueAsString(object);
        return HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .header("Content-Type", "application/json")
                .header("Signature", signature)
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
    }

    public record CrptApiConfig(TimeUnit timeUnit, int requestLimit, String apiUrl) {}

    @Data
    public static class Document {
        @JsonProperty("description")
        public Description description;
        @JsonProperty("doc_id")
        public String docId;
        @JsonProperty("doc_status")
        public String docStatus;
        @JsonProperty("doc_type")
        public String docType;
        @JsonProperty("importRequest")
        public boolean importRequest;
        @JsonProperty("owner_inn")
        public String ownerInn;
        @JsonProperty("participant_inn")
        public String participantInn;
        @JsonProperty("producer_inn")
        public String producerInn;
        @JsonProperty("production_date")
        public String productionDate;
        @JsonProperty("production_type")
        public String productionType;
        @JsonProperty("products")
        public Product[] products;
        @JsonProperty("reg_date")
        public String regDate;
        @JsonProperty("reg_number")
        public String regNumber;
    }

    @Data
    public static class Description {
        @JsonProperty("participantInn")
        public String participantInn;
    }

    @Data
    public static class Product {
        @JsonProperty("certificate_document")
        public String certificateDocument;
        @JsonProperty("certificate_document_date")
        public String certificateDocumentDate;
        @JsonProperty("certificate_document_number")
        public String certificateDocumentNumber;
        @JsonProperty("owner_inn")
        public String ownerInn;
        @JsonProperty("producer_inn")
        public String producerInn;
        @JsonProperty("production_date")
        public String productionDate;
        @JsonProperty("tnved_code")
        public String tnvedCode;
        @JsonProperty("uit_code")
        public String uitCode;
        @JsonProperty("uitu_code")
        public String uituCode;
    }

    /**
     * Get product object
     *
     * @return product object
     */
    private static Product getProduct() {
        Product product = new Product();
        product.certificateDocument = "cert123";
        product.certificateDocumentDate = "2024-07-15";
        product.certificateDocumentNumber = "certNumber";
        product.ownerInn = "1234567890";
        product.producerInn = "1234567890";
        product.productionDate = "2024-07-18";
        product.tnvedCode = "123456";
        product.uitCode = "uitCode123";
        product.uituCode = "uituCode123";
        return product;
    }

    /**
     * Get Document object
     *
     * @return Document object
     */
    private static Document getDocument() {
        Document document = new Document();
        document.description = new Description();
        document.description.participantInn = "123456789";
        document.docId = "doc123";
        document.docStatus = "NEW";
        document.docType = "LP_INTRODUCE_GOODS";
        document.importRequest = true;
        document.ownerInn = "1234567891";
        document.participantInn = "0987654321";
        document.producerInn = "1234567896";
        document.productionDate = "2024-07-17";
        document.productionType = "TYPE1";
        document.regDate = "2024-07-18";
        document.regNumber = "reg123";
        Product product = getProduct();
        document.products = new Product[]{product};
        return document;
    }

    /**
     * To showcase the flow of document creation
     *
     * @param args arguments
     */
    public static void main(String[] args) {
        // 5 requests in a second allowed
        CrptApiConfig config = new CrptApiConfig(TimeUnit.SECONDS, 5, "https://ismp.crpt.ru/api/v3/lk/documents/create");
        CrptApi crptApi = new CrptApi(config);
        String signature = "signature123";
        Document document = getDocument();
        try {
            crptApi.createDocument(document, signature);
            log.info("Document created successfully.");
        } catch (InterruptedException | IOException e) {
            log.info("Failed to create document: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}

