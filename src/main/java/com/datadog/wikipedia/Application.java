package com.datadog.wikipedia;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.storage.StorageLevel.OFF_HEAP;

import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final int TIMEOUT = 15000;
    private static final String OUTPUT_FOLDER = "output";
    private static final String TEMP_FOLDER = "temp";
    private static final String BLACKLIST_FILE_NAME = "blacklist_domains_and_pages";
    private static final String WIKI_DOWNLOAD_URL_TEMPLATE = "https://dumps.wikimedia.org/other/pageviews/#{year}/#{year}-#{month}/pageviews-#{isoDate}-#{hhmmss}.gz";
    private static final String DOMAIN_CODE = "domain_code";
    private static final String PAGE_TITLE = "page_title";
    private static final String COUNT_VIEWS = "count_views";
    private static final String TOTAL_RESPONSE_SIZE = "total_response_size";

    private final SparkSession session;
    private final LocalDateTime startTime;
    private final LocalDateTime endTime;

    @Autowired
    public Application(
        @Value("${spark.master:local[*]}") String master,
        @Value("${startTime}") String startTime,
        @Value("${endTime:}") String endTime
    ) {
        this.session = SparkSession.builder().master(master).getOrCreate();
        this.startTime = LocalDateTime.parse(startTime);
        this.endTime = endTime.isEmpty() ? this.startTime : LocalDateTime.parse(endTime);
        validateDates();
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        disableSSLCertificateCheck();

        Dataset<Row> blacklistDF = createWikiDataset(session, BLACKLIST_FILE_NAME);
        blacklistDF.cache();

        for (LocalDateTime currentTime = startTime; !currentTime.isAfter(endTime); currentTime = currentTime.plusHours(1)) {

            String outputFile = createOutputFilePath(currentTime);

            if (new File(outputFile).exists()) {
                System.out.println("Output file exists for " + currentTime.toString());
                return;
            }

            String wikiDownloadUrl = createDownloadUrl(startTime);
            String gzFilePath = createZippedFilePath(startTime);

            // There is a potential race condition here, if multiple jobs for the same date/hour are started at the same time.
            // Since there is time between checking if the file exists, and writing the file,
            // two jobs could end up writing to the same file at the same time, causing data corruption
            // I'm ok not solving for this, because this is an internal job, and we presumably
            // don't need to worry about this situation happening

            try {
                FileUtils.copyURLToFile(new URL(wikiDownloadUrl), new File(gzFilePath), TIMEOUT, TIMEOUT);
            } catch (IOException e) {
                System.err.println("Error download wiki file from " + wikiDownloadUrl + " to " + gzFilePath);
                e.printStackTrace();
            }

            Dataset<Row> pageViewDF = createWikiDataset(session, gzFilePath);

            // left_anti join to remove items from the blacklist. A plain filter would be preferable,
            // but the number of columns is different
            // https://stackoverflow.com/questions/39887526/filter-spark-dataframe-based-on-another-dataframe-that-specifies-blacklist-crite

            // broadcasting the blacklistDF will theoretically improve join performance
            // but I didn't see much improvement in practice
            Dataset<Row> filteredPageViewDF = pageViewDF.join(broadcast(blacklistDF),
                    convertListToSeq(Arrays.asList(DOMAIN_CODE, PAGE_TITLE)), "left_anti");

            WindowSpec rank = Window.partitionBy(DOMAIN_CODE).orderBy(desc(COUNT_VIEWS));
            Dataset<Row> results = filteredPageViewDF
                    .withColumn("rank", functions.row_number().over(rank))
                    .where("rank <= 25");

            // This forces exactly 1 partition, thus 1 output file.
            // This seems OK, given the size of the data (hundreds of MBs)
            // The output file name is not optimal, due to an oddity of spark, but I have chosen to live with it
            // ***I would not "live with it" if this were production
            results.coalesce(1).write().csv(outputFile);

            FileUtils.forceDelete(new File(gzFilePath));

        }
        this.session.close();
    }

    private void validateDates() {
        LocalDateTime firstAvailableDataDate = LocalDateTime.of(2015, 5, 1, 1, 0);
        if (this.startTime.isAfter(this.endTime)) {
            throw new RuntimeException("startTime cannot be after endTime");
        } else if (this.startTime.isAfter(LocalDateTime.now()) || this.endTime.isAfter(LocalDateTime.now())) {
            throw new RuntimeException("startTime and endTime must both be in the past");
        } else if (this.startTime.isBefore(firstAvailableDataDate) || this.endTime.isBefore(firstAvailableDataDate)) {
            throw new RuntimeException("startTime and endTime must both be after 05/01/2015, due to Wikipedia data limitation");
        }
    }

    private Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    private Dataset<Row> createWikiDataset(SparkSession session, String path) {
        // data file has 4 columns, blacklist file has 2 columns, but
        // both files begin with domain_code and page_title, so this works in both cases
        // no extra columns are added to the blacklist file

        // specifying .format("csv") can read in .gz and .csv files
        return session.sqlContext().read().format("csv")
                .option("delimiter", " ").option("inferSchema", true).load(path)
                .withColumnRenamed("_c0", DOMAIN_CODE)
                .withColumnRenamed("_c1", PAGE_TITLE)
                .withColumnRenamed("_c2", COUNT_VIEWS)
                .withColumnRenamed("_c3", TOTAL_RESPONSE_SIZE);
    }

    private void disableSSLCertificateCheck() {
        // I was getting an SSL exception when trying to download files from code
        // I tried adding it to the list of trusted certificates (keystore/truststore)
        // but had no luck. I'm ok with this "disabling SSL check" approach, as I currently trust dumps.wikipedia
        TrustManager[] trustAllCerts = new TrustManager[] {
            new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException { }

                public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException { }

                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            }
        };

        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private String createZippedFilePath(LocalDateTime dateTime) {
        return String.join("/", TEMP_FOLDER, dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd-HH"))) + ".gz";
    }

    private String createOutputFilePath(LocalDateTime dateTime) {
        return String.join("/", OUTPUT_FOLDER, dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd-HH")));
    }

    private String createDownloadUrl(LocalDateTime dateTime) {
        return WIKI_DOWNLOAD_URL_TEMPLATE
            .replace("#{year}", String.valueOf(dateTime.getYear()))
            .replace("#{month}", dateTime.toLocalDate().format(DateTimeFormatter.ofPattern("MM")))
            .replace("#{isoDate}", dateTime.toLocalDate().format(DateTimeFormatter.BASIC_ISO_DATE))
            .replace("#{hhmmss}", dateTime.toLocalTime().format(DateTimeFormatter.ofPattern("HHmmss")));
    }
}
