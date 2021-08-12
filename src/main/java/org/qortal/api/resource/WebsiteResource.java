package org.qortal.api.resource;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.api.ApiError;
import org.qortal.api.ApiExceptionFactory;
import org.qortal.api.HTMLParser;
import org.qortal.api.Security;
import org.qortal.block.BlockChain;
import org.qortal.crypto.Crypto;
import org.qortal.data.PaymentData;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.data.transaction.ArbitraryTransactionData.*;
import org.qortal.data.transaction.BaseTransactionData;
import org.qortal.group.Group;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.storage.DataFile;
import org.qortal.storage.DataFile.*;
import org.qortal.storage.DataFileReader;
import org.qortal.storage.DataFileWriter;
import org.qortal.transaction.ArbitraryTransaction;
import org.qortal.transaction.Transaction;
import org.qortal.transform.TransformationException;
import org.qortal.transform.transaction.ArbitraryTransactionTransformer;
import org.qortal.utils.Base58;
import org.qortal.utils.NTP;


@Path("/site")
@Tag(name = "Website")
public class WebsiteResource {

    private static final Logger LOGGER = LogManager.getLogger(WebsiteResource.class);

    @Context HttpServletRequest request;
    @Context HttpServletResponse response;
    @Context ServletContext context;

    @POST
    @Path("/upload/creator/{publickey}")
    @Operation(
            summary = "Build raw, unsigned, ARBITRARY transaction, based on a user-supplied path to a static website",
            requestBody = @RequestBody(
                    required = true,
                    content = @Content(
                            mediaType = MediaType.TEXT_PLAIN,
                            schema = @Schema(
                                    type = "string", example = "/Users/user/Documents/MyStaticWebsite"
                            )
                    )
            ),
            responses = {
                    @ApiResponse(
                            description = "raw, unsigned, ARBITRARY transaction encoded in Base58",
                            content = @Content(
                                    mediaType = MediaType.TEXT_PLAIN,
                                    schema = @Schema(
                                            type = "string"
                                    )
                            )
                    )
            }
    )
    public String uploadWebsite(@PathParam("publickey") String creatorPublicKeyBase58, String path) {
        Security.checkApiCallAllowed(request);

        // It's too dangerous to allow user-supplied filenames in weaker security contexts
        if (Settings.getInstance().isApiRestricted()) {
            throw ApiExceptionFactory.INSTANCE.createException(request, ApiError.NON_PRODUCTION);
        }

        if (creatorPublicKeyBase58 == null || path == null) {
            throw ApiExceptionFactory.INSTANCE.createException(request, ApiError.INVALID_CRITERIA);
        }
        byte[] creatorPublicKey = Base58.decode(creatorPublicKeyBase58);

        String name = null;
        ArbitraryTransactionData.Method method = ArbitraryTransactionData.Method.PUT;
        ArbitraryTransactionData.Service service = ArbitraryTransactionData.Service.WEBSITE;
        ArbitraryTransactionData.Compression compression = ArbitraryTransactionData.Compression.ZIP;

        DataFileWriter dataFileWriter = new DataFileWriter(Paths.get(path), method, compression);
        try {
            dataFileWriter.save();
        } catch (IOException e) {
            throw ApiExceptionFactory.INSTANCE.createException(request, ApiError.REPOSITORY_ISSUE);
        } catch (IllegalStateException e) {
            throw ApiExceptionFactory.INSTANCE.createException(request, ApiError.INVALID_DATA);
        }

        DataFile dataFile = dataFileWriter.getDataFile();
        if (dataFile == null) {
            throw ApiExceptionFactory.INSTANCE.createException(request, ApiError.INVALID_DATA);
        }

        String digest58 = dataFile.digest58();
        if (digest58 == null) {
            LOGGER.error("Unable to calculate digest");
            throw ApiExceptionFactory.INSTANCE.createException(request, ApiError.INVALID_DATA);
        }
        
        try (final Repository repository = RepositoryManager.getRepository()) {

            final String creatorAddress = Crypto.toAddress(creatorPublicKey);
            final byte[] lastReference = repository.getAccountRepository().getLastReference(creatorAddress);

            final BaseTransactionData baseTransactionData = new BaseTransactionData(NTP.getTime(), Group.NO_GROUP,
                    lastReference, creatorPublicKey, BlockChain.getInstance().getUnitFee(), null);
            final int size = (int)dataFile.size();
            final int version = 5;
            final int nonce = 0;
            byte[] secret = dataFile.getSecret();
            final ArbitraryTransactionData.DataType dataType = ArbitraryTransactionData.DataType.DATA_HASH;
            final byte[] digest = dataFile.digest();
            final byte[] chunkHashes = dataFile.chunkHashes();
            final List<PaymentData> payments = new ArrayList<>();

            ArbitraryTransactionData transactionData = new ArbitraryTransactionData(baseTransactionData,
                    version, service, nonce, size, name, method,
                    secret, compression, digest, dataType, chunkHashes, payments);

            ArbitraryTransaction transaction = (ArbitraryTransaction) Transaction.fromData(repository, transactionData);
            transaction.computeNonce();

            Transaction.ValidationResult result = transaction.isValidUnconfirmed();
            if (result != Transaction.ValidationResult.OK) {
                dataFile.deleteAll();
                throw TransactionsResource.createTransactionInvalidException(request, result);
            }

            byte[] bytes = ArbitraryTransactionTransformer.toBytes(transactionData);
            return Base58.encode(bytes);

        } catch (TransformationException e) {
            dataFile.deleteAll();
            throw ApiExceptionFactory.INSTANCE.createException(request, ApiError.TRANSFORMATION_ERROR, e);
        } catch (DataException e) {
            dataFile.deleteAll();
            throw ApiExceptionFactory.INSTANCE.createException(request, ApiError.REPOSITORY_ISSUE, e);
        }
    }

    @POST
    @Path("/preview")
    @Operation(
            summary = "Generate preview URL based on a user-supplied path to a static website",
            requestBody = @RequestBody(
                    required = true,
                    content = @Content(
                            mediaType = MediaType.TEXT_PLAIN,
                            schema = @Schema(
                                    type = "string", example = "/Users/user/Documents/MyStaticWebsite"
                            )
                    )
            ),
            responses = {
                    @ApiResponse(
                            description = "a temporary URL to preview the website",
                            content = @Content(
                                    mediaType = MediaType.TEXT_PLAIN,
                                    schema = @Schema(
                                            type = "string"
                                    )
                            )
                    )
            }
    )
    public String previewWebsite(String directoryPath) {
        Security.checkApiCallAllowed(request);

        // It's too dangerous to allow user-supplied filenames in weaker security contexts
        if (Settings.getInstance().isApiRestricted()) {
            throw ApiExceptionFactory.INSTANCE.createException(request, ApiError.NON_PRODUCTION);
        }

        Method method = Method.PUT;
        Compression compression = Compression.ZIP;

        DataFileWriter dataFileWriter = new DataFileWriter(Paths.get(directoryPath), method, compression);
        try {
            dataFileWriter.save();
        } catch (IOException e) {
            throw ApiExceptionFactory.INSTANCE.createException(request, ApiError.REPOSITORY_ISSUE);
        } catch (IllegalStateException e) {
            throw ApiExceptionFactory.INSTANCE.createException(request, ApiError.INVALID_DATA);
        }

        DataFile dataFile = dataFileWriter.getDataFile();
        if (dataFile != null) {
            String digest58 = dataFile.digest58();
            if (digest58 != null) {
                return "http://localhost:12393/site/hash/" + digest58 + "?secret=" + Base58.encode(dataFile.getSecret());
            }
        }
        return "Unable to generate preview URL";
    }

    @GET
    @Path("{signature}")
    public HttpServletResponse getIndexBySignature(@PathParam("signature") String signature) {
        return this.get(signature, ResourceIdType.SIGNATURE, "/", null,true);
    }

    @GET
    @Path("{signature}/{path:.*}")
    public HttpServletResponse getPathBySignature(@PathParam("signature") String signature, @PathParam("path") String inPath) {
        return this.get(signature, ResourceIdType.SIGNATURE, inPath,null,true);
    }

    @GET
    @Path("/hash/{hash}")
    public HttpServletResponse getIndexByHash(@PathParam("hash") String hash58, @QueryParam("secret") String secret58) {
        return this.get(hash58, ResourceIdType.FILE_HASH, "/", secret58,true);
    }

    @GET
    @Path("/hash/{hash}/{path:.*}")
    public HttpServletResponse getPathByHash(@PathParam("hash") String hash58, @PathParam("path") String inPath,
                                             @QueryParam("secret") String secret58) {
        return this.get(hash58, ResourceIdType.FILE_HASH, inPath, secret58,true);
    }

    @GET
    @Path("/domainmap")
    public HttpServletResponse getIndexByDomainMap() {
        return this.getDomainMap("/");
    }

    @GET
    @Path("/domainmap/{path:.*}")
    public HttpServletResponse getPathByDomainMap(@PathParam("path") String inPath) {
        return this.getDomainMap(inPath);
    }

    private HttpServletResponse getDomainMap(String inPath) {
        Map<String, String> domainMap = Settings.getInstance().getSimpleDomainMap();
        if (domainMap != null && domainMap.containsKey(request.getServerName())) {
            return this.get(domainMap.get(request.getServerName()), ResourceIdType.SIGNATURE, inPath, null, false);
        }
        return this.get404Response();
    }

    private HttpServletResponse get(String resourceId, ResourceIdType resourceIdType, String inPath, String secret58, boolean usePrefix) {
        if (!inPath.startsWith(File.separator)) {
            inPath = File.separator + inPath;
        }

        DataFileReader dataFileReader = new DataFileReader(resourceId, resourceIdType);
        dataFileReader.setSecret58(secret58); // Optional, used for loading encrypted file hashes only
        try {
            dataFileReader.load(false);
        } catch (Exception e) {
            return this.get404Response();
        }
        java.nio.file.Path path = dataFileReader.getFilePath();
        if (path == null) {
            return this.get404Response();
        }
        String unzippedPath = path.toString();

        try {
            String filename = this.getFilename(unzippedPath.toString(), inPath);
            String filePath = unzippedPath + File.separator + filename;

            if (HTMLParser.isHtmlFile(filename)) {
                // HTML file - needs to be parsed
                byte[] data = Files.readAllBytes(Paths.get(filePath)); // TODO: limit file size that can be read into memory
                HTMLParser htmlParser = new HTMLParser(resourceId, inPath, usePrefix);
                data = htmlParser.replaceRelativeLinks(filename, data);
                response.setContentType(context.getMimeType(filename));
                response.setContentLength(data.length);
                response.getOutputStream().write(data);
            }
            else {
                // Regular file - can be streamed directly
                File file = new File(filePath);
                FileInputStream inputStream = new FileInputStream(file);
                response.setContentType(context.getMimeType(filename));
                int bytesRead, length = 0;
                byte[] buffer = new byte[10240];
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    response.getOutputStream().write(buffer, 0, bytesRead);
                    length += bytesRead;
                }
                response.setContentLength(length);
                inputStream.close();
            }
            return response;
        } catch (FileNotFoundException | NoSuchFileException e) {
            LOGGER.info("File not found at path: {}", unzippedPath);
            if (inPath.equals("/")) {
                // Delete the unzipped folder if no index file was found
                try {
                    FileUtils.deleteDirectory(new File(unzippedPath));
                } catch (IOException ioException) {
                    LOGGER.info("Unable to delete directory: {}", unzippedPath, e);
                }
            }
        } catch (IOException e) {
            LOGGER.info("Unable to serve file at path: {}", inPath, e);
        }

        return this.get404Response();
    }

    private String getFilename(String directory, String userPath) {
        if (userPath == null || userPath.endsWith("/") || userPath.equals("")) {
            // Locate index file
            List<String> indexFiles = this.indexFiles();
            for (String indexFile : indexFiles) {
                String filePath = directory + File.separator + indexFile;
                if (Files.exists(Paths.get(filePath))) {
                    return userPath + indexFile;
                }
            }
        }
        return userPath;
    }

    private HttpServletResponse get404Response() {
        try {
            String responseString = "404: File Not Found";
            byte[] responseData = responseString.getBytes();
            response.setStatus(404);
            response.setContentLength(responseData.length);
            response.getOutputStream().write(responseData);
        } catch (IOException e) {
            LOGGER.info("Error writing 404 response");
        }
        return response;
    }

    private List<String> indexFiles() {
        List<String> indexFiles = new ArrayList<>();
        indexFiles.add("index.html");
        indexFiles.add("index.htm");
        indexFiles.add("default.html");
        indexFiles.add("default.htm");
        indexFiles.add("home.html");
        indexFiles.add("home.htm");
        return indexFiles;
    }

}
