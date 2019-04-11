package org.schambon.test;

import com.mongodb.Block;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static java.lang.Thread.sleep;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.schambon.test.Test.*;

public class Recovery implements Runnable {

    static final Logger logger = LoggerFactory.getLogger(Recovery.class);

    private final MongoCollection<Document> collection;
    private final MongoCollection<Document> txns;

    public Recovery(MongoCollection<Document> collection, MongoCollection<Document> txns) {
        this.collection = collection;
        this.txns = txns;
    }

    public static void main(String[] args) {
        MongoClient client = MongoClients.create(MONGODB_URI);
        MongoDatabase database = client.getDatabase("leshalles");
        MongoCollection<Document> collection = database.getCollection("testimport");
        MongoCollection<Document> txns = database.getCollection("txns");

        Thread thread = new Thread(new Recovery(collection, txns));
        thread.setDaemon(false); // we want to keep the JVM around
        thread.start();

        logger.info("Recovery process started");

    }

    @Override
    public void run() {

        while (true) {
            try {
                logger.info("Waking up");

                // roll back failed transactions
                txns.find(or(
                        and(eq("_id", Status.STARTED.toString()), lt(UPDATE, now().minus(2, MINUTES))),
                        eq(STATUS, Status.ROLLBACK.toString())
                )).forEach((Consumer<? super Document>) tx -> {
                    ObjectId txnId = tx.getObjectId("_id");
                    logger.info("Detected failed transaction {}, rolling it back", txnId.toString());
                    // mark txn as rollback
                    UpdateResult setTxnAsRollback = txns.updateOne(and(eq("_id", txnId), ne(STATUS, Status.ROLLBACK.toString())), combine(set(STATUS, Status.ROLLBACK.toString()), currentDate(UPDATE)));
                    logger.info("Set txn as rollback: modified {}", setTxnAsRollback.getModifiedCount());
                    DeleteResult deleteResult = collection.deleteMany(eq("txnId", txnId));
                    logger.info("Deleted {} documents", deleteResult.getDeletedCount());
                    UpdateResult setTxnAsDone = txns.updateOne(and(eq("_id", txnId), ne(STATUS, Status.DONE.toString())), combine(set(STATUS, Status.DONE.toString()), currentDate(UPDATE)));
                    logger.info("Transaction {} rolled back, marked as done: modified {}", txnId.toString(), setTxnAsDone.getModifiedCount());
                });

                // finalise ok transactions
                txns.find(eq(STATUS, Status.COMMIT.toString())).forEach((Consumer<? super Document>) tx -> {
                    ObjectId txnId = tx.getObjectId("_id");
                    logger.info("Detected committing transaction {}, finishing it", txnId.toString());
                    UpdateResult validateResult = collection.updateMany(eq("txnId", txnId), combine(unset("txnId"), set(VALID, true)));
                    logger.info("Validated {} documents", validateResult.getModifiedCount());
                    UpdateResult commitResult = txns.updateOne(and(eq("_id", txnId), ne(STATUS, Status.DONE.toString())), combine(set(STATUS, Status.DONE.toString()), currentDate(UPDATE)));
                    logger.info("Committed transaction {}, marked as done: modified {}", txnId.toString(), commitResult.getModifiedCount());
                });

                logger.info("Going back to sleep");
                sleep(60000);
            } catch (Throwable e) {
                logger.warn("Caught a Throwable, not worrying about it now", e);
            }
        }
    }
}
