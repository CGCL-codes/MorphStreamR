package common.topology.transactional.initializer;

import benchmark.DataHolder;
import benchmark.datagenerator.DataGeneratorConfig;
import benchmark.datagenerator.SpecialDataGenerator;
import benchmark.datagenerator.apps.SL.OCTxnGenerator.LayeredOCDataGeneratorConfig;
import benchmark.datagenerator.apps.SL.OCTxnGenerator.LayeredOCDataGenerator;
import benchmark.datagenerator.apps.SL.TPGTxnGenerator.TPGDataGeneratorConfig;
import benchmark.datagenerator.apps.SL.TPGTxnGenerator.TPGDataGenerator;
import common.SpinLock;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.sl.TransactionEvent;
import db.Database;
import db.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.DataBox;
import storage.datatype.LongDataBox;
import storage.datatype.StringDataBox;
import storage.table.RecordSchema;
import transaction.TableInitilizer;
import transaction.scheduler.tpg.struct.Controller;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

import static common.constants.StreamLedgerConstants.Constant.*;
import static transaction.State.configure_store;

//import static xerial.jnuma.Numa.setLocalAlloc;
public class SLInitializer extends TableInitilizer {

    private static final Logger LOG = LoggerFactory.getLogger(SLInitializer.class);
    private final int totalRecords;
    private final String idsGenType;
    private String dataRootPath;
    private SpecialDataGenerator dataGenerator;

    private final int startingBalance = 1000000;
    private final String actTableKey = "accounts";
    private final String bookTableKey = "bookEntries";
    private final int partitionOffset;

    private final boolean isOC;
    private final boolean isTPG;

    public SLInitializer(Database db, String dataRootPath, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        this.dataRootPath = dataRootPath;
        configure_store(scale_factor, theta, tthread, config.getInt("NUM_ITEMS"));
        totalRecords = config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches");
        idsGenType = config.getString("idGenType");
//        this.mPartitionOffset = (totalRecords * 5) / tthread;
        this.partitionOffset = config.getInt("NUM_ITEMS") / tthread;

        Controller.setExec(tthread);

        String generator = config.getString("generator");
        isOC = generator.equals("OCGenerator");
//        isBFS = true; // just for test, make tpg and bfs use the same data generator - bfs
        isTPG = generator.equals("TPGGenerator");
        if (isOC) {
            createDataGeneratorForBFS(config);
        } else if (isTPG) {
            createDataGeneratorForTPG(config);
        } else {
            throw new UnsupportedOperationException("wrong scheduler set up: " + generator);
        }
    }

    protected void createDataGeneratorForBFS(Configuration config) {

        DataGeneratorConfig dataConfig = new LayeredOCDataGeneratorConfig();
        dataConfig.initialize(config);

        configurePath(dataConfig);
        dataGenerator = new LayeredOCDataGenerator((LayeredOCDataGeneratorConfig) dataConfig);
    }

    protected void createDataGeneratorForTPG(Configuration config) {

        DataGeneratorConfig dataConfig = new TPGDataGeneratorConfig();
        dataConfig.initialize(config);

        configurePath(dataConfig);
        dataGenerator = new TPGDataGenerator((TPGDataGeneratorConfig) dataConfig);
    }

    private void configurePath(DataGeneratorConfig dataConfig) {
        MessageDigest digest;
        String subFolder = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            subFolder = OsUtils.osWrapperPostFix(
                    DatatypeConverter.printHexBinary(
                            digest.digest(
                                    String.format("%d_%d",
                                            dataConfig.getTotalThreads(),
                                                    dataConfig.getTuplesPerBatch() * dataConfig.getTotalBatches())
                                            .getBytes(StandardCharsets.UTF_8))));
        } catch (Exception e) {
            e.printStackTrace();
        }
        dataConfig.setRootPath(dataConfig.getRootPath() + OsUtils.OS_wrapper(subFolder));
        dataConfig.setIdsPath(dataConfig.getIdsPath() + OsUtils.OS_wrapper(subFolder));
        this.dataRootPath += OsUtils.OS_wrapper(subFolder);
    }


    @Override
    public void loadDB(int thread_id, int NUM_TASK) {
        loadDB(thread_id, null, NUM_TASK);
    }

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, int NUM_TASK) {
        int partition_interval = (int) Math.ceil(config.getInt("NUM_ITEMS") / (double) NUM_TASK);
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == NUM_TASK - 1) {//last executor need to handle left-over
            right_bound = config.getInt("NUM_ITEMS");
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }
        for (int key = left_bound; key < right_bound; key++) {
            int pid = get_pid(partition_interval, key);
            String _key = GenerateKey(ACCOUNT_ID_PREFIX, key);
            insertAccountRecord(_key, startingBalance, pid, spinlock);
            Controller.UpdateMapping(thread_id, "accounts" + "|" + _key);
            _key = GenerateKey(BOOK_ENTRY_ID_PREFIX, key);
            insertAssetRecord(_key, startingBalance, pid, spinlock);
            Controller.UpdateMapping(thread_id, "bookEntries" + "|" + _key);

        }
        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertAccountRecord(String key, long value, int pid, SpinLock[] spinlock_) {
        try {
            if (spinlock_ != null)
                db.InsertRecord("accounts", new TableRecord(AccountRecord(key, value), pid, spinlock_));
            else
                db.InsertRecord("accounts", new TableRecord(AccountRecord(key, value)));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial asset value_list is 0...?
     */
    private void insertAssetRecord(String key, long value, int pid, SpinLock[] spinlock_) {
        try {
            if (spinlock_ != null)
                db.InsertRecord("bookEntries", new TableRecord(AssetRecord(key, value), pid, spinlock_));
            else
                db.InsertRecord("bookEntries", new TableRecord(AssetRecord(key, value)));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private SchemaRecord AccountRecord(String key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        return new SchemaRecord(values);
    }

    private SchemaRecord AssetRecord(String key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        return new SchemaRecord(values);
    }

    //    private String rightpad(String text, int length) {
//        return String.format("%-" + length + "." + length + "s", text);
//    }
//
    private String GenerateKey(String prefix, int key) {
//        return rightpad(prefix + String.valueOf(key), VALUE_LEN);
        return prefix + key;
    }

    private RecordSchema getRecordSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new LongDataBox());
        fieldNames.add("Key");//PK
        fieldNames.add("Value");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    private RecordSchema AccountsScheme() {
        return getRecordSchema();
    }

    private RecordSchema BookEntryScheme() {
        return getRecordSchema();
    }

    @Override
    public boolean Prepared(String fileN) {

        int tuplesPerBatch = dataGenerator.getDataConfig().getTuplesPerBatch();
        int totalBatches = dataGenerator.getDataConfig().getTotalBatches();
        boolean shufflingActive = dataGenerator.getDataConfig().getShufflingActive();
        String folder = dataGenerator.getDataConfig().getRootPath();

        dataGenerator.prepareForExecution();

        loadTransactionEvents(tuplesPerBatch, totalBatches, shufflingActive, folder);
        return true;
    }

    @Override
    public void store(String file_name) throws IOException {

    }

    protected void loadTransactionEvents(int tuplesPerBatch, int totalBatches, boolean shufflingActive, String folder) {

        if (DataHolder.events == null) {
            DataHolder.events = new TransactionEvent[totalRecords];
            File file = new File(folder + "transactions.txt");
            if (file.exists()) {
                LOG.info(String.format("Reading transactions..."));
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                    String txn = reader.readLine();
                    int count = 0;
                    int p_bids[] = new int[tthread];
                    while (txn != null) {
                        String[] split = txn.split(",");
                        int npid = (int) (Long.valueOf(split[1]) / partitionOffset);
                        TransactionEvent event = new TransactionEvent(
                                Integer.parseInt(split[0]), //bid
                                npid, //pid
                                Arrays.toString(p_bids), //bid_array
                                4,//num_of_partition
                                split[1],//getSourceAccountId
                                split[2],//getSourceBookEntryId
                                split[3],//getTargetAccountId
                                split[4],//getTargetBookEntryId
                                100,  //getAccountTransfer
                                100  //getBookEntryTransfer
                        );
                        for (int x = 0; x < 4; x++)
                            p_bids[(npid + x) % tthread]++;
                        DataHolder.events[count] = event;
                        count++;
                        if (count % 100000 == 0)
                            LOG.info(String.format("%d transactions read...", count));
                        txn = reader.readLine();
                    }
                    reader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                LOG.info(String.format("Done reading transactions..."));

                if (shufflingActive) {
                    Random random = new Random();
                    int index;
                    TransactionEvent temp;
                    for (int lop = 0; lop < totalBatches; lop++) {
                        int start = lop * tuplesPerBatch;
                        int end = (lop + 1) * tuplesPerBatch;

                        for (int i = end - 1; i > start; i--) {
                            index = start + random.nextInt(i - start + 1);
                            temp = DataHolder.events[index];
                            DataHolder.events[index] = DataHolder.events[i];
                            DataHolder.events[i] = temp;
                        }
                    }
                }
            }
        }
    }

    @Override
    public Object create_new_event(int num_p, int bid) {
        return null;
    }

    public void creates_Table(Configuration config) {
        RecordSchema s = AccountsScheme();
        db.createTable(s, "accounts");
        RecordSchema b = BookEntryScheme();
        db.createTable(b, "bookEntries");
        try {
            prepare_input_events("SL_Events", config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
