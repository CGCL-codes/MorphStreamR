package benchmark.datagenerator;

import benchmark.datagenerator.output.GephiOutputHandler;
import benchmark.datagenerator.output.IOutputHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

public class DataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);
    HashMap<Long, Integer> mGeneratedAccountIds = new HashMap<>();
    HashMap<Long, Integer> mGeneratedAssetIds = new HashMap<>();
    private Random mRandomGenerator = new Random();
    private Random mRandomGeneratorForAccIds = new Random(12345678);
    private Random mRandomGeneratorForAstIds = new Random(123456789);
    private int totalAccountRecords = 0;
    private int totalAssetRecords = 0;
    private int mTotalTuplesToGenerate;
    private DataGeneratorConfig dataConfig;
    private ArrayList<DataTransaction> mDataTransactions;
    private HashMap<Integer, ArrayList<DataOperationChain>> mAccountOperationChainsByLevel;
    private HashMap<Integer, ArrayList<DataOperationChain>> mAssetsOperationChainsByLevel;
    private IOutputHandler mDataOutputHandler;
    private float[] mAccountLevelsDistribution;
    private float[] mAssetLevelsDistribution;
    private float[] mOcLevelsDistribution;
    private boolean[] mPickAccount;
    private int mTransactionId = 0;
    private long totalTimeStart = 0;
    private long totalTime = 0;
    private long selectTuplesStart = 0;
    private long selectTuples = 0;
    private long updateDependencyStart = 0;
    private long updateDependency = 0;

    private long mPartitionOffset = 0;
    private int mPId = 0;


    public DataGenerator(DataGeneratorConfig dataConfig) {
        this.dataConfig = dataConfig;
        this.mTotalTuplesToGenerate = dataConfig.tuplesPerBatch * dataConfig.totalBatches;
        this.mDataTransactions = new ArrayList<>(mTotalTuplesToGenerate);
        this.mAccountOperationChainsByLevel = new HashMap<>();
        this.mAssetsOperationChainsByLevel = new HashMap<>();
        this.mAccountLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mAssetLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mOcLevelsDistribution = new float[dataConfig.dependenciesDistributionForLevels.length];
        this.mPickAccount = new boolean[dataConfig.dependenciesDistributionForLevels.length];
        this.mPartitionOffset = (mTotalTuplesToGenerate * 5) / dataConfig.totalThreads;
    }

    public DataGeneratorConfig getDataConfig() {
        return dataConfig;
    }

    public void GenerateData() {

        File file = new File(dataConfig.rootPath);
        if (file.exists()) {
            LOG.info("Data already exists.. skipping data generation...");
            LOG.info(dataConfig.rootPath);
            return;
        }

        mDataOutputHandler = new GephiOutputHandler(dataConfig.rootPath);

        LOG.info(String.format("Data Generator will dump data at %s.", dataConfig.rootPath));
        for (int tupleNumber = 0; tupleNumber < mTotalTuplesToGenerate / 10; tupleNumber++) {
            totalTimeStart = System.nanoTime();
            GenerateTuple();
            totalTime += System.nanoTime() - totalTimeStart;
            UpdateStats();

            if (mTransactionId % 100000 == 0) {
                float selectTuplesPer = (selectTuples * 1.0f) / (totalTime * 1.0f) * 100.0f;
                float updateDependencyPer = (updateDependency * 1.0f) / (totalTime * 1.0f) * 100.0f;
                LOG.info(String.format("Dependency Distribution...select tuple time: %.3f%%, update dependency time: %.3f%%", selectTuplesPer, updateDependencyPer));

                for (int lop = 0; lop < mOcLevelsDistribution.length; lop++) {
                    System.out.print(lop + ": " + mOcLevelsDistribution[lop] + "; ");
                }
                LOG.info(" ");
            }
        }
        dumpGeneratedDataToFile();
        LOG.info("Date Generation is done...");
        clearDataStructures();
        this.dataConfig = null;
    }


    private void GenerateTuple() {

        DataOperationChain srcAccOC = null;
        DataOperationChain srcAstOC = null;
        DataOperationChain dstAccOC = null;
        DataOperationChain dstAstOC = null;

        boolean srcAcc_dependsUpon_srcAst = false;
        boolean srcAst_dependsUpon_srcAcc = false;

        boolean dstAst_dependsUpon_srcAst = false;
        boolean dstAst_dependsUpon_srcAcc = false;
        boolean dstAst_dependsUpon_dstAcc = false;

        boolean dstAcc_dependsUpon_srcAst = false;
        boolean dstAcc_dependsUpon_srcAcc = false;
        boolean dstAcc_dependsUpon_dstAst = false;

        selectTuplesStart = System.nanoTime();
        if (mOcLevelsDistribution[0] >= dataConfig.dependenciesDistributionForLevels[0]) {

            int selectedLevel = 0;
            for (int lop = 1; lop < dataConfig.numberOfDLevels; lop++) {
                if (mOcLevelsDistribution[lop] < dataConfig.dependenciesDistributionForLevels[lop]) {
                    selectedLevel = lop - 1;
                    break;
                }
            }

            mPId = mRandomGenerator.nextInt(dataConfig.totalThreads);

            if (mPickAccount[selectedLevel]) {

                srcAccOC = getRandomExistingOC(selectedLevel, mAccountOperationChainsByLevel);
                if (srcAccOC == null)
                    srcAccOC = getNewAccountOC();
                mPId += 1;
                mPId = mPId % dataConfig.totalThreads;

                srcAstOC = getNewAssetOC();
                mPId += 1;
                mPId = mPId % dataConfig.totalThreads;

                dstAccOC = getRandomExistingDestOC(mAccountOperationChainsByLevel, srcAccOC, srcAstOC);
                if (dstAccOC == null)
                    dstAccOC = getNewAccountOC();
                mPId += 1;
                mPId = mPId % dataConfig.totalThreads;

                dstAstOC = getRandomExistingDestOC(mAssetsOperationChainsByLevel, srcAccOC, srcAstOC);
                if (dstAstOC == null)
                    dstAstOC = getNewAssetOC();

            } else {

                srcAccOC = getNewAccountOC();
                mPId += 1;
                mPId = mPId % dataConfig.totalThreads;

                srcAstOC = getRandomExistingOC(selectedLevel, mAssetsOperationChainsByLevel);
                if (srcAstOC == null)
                    srcAstOC = getNewAssetOC();
                mPId += 1;
                mPId = mPId % dataConfig.totalThreads;

                dstAccOC = getRandomExistingDestOC(mAccountOperationChainsByLevel, srcAccOC, srcAstOC);
                if (dstAccOC == null)
                    dstAccOC = getNewAccountOC();
                mPId += 1;
                mPId = mPId % dataConfig.totalThreads;

                dstAstOC = getRandomExistingDestOC(mAssetsOperationChainsByLevel, srcAccOC, srcAstOC);
                if (dstAstOC == null)
                    dstAstOC = getNewAssetOC();
            }

        } else {
            srcAccOC = getNewAccountOC();
            srcAstOC = getNewAssetOC();
            dstAccOC = getNewAccountOC();
            dstAstOC = getNewAssetOC();
        }
        selectTuples += System.nanoTime() - selectTuplesStart;

        dstAcc_dependsUpon_dstAst = dstAccOC.doesDependsUpon(dstAstOC);
        dstAst_dependsUpon_dstAcc = dstAstOC.doesDependsUpon(dstAccOC);

        // register dependencies for srcAssets
        if (srcAccOC.getOperationsCount() > 0) {
            srcAst_dependsUpon_srcAcc = srcAstOC.doesDependsUpon(srcAccOC);
            dstAcc_dependsUpon_srcAcc = dstAccOC.doesDependsUpon(srcAccOC);
            dstAst_dependsUpon_srcAcc = dstAstOC.doesDependsUpon(srcAccOC);

            if (!srcAst_dependsUpon_srcAcc) {
                srcAccOC.addDependent(srcAstOC);
                srcAstOC.addDependency(srcAccOC);
                updateDependencyStart = System.nanoTime();
                srcAstOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

            if (!dstAcc_dependsUpon_srcAcc && !dstAcc_dependsUpon_dstAst) {
                srcAccOC.addDependent(dstAccOC);
                dstAccOC.addDependency(srcAccOC);
                updateDependencyStart = System.nanoTime();
                dstAccOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

            if (!dstAst_dependsUpon_srcAcc && !dstAst_dependsUpon_dstAcc) {
                srcAccOC.addDependent(dstAstOC);
                dstAstOC.addDependency(srcAccOC);
                updateDependencyStart = System.nanoTime();
                dstAstOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

        } else if (srcAstOC.getOperationsCount() > 0) {
            srcAcc_dependsUpon_srcAst = srcAstOC.doesDependsUpon(srcAstOC);
            dstAcc_dependsUpon_srcAst = dstAccOC.doesDependsUpon(srcAstOC);
            dstAst_dependsUpon_srcAst = dstAstOC.doesDependsUpon(srcAstOC);

            if (!srcAcc_dependsUpon_srcAst) {
                srcAstOC.addDependent(srcAccOC);
                srcAccOC.addDependency(srcAstOC);
                updateDependencyStart = System.nanoTime();
                srcAccOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

            if (!dstAcc_dependsUpon_srcAst && !dstAcc_dependsUpon_dstAst) {
                srcAstOC.addDependent(dstAccOC);
                dstAccOC.addDependency(srcAstOC);
                updateDependencyStart = System.nanoTime();
                dstAccOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }

            if (!dstAst_dependsUpon_srcAst && !dstAst_dependsUpon_dstAcc) {
                srcAstOC.addDependent(dstAstOC);
                dstAstOC.addDependency(srcAstOC);
                updateDependencyStart = System.nanoTime();
                dstAstOC.updateAllDependencyLevel();
                updateDependency += System.nanoTime() - updateDependencyStart;
            }
        }

        srcAccOC.addAnOperation();
        srcAstOC.addAnOperation();
        dstAccOC.addAnOperation();
        dstAstOC.addAnOperation();

        DataTransaction t = new DataTransaction(mTransactionId, srcAccOC.getId(), srcAstOC.getId(), dstAccOC.getId(), dstAstOC.getId());
        mDataTransactions.add(t);
        mTransactionId++;
        if (mTransactionId % 100000 == 0)
            LOG.info(String.valueOf(mTransactionId));
    }

    private void UpdateStats() {

        for (int lop = 0; lop < dataConfig.numberOfDLevels; lop++) {

            float accountLevelCount = 0;
            if (mAccountOperationChainsByLevel.containsKey(lop))
                accountLevelCount = mAccountOperationChainsByLevel.get(lop).size() * 1.0f;

            float assetLevelCount = 0;
            if (mAssetsOperationChainsByLevel.containsKey(lop))
                assetLevelCount = mAssetsOperationChainsByLevel.get(lop).size() * 1.0f;

            mAccountLevelsDistribution[lop] = accountLevelCount / totalAccountRecords;
            mAssetLevelsDistribution[lop] = assetLevelCount / totalAssetRecords;
            mOcLevelsDistribution[lop] = (accountLevelCount + assetLevelCount) / (totalAccountRecords + totalAssetRecords);
            mPickAccount[lop] = mAccountLevelsDistribution[lop] < mAssetLevelsDistribution[lop];
        }
    }

    private DataOperationChain getRandomExistingDestOC(HashMap<Integer, ArrayList<DataOperationChain>> allOcs, DataOperationChain srcOC, DataOperationChain srcAst) {

        ArrayList<DataOperationChain> independentOcs = allOcs.get(0);
        if (independentOcs == null || independentOcs.size() == 0)
            return null;

        DataOperationChain oc = null;
        for (int lop = independentOcs.size() - 1; lop >= 0; lop--) {
            oc = independentOcs.get(lop);
            if (!oc.hasDependents() &&
                    oc != srcOC &&
                    oc != srcAst &&
                    !srcOC.doesDependsUpon(oc) &&
                    !srcAst.doesDependsUpon(oc)
                    && (oc.getId() % mPartitionOffset) == mPId)
                break;
            if (independentOcs.size() - lop > 100) {
                oc = null;
                break;
            }
            oc = null;
        }
        return oc;

    }

    private DataOperationChain getRandomExistingOC(int selectionLevel, HashMap<Integer, ArrayList<DataOperationChain>> ocs) {

        ArrayList<DataOperationChain> selectedLevelFilteredOCs = null;
        if (ocs.containsKey(selectionLevel))
            selectedLevelFilteredOCs = ocs.get(selectionLevel);

        DataOperationChain oc = null;
        if (selectedLevelFilteredOCs != null && selectedLevelFilteredOCs.size() > 0) {
            oc = selectedLevelFilteredOCs.get(mRandomGenerator.nextInt(selectedLevelFilteredOCs.size()));
        } else {
            oc = null;
        }
        if (oc != null && (oc.getId() % mPartitionOffset) == mPId)
            oc = null;
        return oc;
    }

    private void dumpGeneratedDataToFile() {

        File file = new File(dataConfig.rootPath);
        if (file.exists()) {
            LOG.info("Data already exists.. skipping data generation...");
            return;
        }
        file.mkdirs();

        File versionFile = new File(dataConfig.rootPath.substring(0, dataConfig.rootPath.length() - 1)
                + String.format("_%d_%d_%d.txt", dataConfig.tuplesPerBatch, dataConfig.totalBatches, dataConfig.numberOfDLevels));
        try {
            versionFile.createNewFile();
            FileWriter fileWriter = new FileWriter(versionFile);
            fileWriter.write(String.format("Tuples per batch      : %d\n", dataConfig.tuplesPerBatch));
            fileWriter.write(String.format("Total batches         : %d\n", dataConfig.totalBatches));
            fileWriter.write(String.format("Dependency depth      : %d\n", dataConfig.numberOfDLevels));
            fileWriter.write(String.format("%s\n", Arrays.toString(mOcLevelsDistribution)));
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOG.info(String.format("Dumping transactions..."));
        mDataOutputHandler.sinkTransactions(mDataTransactions);
//         LOG.info(String.format("Dumping Dependency Edges..."));
//        mDataOutputHandler.sinkDependenciesEdges(mAccountOperationChainsByLevel, mAssetsOperationChainsByLevel);
        LOG.info(String.format("Dumping Dependency Vertices..."));
        mDataOutputHandler.sinkDependenciesVertices(mAccountOperationChainsByLevel, mAssetsOperationChainsByLevel);
        LOG.info(String.format("Dumping Dependency Vertices ids range..."));
        mDataOutputHandler.sinkDependenciesVerticesIdsRange(totalAccountRecords, totalAssetRecords);
    }

    private DataOperationChain getNewAccountOC() {

        long id = 0;
//        int range = 10 * mTotalTuplesToGenerate * 5;
        int range = (int) mPartitionOffset;
        if (dataConfig.idGenType.equals("uniform")) {
            id = mRandomGeneratorForAccIds.nextInt(range);
            id += mPartitionOffset * mPId;
            id *= 10;
            totalAccountRecords++;
            while (mGeneratedAccountIds.containsKey(id)) {
                id = mRandomGeneratorForAccIds.nextInt(range);
                id += mPartitionOffset * mPId;
                id *= 10;
                totalAccountRecords++;
            }
        } else if (dataConfig.idGenType.equals("normal")) {
            id = (int) Math.floor(Math.abs(mRandomGeneratorForAccIds.nextGaussian() / 3.5) * range) % range;
            id += mPartitionOffset * mPId;
            id *= 10;
            totalAccountRecords++;
            while (mGeneratedAccountIds.containsKey(id)) {
                id = (int) Math.floor(Math.abs(mRandomGeneratorForAccIds.nextGaussian() / 3.5) * range) % range;
                id += mPartitionOffset * mPId;
                id *= 10;
                totalAccountRecords++;
            }
        }

        mGeneratedAccountIds.put(id, null);
        DataOperationChain oc = new DataOperationChain("act_" + id, (10 * mTotalTuplesToGenerate * 5) / dataConfig.numberOfDLevels, mAccountOperationChainsByLevel);

        return oc;
    }

    private DataOperationChain getNewAssetOC() {

        long id = 0;
//        int range = 10 * mTotalTuplesToGenerate * 5;
        int range = (int) mPartitionOffset;
        if (dataConfig.idGenType.equals("uniform")) {
            id = mRandomGeneratorForAstIds.nextInt(range);
            id += mPartitionOffset * mPId;
            id *= 10;
            totalAssetRecords++;
            while (mGeneratedAssetIds.containsKey(id)) {
                id = mRandomGeneratorForAstIds.nextInt(range);
                id += mPartitionOffset * mPId;
                id *= 10;
                totalAssetRecords++;
            }
        } else if (dataConfig.idGenType.equals("normal")) {
            id = (int) Math.floor(Math.abs(mRandomGeneratorForAstIds.nextGaussian() / 3.5) * range) % range;
            id += mPartitionOffset * mPId;
            id *= 10;
            totalAssetRecords++;
            while (mGeneratedAssetIds.containsKey(id)) {
                id = (int) Math.floor(Math.abs(mRandomGeneratorForAstIds.nextGaussian() / 3.5) * range) % range;
                id += mPartitionOffset * mPId;
                id *= 10;
                totalAssetRecords++;
            }
        }
        mGeneratedAssetIds.put(id, null);
        DataOperationChain oc = new DataOperationChain("ast_" + id, (10 * mTotalTuplesToGenerate * 5) / dataConfig.numberOfDLevels, mAssetsOperationChainsByLevel);
        return oc;
    }

    private void clearDataStructures() {

        if (mDataTransactions != null) {
            mDataTransactions.clear();
        }
        mDataTransactions = new ArrayList<>();

        if (mAccountOperationChainsByLevel != null) {
            mAccountOperationChainsByLevel.clear();
        }
        mAccountOperationChainsByLevel = new HashMap<>();

        if (mAssetsOperationChainsByLevel != null) {
            mAssetsOperationChainsByLevel.clear();
        }
        mAssetsOperationChainsByLevel = new HashMap<>();

        this.mAccountLevelsDistribution = new float[dataConfig.numberOfDLevels];
        this.mAssetLevelsDistribution = new float[dataConfig.numberOfDLevels];
        this.mOcLevelsDistribution = new float[dataConfig.numberOfDLevels];
        this.mPickAccount = new boolean[dataConfig.numberOfDLevels];
    }
}
