package scheduler.oplevel.context;


import scheduler.oplevel.signal.op.OnParentUpdatedSignal;
import scheduler.oplevel.struct.Operation;
import scheduler.oplevel.struct.OperationChain;
import utils.lib.ConcurrentHashMap;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OPLayeredContext extends OPSchedulerContext {
    public HashMap<Integer, ArrayList<Operation>> allocatedLayeredOPBucket;// <LevelID, ArrayDeque<OperationChain>
    public int currentLevel;
    public int currentLevelIndex;
    public int totalThreads;
    public int maxLevel;//total number of operations to process per thread.
    public Operation ready_oc;//ready operation chain per thread.
    public ConcurrentLinkedQueue<OnParentUpdatedSignal> layerBuildHelperQueue = new ConcurrentLinkedQueue<>();

    public OPLayeredContext(int thisThreadId) {
        super(thisThreadId);
        this.totalThreads = totalThreads;
        this.allocatedLayeredOPBucket = new HashMap<>();
    }

    @Override
    public void reset() {
        super.reset();
        this.allocatedLayeredOPBucket.clear();
        currentLevel = 0;
        currentLevelIndex = 0;
        maxLevel = 0;
        layerBuildHelperQueue.clear();
    }

    public void redo() {
        super.redo();
        currentLevel = 0;
        currentLevelIndex = 0;
    }

    @Override
    public OperationChain createTask(String tableName, String pKey) {
        return new OperationChain(tableName, pKey);
    }

    public ArrayList<Operation> OPSCurrentLayer() {
        return allocatedLayeredOPBucket.get(currentLevel);
    }

    /**
     * Build buckets with submitted ocs.
     * Return the local maximal dependency level.
     *
     * @param ocs
     */
    public void buildBucketPerThread(Collection<OperationChain> ocs) {
        // TODO: update this logic to the latest logic that we proposed in operation level
        int localMaxDLevel = 0;
        int dependencyLevel;
        for (OperationChain oc : ocs) {
            if (oc.getOperations().isEmpty()) {
                continue;
            }
            this.totalOsToSchedule += oc.getOperations().size();
            oc.updateDependencyLevel();
            dependencyLevel = oc.getDependencyLevel();
            if (localMaxDLevel < dependencyLevel)
                localMaxDLevel = dependencyLevel;
            if (!allocatedLayeredOPBucket.containsKey(dependencyLevel))
                allocatedLayeredOPBucket.put(dependencyLevel, new ArrayList<>());
            allocatedLayeredOPBucket.get(dependencyLevel).addAll(oc.getOperations());
        }
//        if (enable_log) LOG.debug("localMaxDLevel" + localMaxDLevel);
        this.maxLevel = localMaxDLevel;
    }

    /**
     * Build buckets with submitted ocs.
     * Return the local maximal dependency level.
     *
     * @param ops
     * @param layeredOPBucket
     */
    public void buildBucketPerThread(Collection<Operation> ops, Collection<Operation> roots,
                                     ConcurrentHashMap<Integer, ConcurrentLinkedDeque<Operation>> layeredOPBucket) {
//        int localMaxDLevel = 0;
        int dependencyLevel;

        ArrayDeque<Operation> processedOps = new ArrayDeque<>();

        // traverse from roots to update dependency levels
        for (Operation root : roots) {
            updateDependencyLevel(processedOps, root);
        }

        // this procedure is similar to how partition state manager solves the dependnecies among operations,
        // where all dependencies of operations are handled by associated thread
        while (processedOps.size() != ops.size()) {
            OnParentUpdatedSignal signal = layerBuildHelperQueue.poll();
            while (signal != null) {
                Operation operation = signal.getTargetOperation();
                operation.updateDependencies(signal.getType(), signal.getState());
                if (!operation.hasParents()) {
                    updateDependencyLevel(processedOps, operation);
                }
                signal = layerBuildHelperQueue.poll();
            }
        }

        for (Operation op : ops) {
            assert op.hasValidDependencyLevel();
            dependencyLevel = op.getDependencyLevel();
//            if (localMaxDLevel < dependencyLevel)
//                localMaxDLevel = dependencyLevel;
            layeredOPBucket.computeIfAbsent(dependencyLevel, s -> new ConcurrentLinkedDeque<>());
            layeredOPBucket.get(dependencyLevel).add(op);
//            allocatedLayeredOPBucket.computeIfAbsent(dependencyLevel, s -> new ArrayList<>());
//            allocatedLayeredOPBucket.get(dependencyLevel).add(op);
        }
//        this.maxLevel = localMaxDLevel;
    }

    private void updateDependencyLevel(ArrayDeque<Operation> processedOps, Operation operation) {
        operation.calculateDependencyLevelDuringExploration();
        operation.layeredNotifyChildren();
        processedOps.add(operation);
    }
}