package durability.recovery.dependency;

import common.util.io.IOUtils;
import durability.logging.LoggingStrategy.ImplLoggingManager.DependencyLoggingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.SOURCE_CONTROL;
import utils.lib.ConcurrentHashMap;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;

public class CommandPrecedenceGraph {
    private static final Logger LOG = LoggerFactory.getLogger(CommandPrecedenceGraph.class);
    public ConcurrentHashMap<Integer, ConcurrentSkipListMap<String, CommandTask>> threadToTaskMap = new ConcurrentHashMap<>();
    public ConcurrentHashMap<Integer, CSContext> threadToCSContextMap = new ConcurrentHashMap<>();
    private int maxLevel = 0;//just for layered scheduling
    public int delta = 0;
    public void addTask(int threadId, CommandTask task) {
        threadToTaskMap.putIfAbsent(threadId, new ConcurrentSkipListMap<>());
        threadToTaskMap.get(threadId).put(task.dependencyLog.id, task);
        task.setContext(threadToCSContextMap.get(threadId));
    }
    public void addContext(int threadId, CSContext context) {
        threadToCSContextMap.put(threadId, context);
    }
    public void construct_graph(CSContext context){
        ArrayList<CommandTask> roots = new ArrayList<>();
        for (CommandTask task : threadToTaskMap.get(context.threadId).values()) {
            if (task.dependencyLog.isRoot()) {
                roots.add(task);
            }
            for (String childId : task.dependencyLog.getOutEdges()) {
                if (getTask(childId) != null)
                    task.addChild(getTask(childId));
            }
            for (String parentId : task.dependencyLog.getInEdges()) {
                if (getTask(parentId) != null)
                    task.addParent(getTask(parentId));
            }
        }
        context.totalTaskCount = threadToTaskMap.get(context.threadId).size();
        IOUtils.println("total task count: " + context.totalTaskCount + " thread id: " + context.threadId);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.threadId);
        context.buildBucketsPerThread(threadToTaskMap.get(context.threadId).values(), roots);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.threadId);
        if (context.threadId == 0) {
            for (CSContext csContext : threadToCSContextMap.values()) {
                maxLevel = Math.max(maxLevel, csContext.maxLevel);
            }
            LOG.info("max level: {}", maxLevel);
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.threadId);
        context.maxLevel = maxLevel;
    }
    public CommandTask getTask(String id) {
        for (ConcurrentSkipListMap<String, CommandTask> map : threadToTaskMap.values()) {
            if (map.containsKey(id))
                return map.get(id);
        }
        return null;
    }
    public void reset(CSContext context) {
        threadToTaskMap.get(context.threadId).clear();
        maxLevel = 0;
    }
}
