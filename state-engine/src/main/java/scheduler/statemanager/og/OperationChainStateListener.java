package scheduler.statemanager.og;

import scheduler.struct.MetaTypes.DependencyType;
import scheduler.struct.og.OperationChain;

public interface OperationChainStateListener {

    void onOcRootStart(OperationChain operationChain);

    void onOcExecuted(OperationChain operationChain);

    void onOcParentExecuted(OperationChain operationChain, DependencyType dependencyType);
}
