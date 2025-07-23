package org.logart.node;

import org.logart.page.MMAPBasedPageManager;
import org.logart.page.Page;

public class PageBasedNodeManager extends AbstractNodeManager {
    public PageBasedNodeManager(MMAPBasedPageManager mmapBasedPageManager) {
       super(mmapBasedPageManager);
    }

    @Override
    protected BTreeNode allocateNodeBackedByPage(Page page) {
        return new InMemoryBTreeNode(page);
    }
}