package org.logart.node;

import org.logart.page.Page;
import org.logart.page.PageManager;

public class MapBasedNodeManager extends AbstractNodeManager {
    public MapBasedNodeManager(PageManager pageManager) {
        super(pageManager);
    }

    @Override
    protected BTreeNode allocateNodeBackedByPage(Page page) {
        return new InMemoryBTreeNode(page);
    }
}