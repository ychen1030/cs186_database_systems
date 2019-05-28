package edu.berkeley.cs186.database.index;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.table.RecordId;

public class TestBPlusNode {
    public static final String testFile = "TestBPlusNode";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 1 seconds max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.seconds(1));

    private BPlusTreeMetadata getBPlusTreeMetadata(Type keySchema, int order)
        throws IOException {
      File file = tempFolder.newFile(testFile);
      PageAllocator allocator = new PageAllocator(file.getAbsolutePath(), false);
      return new BPlusTreeMetadata(allocator, keySchema, order);
    }

    @Test
    public void testFromBytes() throws IOException {
      int d = 5;
      BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), d);

      // Leaf node.
      List<DataBox> leafKeys = new ArrayList<>();
      List<RecordId> leafRids = new ArrayList<>();
      for (int i = 0; i < 2 * d; ++i) {
        leafKeys.add(new IntDataBox(i));
        leafRids.add(new RecordId(i, (short) i));
      }
      LeafNode leaf = new LeafNode(meta, leafKeys, leafRids, Optional.of(42));

      // Inner node.
      List<DataBox> innerKeys = new ArrayList<>();
      List<Integer> innerChildren = new ArrayList<>();
      for (int i = 0; i < 2 * d; ++i) {
        innerKeys.add(new IntDataBox(i));
        innerChildren.add(i);
      }
      innerChildren.add(2*d);
      InnerNode inner = new InnerNode(meta, innerKeys, innerChildren);

      int leafPageNum = leaf.getPage().getPageNum();
      int innerPageNum = inner.getPage().getPageNum();
      assertEquals(leaf, BPlusNode.fromBytes(meta, leafPageNum));
      assertEquals(inner, BPlusNode.fromBytes(meta, innerPageNum));
    }
}
