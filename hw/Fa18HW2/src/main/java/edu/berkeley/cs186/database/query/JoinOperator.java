package edu.berkeley.cs186.database.query; //hw2

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

public abstract class JoinOperator extends QueryOperator {

  public enum JoinType {
    SNLJ,
    PNLJ,
    BNLJ,
    GRACEHASH,
    SORTMERGE
  }

  private JoinType joinType;
  private QueryOperator leftSource;
  private QueryOperator rightSource;
  private int leftColumnIndex;
  private int rightColumnIndex;
  private String leftColumnName;
  private String rightColumnName;
  private Database.Transaction transaction;

  /**
   * Create a join operator that pulls tuples from leftSource and rightSource. Returns tuples for which
   * leftColumnName and rightColumnName are equal.
   *
   * @param leftSource the left source operator
   * @param rightSource the right source operator
   * @param leftColumnName the column to join on from leftSource
   * @param rightColumnName the column to join on from rightSource
   * @throws QueryPlanException
   */
  public JoinOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction,
                      JoinType joinType) throws QueryPlanException {
    super(OperatorType.JOIN);
    this.joinType = joinType;
    this.leftSource = leftSource;
    this.rightSource = rightSource;
    this.leftColumnName = leftColumnName;
    this.rightColumnName = rightColumnName;
    this.setOutputSchema(this.computeSchema());
    this.transaction = transaction;
  }

  public abstract Iterator<Record> iterator() throws QueryPlanException, DatabaseException;

  @Override
  public QueryOperator getSource() throws QueryPlanException {
    throw new QueryPlanException("There is no single source for join operators. Please use " +
        "getRightSource and getLeftSource and the corresponding set methods.");
  }

  public QueryOperator getLeftSource() {
    return this.leftSource;
  }

  public QueryOperator getRightSource() {
    return this.rightSource;
  }

  public void setLeftSource(QueryOperator leftSource) {
    this.leftSource = leftSource;
  }

  public void setRightSource(QueryOperator rightSource) {
    this.rightSource = rightSource;
  }

  public Schema computeSchema() throws QueryPlanException {
    Schema leftSchema = this.leftSource.getOutputSchema();
    Schema rightSchema = this.rightSource.getOutputSchema();
    List<String> leftSchemaNames = new ArrayList<String>(leftSchema.getFieldNames());
    List<String> rightSchemaNames = new ArrayList<String>(rightSchema.getFieldNames());
    this.leftColumnName = this.checkSchemaForColumn(leftSchema, this.leftColumnName);
    this.leftColumnIndex = leftSchemaNames.indexOf(leftColumnName);
    this.rightColumnName = this.checkSchemaForColumn(rightSchema, this.rightColumnName);
    this.rightColumnIndex = rightSchemaNames.indexOf(rightColumnName);
    List<Type> leftSchemaTypes = new ArrayList<>(leftSchema.getFieldTypes());
    List<Type> rightSchemaTypes = new ArrayList<>(rightSchema.getFieldTypes());
    if (!leftSchemaTypes.get(this.leftColumnIndex).getClass().equals(rightSchemaTypes.get(
        this.rightColumnIndex).getClass())) {
      throw new QueryPlanException("Mismatched types of columns " + leftColumnName + " and "
          + rightColumnName + ".");
    }
    leftSchemaNames.addAll(rightSchemaNames);
    leftSchemaTypes.addAll(rightSchemaTypes);
    return new Schema(leftSchemaNames, leftSchemaTypes);
  }

  public String str() {
    return "type: " + this.joinType +
            "\nleftColumn: " + this.leftColumnName +
            "\nrightColumn: " + this.rightColumnName;
  }

  @Override
  public String toString() {
    String r = this.str();
    if (this.leftSource != null) {
      r += "\n" + ("(left)\n" + this.leftSource.toString()).replaceAll("(?m)^", "\t");
    }
    if (this.rightSource != null) {
      if (this.leftSource != null) {
        r += "\n";
      }
      r += "\n" + ("(right)\n" + this.rightSource.toString()).replaceAll("(?m)^", "\t");
    }
    return r;
  }

/**
   * Estimates the table statistics for the result of executing this query operator.
   *
   * @return estimated TableStats
   */
  public TableStats estimateStats() throws QueryPlanException {
    TableStats leftStats = this.leftSource.getStats();
    TableStats rightStats = this.rightSource.getStats();


    return leftStats.copyWithJoin(this.leftColumnIndex,
            rightStats,
            this.rightColumnIndex);
  }

  public abstract int estimateIOCost() throws QueryPlanException;

  public Schema getSchema(String tableName) throws DatabaseException {
    return this.transaction.getSchema(tableName);
  }



  public Iterator<Page> getPageIterator(String tableName) throws DatabaseException {
    return this.transaction.getPageIterator(tableName);
  }

  public byte[] getPageHeader(String tableName, Page p) throws DatabaseException {
    return this.transaction.readPageHeader(tableName, p);
  }

  public int getNumEntriesPerPage(String tableName) throws DatabaseException {
    return this.transaction.getNumEntriesPerPage(tableName);
  }

  public int getEntrySize(String tableName) throws DatabaseException {
    return this.transaction.getEntrySize(tableName);
  }

  public int getHeaderSize(String tableName) throws DatabaseException {
    return this.transaction.getPageHeaderSize(tableName);
  }

  public String getLeftColumnName() {
    return this.leftColumnName;
  }

  public String getRightColumnName() {
    return this.rightColumnName;
  }

  public Database.Transaction getTransaction() {
    return this.transaction;
  }

  public int getLeftColumnIndex() {
    return this.leftColumnIndex;
  }

  public int getRightColumnIndex() {
    return this.rightColumnIndex;
  }

  public Record getRecord(String tableName, RecordId rid) throws DatabaseException {
    return this.transaction.getRecord(tableName, rid);
  }

  public RecordIterator getRecordIterator(String tableName) throws DatabaseException {
    return this.transaction.getRecordIterator(tableName);
  }

  public BacktrackingIterator<Record> getBlockIterator(String tableName, Page[] block) throws DatabaseException {
    return this.transaction.getBlockIterator(tableName, block);
  }

  public BacktrackingIterator<Record> getBlockIterator(String tableName, BacktrackingIterator<Page> block) throws DatabaseException {
    return this.transaction.getBlockIterator(tableName, block);
  }

  public BacktrackingIterator<Record> getBlockIterator(String tableName, Iterator<Page> block, int maxPages) throws DatabaseException {
    return this.transaction.getBlockIterator(tableName, block, maxPages);
  }

  public RecordIterator getTableIterator(String tableName) throws DatabaseException {
    return this.transaction.getRecordIterator(tableName);
  }

  public String createTempTable(Schema schema) throws DatabaseException {
    return this.transaction.createTempTable(schema);
  }

  public void createTempTable(Schema schema, String tempTableName) throws DatabaseException {
    this.transaction.createTempTable(schema, tempTableName);
  }

  public RecordId addRecord(String tableName, List<DataBox> values) throws DatabaseException {
    return this.transaction.addRecord(tableName, values);
  }

  public JoinType getJoinType() {
    return this.joinType;
  }

  /**
   * All iterators for subclasses of JoinOperator should subclass from
   * JoinIterator; JoinIterator handles creating temporary tables out of the left and right
   * input operators.
   */
  protected abstract class JoinIterator implements Iterator<Record> {
    private String leftTableName;
    private String rightTableName;

    public JoinIterator() throws QueryPlanException, DatabaseException {
      if (JoinOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator) JoinOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = JoinOperator.this.createTempTable(JoinOperator.this.getLeftSource().getOutputSchema());
        Iterator<Record> leftIter = JoinOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          JoinOperator.this.addRecord(this.leftTableName, leftIter.next().getValues());
        }
      }
      if (JoinOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator) JoinOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = JoinOperator.this.createTempTable(JoinOperator.this.getRightSource().getOutputSchema());
        Iterator<Record> rightIter = JoinOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          JoinOperator.this.addRecord(this.rightTableName, rightIter.next().getValues());
        }
      }
    }

    protected String getLeftTableName() {
      return this.leftTableName;
    }

    protected String getRightTableName() {
      return this.rightTableName;
    }
  }
}
