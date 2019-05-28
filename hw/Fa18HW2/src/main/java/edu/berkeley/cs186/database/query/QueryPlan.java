package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * QueryPlan provides a set of functions to generate simple queries. Calling the methods corresponding
 * to SQL syntax stores the information in the QueryPlan, and calling execute generates and executes
 * a QueryPlan DAG.
 */
public class QueryPlan {
  public enum PredicateOperator {
    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS,
    GREATER_THAN,
    GREATER_THAN_EQUALS
  }

  private Database.Transaction transaction;
  private QueryOperator finalOperator;
  private String startTableName;

  private List<String> joinTableNames;
  private List<String> joinLeftColumnNames;
  private List<String> joinRightColumnNames;
  private List<String> selectColumnNames;
  private List<PredicateOperator> selectOperators;
  private List<DataBox> selectDataBoxes;
  private List<String> projectColumns;
  private String groupByColumn;
  private boolean hasCount;
  private String averageColumnName;
  private String sumColumnName;

  /**
   * Creates a new QueryPlan within transaction. The base table is startTableName.
   *
   * @param transaction the transaction containing this query
   * @param startTableName the source table for this query
   */
  public QueryPlan(Database.Transaction transaction, String startTableName) {
    this.transaction = transaction;
    this.startTableName = startTableName;

    this.projectColumns = new ArrayList<String>();
    this.joinTableNames = new ArrayList<String>();
    this.joinLeftColumnNames = new ArrayList<String>();
    this.joinRightColumnNames = new ArrayList<String>();

    this.selectColumnNames = new ArrayList<String>();
    this.selectOperators = new ArrayList<PredicateOperator>();
    this.selectDataBoxes = new ArrayList<DataBox>();

    this.hasCount = false;
    this.averageColumnName = null;
    this.sumColumnName = null;

    this.groupByColumn = null;

    this.finalOperator = null;
  }

  public QueryOperator getFinalOperator() {
    return this.finalOperator;
  }

  /**
   * Add a project operator to the QueryPlan with a list of column names. Can only specify one set
   * of projections.
   *
   * @param columnNames the columns to project
   * @throws QueryPlanException
   */
  public void project(List<String> columnNames) throws QueryPlanException {
    if (!this.projectColumns.isEmpty()) {
      throw new QueryPlanException("Cannot add more than one project operator to this query.");
    }

    if (columnNames.isEmpty()) {
      throw new QueryPlanException("Cannot project no columns.");
    }

    this.projectColumns = columnNames;
  }

  /**
   * Add a select operator. Only returns columns in which the column fulfills the predicate relative
   * to value.
   *
   * @param column the column to specify the predicate on
   * @param comparison the comparator
   * @param value the value to compare against
   * @throws QueryPlanException
   */
  public void select(String column, PredicateOperator comparison, DataBox value) throws QueryPlanException {
    this.selectColumnNames.add(column);
    this.selectOperators.add(comparison);
    this.selectDataBoxes.add(value);
  }

  /**
   * Set the group by column for this query.
   *
   * @param column the column to group by
   * @throws QueryPlanException
   */
  public void groupBy(String column) throws QueryPlanException {
    this.groupByColumn = column;
  }

  /**
   * Add a count aggregate to this query. Only can specify count(*).
   *
   * @throws QueryPlanException
   */
  public void count() throws QueryPlanException {
    this.hasCount = true;
  }

  /**
   * Add an average on column. Can only average over integer or float columns.
   *
   * @param column the column to average
   * @throws QueryPlanException
   */
  public void average(String column) throws QueryPlanException {
    this.averageColumnName = column;
  }

  /**
   * Add a sum on column. Can only sum integer or float columns
   *
   * @param column the column to sum
   * @throws QueryPlanException
   */
  public void sum(String column) throws QueryPlanException {
    this.sumColumnName = column;
  }

  /**
   * Join the leftColumnName column of the existing queryplan against the rightColumnName column
   * of tableName.
   *
   * @param tableName the table to join against
   * @param leftColumnName the join column in the existing QueryPlan
   * @param rightColumnName the join column in tableName
   */
  public void join(String tableName, String leftColumnName, String rightColumnName) {
    this.joinTableNames.add(tableName);
    this.joinLeftColumnNames.add(leftColumnName);
    this.joinRightColumnNames.add(rightColumnName);
  }

  //Returns a 2-array of table name, column name
  public String [] getJoinLeftColumnNameByIndex(int i){
    return this.joinLeftColumnNames.get(i).split("\\.");
  }

  //Returns a 2-array of table name, column name
  public String [] getJoinRightColumnNameByIndex(int i){
    return this.joinRightColumnNames.get(i).split("\\.");
  }


  /**
   * Generates a naïve QueryPlan in which all joins are at the bottom of the DAG followed by all select
   * predicates, an optional group by operator, and a set of projects (in that order).
   *
   * @return an iterator of records that is the result of this query
   * @throws DatabaseException
   * @throws QueryPlanException
   */
  public Iterator<Record> execute() throws DatabaseException, QueryPlanException {
    String indexColumn = this.checkIndexEligible();

    if (indexColumn != null) {
      this.generateIndexPlan(indexColumn);
    } else {
      // start off with the start table scan as the source
      this.finalOperator = new SequentialScanOperator(this.transaction, this.startTableName);

      this.addJoins();
      this.addSelects();
      this.addGroupBy();
      this.addProjects();
    }

    return this.finalOperator.execute();
  }

  /**
   * Generates an optimal QueryPlan based on the System R cost-based query optimizer.
   *
   * @return an iterator of records that is the result of this query
   * @throws DatabaseException
   * @throws QueryPlanException
   */
  public Iterator<Record> executeOptimal() throws DatabaseException, QueryPlanException {

    //TODO: HW4 Implement

    // Pass 1: Iterate through all single tables. For each single table, find
    // the lowest cost QueryOperator to access that table. Construct a mapping
    // of each table name to its lowest cost operator.


    // Pass i: On each pass, use the results from the previous pass to find the
    // lowest cost joins with each single table. Repeat until all tables have
    // been joined.


    // Get the lowest cost operator from the last pass, add GROUP BY and SELECT
    // operators, and return an iterator on the final operator


    return this.execute(); //TODO: HW4 Replace this!!! Allows you to test intermediate functionality

    //return ....
  }

  /**
   * Gets all SELECT predicates for which there exists an index on the column
   * referenced in that predicate for the given table.
   *
   * @return an ArrayList of SELECT predicates
   */
  private List<Integer> getEligibleIndexColumns(String table) {
    List<Integer> selectIndices = new ArrayList<Integer>();

    for (int i = 0; i < this.selectColumnNames.size(); i++) {
      String column = this.selectColumnNames.get(i);

      if (this.transaction.indexExists(table, column) &&
              this.selectOperators.get(i) != PredicateOperator.NOT_EQUALS) {
        selectIndices.add(i);
      }
    }

    return selectIndices;
  }

  /**
   * Gets all columns for which there exists an index for that table
   *
   * @return an ArrayList of column names
   */
  private List<String> getAllIndexColumns(String table) throws DatabaseException{
    List<String> indexColumns = new ArrayList<String>();

    Schema schema = this.transaction.getSchema(table);
    List<String> columnNames = schema.getFieldNames();

    for (int i = 0; i < columnNames.size(); i++) {
      String column = columnNames.get(i);

      if (this.transaction.indexExists(table, column)) {
        indexColumns.add(table + "." + column);
      }
    }

    return indexColumns;
  }

  /**
   * Applies all eligible SELECT predicates to a given source, except for the
   * predicate at index except. The purpose of except is because there might
   * be one SELECT predicate that was already used for an index scan, so no
   * point applying it again. A SELECT predicate is represented as elements of
   * this.selectColumnNames, this.selectOperators, and this.selectDataBoxes that
   * correspond to the same index of these lists.
   *
   * @return a new QueryOperator after SELECT has been applied
   * @throws DatabaseException
   * @throws QueryPlanException
   */
  private QueryOperator addEligibleSelections(QueryOperator source, int except) throws QueryPlanException, DatabaseException {

    for (int i = 0; i < this.selectColumnNames.size(); i++) {
      if (i == except) {
        continue;
      }

      PredicateOperator curPred = this.selectOperators.get(i);
      DataBox curValue = this.selectDataBoxes.get(i);
      try {
        String colName = source.checkSchemaForColumn(source.getOutputSchema(), selectColumnNames.get(i));
        source = new SelectOperator(source, colName, curPred, curValue);
      } catch (QueryPlanException err) {
        continue;
      }
    }

    return source;
  }

  /**
   * Finds the lowest cost QueryOperator that scans the given table. First
   * determine the cost of a sequential scan for the given table. Then for every index that can be
   * used on that table, determine the cost of an index scan. Keep track of
   * the minimum cost operation. Then push down eligible projects (SELECT
   * predicates). If an index scan was chosen, exclude that SELECT predicate when
   * pushing down selects. This method will be called during the first pass of the search
   * algorithm to determine the most efficient way to access each single table.
   *
   * @return a QueryOperator that has the lowest cost of scanning the given table which is
   * either a SequentialScanOperator or an IndexScanOperator nested within any possible
   * pushed down select operators
   * @throws DatabaseException
   * @throws QueryPlanException
   */
  public QueryOperator minCostSingleAccess(String table) throws DatabaseException, QueryPlanException {
    /**
     * TODO: HW4
     */

    QueryOperator minOp = null;

    minOp = new SequentialScanOperator(this.transaction, table);

    //TODO: HW4 Implement

    // 1. Find the cost of a sequential scan of the table

    // 2. For each eligible index column, find the cost of an index scan of the
    // table and retain the lowest cost operator


    // 3. Push down SELECT predicates that apply to this table and that were not
    // used for an index scan

    return minOp;
  }

  /**
   * Given a join condition between an outer relation represented by leftOp
   * and an inner relation represented by rightOp, find the lowest cost join
   * operator out of all the possible join types in JoinOperator.JoinType.
   *
   * @return lowest cost join QueryOperator between the input operators
   * @throws QueryPlanException
   */
  private QueryOperator minCostJoinType(QueryOperator leftOp,
                                        QueryOperator rightOp,
                                        String leftColumn,
                                        String rightColumn) throws QueryPlanException,
          DatabaseException {
    QueryOperator minOp = null;

    int minCost = Integer.MAX_VALUE;
    List<QueryOperator> allJoins = new ArrayList<QueryOperator>();
    allJoins.add(new SNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction));
    allJoins.add(new BNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction));

    for (QueryOperator join : allJoins) {
      int joinCost = join.estimateIOCost();
      if (joinCost < minCost) {
        minOp = join;
        minCost = joinCost;
      }
    }
    return minOp;
  }

  /**
   * Iterate through all table sets in the previous pass of the search. For each
   * table set, check each join predicate to see if there is a valid join
   * condition with a new table. If so, check the cost of each type of join and
   * keep the minimum cost join. Construct and return a mapping of each set of
   * table names being joined to its lowest cost join operator. A join predicate
   * is represented as elements of this.joinTableNames, this.joinLeftColumnNames,
   * and this.joinRightColumnNames that correspond to the same index of these lists.
   *
   * @return a mapping of table names to a join QueryOperator
   * @throws QueryPlanException
   */
  private Map<Set, QueryOperator> minCostJoins(Map<Set, QueryOperator> prevMap,
                                               Map<Set, QueryOperator> pass1Map) throws QueryPlanException,
          DatabaseException {

    /**
     * TODO: HW4
     */

    Map<Set, QueryOperator> map = new HashMap<Set, QueryOperator>();

    //TODO: HW4 Implement

    //We provide a basic description of the logic you have to implement

    //Input: prevMap (maps a set of tables to a query operator--the operator that joins the set)
    //Input: pass1Map (each set is a singleton with one table and single table access query operator)

    //FOR EACH set of tables in prevMap:

    //FOR EACH join condition listed in the query

    //get the left side and the right side (table name and column)

    /**
     * Case 1. Set contains left table but not right, use pass1Map to
     * fetch the right operator to access the rightTable
     *
     * Case 2. Set contains right table but not left, use pass1Map to
     * fetch the right operator to access the leftTable.
     *
     * Case 3. Set contains neither or both the left table or right table (contiue loop)
     *
     * --- Then given the operator, use minCostJoinType to calculate the cheapest join with that
     * and the previously joined tables.
     */

    /**
     * Create a new set that is the union of the new table and previously
     * joined tables. Add to result map this value mapping to the result from
     * minCostJoinType if it doesn't exist or it exists and cost is lower.
     */

    return map;
  }

  /**
   * Finds the lowest cost QueryOperator in the given mapping. A mapping is
   * generated on each pass of the search algorithm, and relates a set of tables
   * to the lowest cost QueryOperator accessing those tables. This method is
   * called at the end of the search algorithm after all passes have been
   * processed.
   *
   * @return a QueryOperator in the given mapping
   * @throws QueryPlanException
   */
  private QueryOperator minCostOperator(Map<Set, QueryOperator> map) throws QueryPlanException, DatabaseException {
    QueryOperator minOp = null;
    QueryOperator newOp;
    int minCost = Integer.MAX_VALUE;
    int newCost;
    for (Set tables : map.keySet()) {
      newOp = map.get(tables);
      newCost = newOp.getIOCost();
      if (newCost < minCost) {
        minOp = newOp;
        minCost = newCost;
      }
    }
    return minOp;
  }

  private String checkIndexEligible() {
    if (this.selectColumnNames.size() > 0
            && this.groupByColumn == null
            && this.joinTableNames.size() == 0) {

      int index = 0;
      for (String column : selectColumnNames) {
        if (this.transaction.indexExists(this.startTableName, column)) {
          if (this.selectOperators.get(index) != PredicateOperator.NOT_EQUALS) {
            return column;
          }
        }

        index++;
      }
    }

    return null;
  }

  private void generateIndexPlan(String indexColumn) throws QueryPlanException, DatabaseException {
    int selectIndex = this.selectColumnNames.indexOf(indexColumn);
    PredicateOperator operator = this.selectOperators.get(selectIndex);
    DataBox value = this.selectDataBoxes.get(selectIndex);

    this.finalOperator = new IndexScanOperator(this.transaction, this.startTableName, indexColumn, operator,
            value);

    this.selectColumnNames.remove(selectIndex);
    this.selectOperators.remove(selectIndex);
    this.selectDataBoxes.remove(selectIndex);

    this.addSelects();
    this.addProjects();
  }

  private void addJoins() throws QueryPlanException, DatabaseException {
    int index = 0;

    for (String joinTable : this.joinTableNames) {
      SequentialScanOperator scanOperator = new SequentialScanOperator(this.transaction, joinTable);

      SNLJOperator joinOperator = new SNLJOperator(finalOperator, scanOperator,
              this.joinLeftColumnNames.get(index), this.joinRightColumnNames.get(index), this.transaction); //changed from new JoinOperator

      this.finalOperator = joinOperator;
      index++;
    }
  }

  private void addSelects() throws QueryPlanException, DatabaseException {
    int index = 0;

    for (String selectColumn : this.selectColumnNames) {
      PredicateOperator operator = this.selectOperators.get(index);
      DataBox value = this.selectDataBoxes.get(index);

      SelectOperator selectOperator = new SelectOperator(this.finalOperator, selectColumn,
              operator, value);

      this.finalOperator = selectOperator;
      index++;
    }
  }

  private void addGroupBy() throws QueryPlanException, DatabaseException {
    if (this.groupByColumn != null) {
      if (this.projectColumns.size() > 2 || (this.projectColumns.size() == 1 &&
              !this.projectColumns.get(0).equals(this.groupByColumn))) {
        throw new QueryPlanException("Can only project columns specified in the GROUP BY clause.");
      }

      GroupByOperator groupByOperator = new GroupByOperator(this.finalOperator, this.transaction,
              this.groupByColumn);

      this.finalOperator = groupByOperator;
    }
  }

  private void addProjects() throws QueryPlanException, DatabaseException {
    if (!this.projectColumns.isEmpty() || this.hasCount || this.sumColumnName != null
            || this.averageColumnName != null) {
      ProjectOperator projectOperator = new ProjectOperator(this.finalOperator, this.projectColumns,
              this.hasCount, this.averageColumnName, this.sumColumnName);

      this.finalOperator = projectOperator;
    }
  }

}
