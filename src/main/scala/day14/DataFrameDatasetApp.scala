package day14

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Day 14 — DataFrame and Dataset
 *
 * Covers:
 *   1. A case class, and converting a DataFrame to a Dataset
 *   2. Converting a Dataset back to a DataFrame
 *   3. Comparing RDD, DataFrame and Dataset
 *   4. Type safety and Catalyst optimization, shown with real .explain() plans
 *   5. Scenario: a typed employee payroll pipeline
 *
 * Run with:
 *   sbt "run local[2]"
 *   sbt "run local[4]"
 */
object DataFrameDatasetApp {

  def main(args: Array[String]): Unit = {
    val masterUrl = if (args.nonEmpty) args(0) else "local[*]"

    val spark = SparkSession.builder()
      .appName("Day14-DataFrameDatasetApp")
      .master(masterUrl)
      .getOrCreate()

    section("1. A case class, and DataFrame -> Dataset")
    val employeesDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/employees.csv")

    // .as[Employee] converts an untyped DataFrame (Dataset[Row]) into a
    // typed Dataset[Employee]. Spark checks column NAMES and TYPES against
    // the case class right here - a mismatch throws immediately, not deep
    // inside some later transformation.
    import employeesDf.sparkSession.implicits._
    val employeesDs: Dataset[Employee] = employeesDf.as[Employee]

    println(s"employeesDf (DataFrame, rows are untyped Row): ${employeesDf.count()} rows")
    println(s"employeesDs (Dataset[Employee], rows are typed objects): ${employeesDs.count()} rows")
    println("First typed row (a real Employee object, with IDE/compiler-checked fields):")
    println(s"  ${employeesDs.head()}")

    section("2. Dataset back to DataFrame")
    val backToDf: DataFrame = employeesDs.toDF()
    println(s"employeesDs.toDF() -> a plain DataFrame again, columns: ${backToDf.columns.mkString(", ")}")
    println("Useful whenever you need the untyped column API again - e.g. to join, pivot,")
    println("or hand the result to code that expects a DataFrame rather than a specific case class.")

    section("3. Comparing RDD, DataFrame and Dataset")
    compareRddDataFrameDataset(spark, employeesDf, employeesDs)

    section("4. Type safety and Catalyst optimization")
    typeSafetyAndCatalyst(employeesDf, employeesDs)

    section("5. Scenario: typed employee payroll pipeline")
    payrollScenario(employeesDs)

    spark.stop()
  }

  // ---------------------------------------------------------------------
  // 3. RDD vs DataFrame vs Dataset
  // ---------------------------------------------------------------------
  def compareRddDataFrameDataset(spark: SparkSession, df: DataFrame, ds: Dataset[Employee]): Unit = {
    val rdd = df.rdd // every DataFrame/Dataset can drop down to its underlying RDD[Row]

    println(s"Same data, three APIs:")
    println(s"  RDD[Row]           : ${rdd.count()} elements, NO schema attached to the RDD itself")
    println(s"  DataFrame (Row)    : ${df.count()} rows, schema = ${df.schema.simpleString}")
    println(s"  Dataset[Employee]  : ${ds.count()} rows, same schema, PLUS a JVM type (Employee) per row")

    println(
      """
        |                     RDD              DataFrame           Dataset[T]
        |                 ---------------  -----------------  -------------------
        |Type safety      compile-time     NONE (runtime      compile-time for
        |                 (whatever T is)  AnalysisException  T's own fields;
        |                                  on a bad column    column-string API
        |                                  name/type)         still untyped
        |
        |Optimization     NONE - just      FULL Catalyst +     Catalyst/Tungsten
        |                 JVM objects +    Tungsten (query     for column-based
        |                 your code        planning, codegen,  ops; REDUCED for
        |                                  off-heap storage)   map/filter lambdas
        |                                                      (see section 4)
        |
        |Ease of use      most flexible,   SQL-like, concise   case-class
        |                 most manual      for tabular work    convenience +
        |                                                      tabular power
        |
        |Best for         unstructured /   large tabular ETL,  business-logic-
        |                 custom data,     joins, aggregation  heavy row-wise
        |                 fine-grained     at scale            transforms with
        |                 control                              compile-time safety
        |""".stripMargin
    )
  }

  // ---------------------------------------------------------------------
  // 4. Type safety + Catalyst, demonstrated with real .explain() plans
  // ---------------------------------------------------------------------
  def typeSafetyAndCatalyst(df: DataFrame, ds: Dataset[Employee]): Unit = {
    println(
      """TYPE SAFETY
        | - DataFrame: df.filter(col("baseSallary") > 50000) - a COLUMN NAME
        |   TYPO - compiles fine. It only fails at RUNTIME, with an
        |   AnalysisException, when the query actually runs.
        | - Dataset[Employee]: ds.filter(_.baseSalary > 50000) accesses a
        |   real Scala field. Typo the field name and it's a COMPILE ERROR -
        |   caught before the code ever runs, let alone ships.
        |""".stripMargin)

    println("Proof: an intentional column-name typo on the DataFrame API fails only at runtime:")
    try {
      df.filter(col("baseSallary") > 50000).count() // "baseSallary" - deliberate typo
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        println(s"  Caught at RUNTIME: ${e.getMessage.linesIterator.next()}")
    }
    println("  (The equivalent typo on ds.filter(_.baseSallary > 50000) would instead be a")
    println("   COMPILE error - this file wouldn't even build. That line is commented out here")
    println("   on purpose, since a real compile error would stop sbt from building this project.)")

    println(
      """
        |CATALYST OPTIMIZATION
        |Every DataFrame/Dataset query goes through four phases:
        | 1. Unresolved logical plan  - parsed from your code/SQL, column
        |    names not yet checked against anything.
        | 2. Analyzed logical plan    - names/types resolved against the
        |    schema/catalog (this is where a bad column name actually fails).
        | 3. Optimized logical plan   - rule-based rewrites: predicate
        |    pushdown, constant folding, column pruning, projection pushdown.
        | 4. Physical plan            - Spark picks a concrete execution
        |    strategy (e.g. broadcast join vs shuffle join) and compiles it
        |    to JVM bytecode via whole-stage code generation (Tungsten),
        |    avoiding virtual calls and extra object overhead.
        |
        |IMPORTANT NUANCE: Catalyst can only optimize operations it can see
        |INSIDE of - Column expressions like col("x") > 5. A Scala lambda
        |passed to .map/.filter on a Dataset is an OPAQUE black box to the
        |optimizer: Spark must deserialize each row to a real Employee
        |object, run your function, then re-serialize the result. Compare
        |the physical plans below:
        |""".stripMargin)

    println("Plan A: df.filter(col(\"baseSalary\") > 50000)  <- a Column expression, fully optimizable")
    df.filter(col("baseSalary") > 50000).explain()

    println("\nPlan B: ds.filter(emp => emp.baseSalary > 50000)  <- a Scala lambda, opaque to Catalyst")
    ds.filter(emp => emp.baseSalary > 50000).explain()

    println(
      """Look for "DeserializeToObject" / "SerializeFromObject" / "Filter (typed)" wrapping
        |the lambda version - that's Spark stepping OUT of its optimized columnar format
        |and back into plain JVM objects just to run your function, then back again.
        |The Column-expression version has no such step: the filter stays inside
        |Catalyst/Tungsten the whole time. This is why, in practice, column
        |expressions and SQL are preferred for the bulk, data-shape-changing work,
        |while typed Dataset lambdas are reserved for business logic that's easiest
        |to express as plain Scala code.
        |""".stripMargin)
  }

  // ---------------------------------------------------------------------
  // 5. Scenario: typed employee payroll pipeline
  // ---------------------------------------------------------------------
  def payrollScenario(employeesDs: Dataset[Employee]): Unit = {
    import employeesDs.sparkSession.implicits._

    // Typed, compile-time-checked business logic: easiest to express as a
    // plain case-class-to-case-class transformation.
    val payroll: Dataset[PayrollRecord] = employeesDs.map { e =>
      val grossPay = e.baseSalary + e.bonus - e.deductions
      val netPay = grossPay * (1 - e.taxRate)
      PayrollRecord(e.employeeId, e.name, e.department, grossPay, netPay)
    }

    println("Sample payroll records (typed, from a plain Scala function via .map):")
    payroll.show(5, truncate = false)

    // Back to the column/SQL API for the aggregation step - this is the
    // part that benefits most from Catalyst/Tungsten, so we use it here
    // rather than a typed groupByKey/reduce.
    println("Department payroll summary (column-based aggregation, fully optimized):")
    val deptSummary = payroll
      .groupBy("department")
      .agg(
        count("*").as("headcount"),
        round(sum("grossPay"), 2).as("totalGross"),
        round(sum("netPay"), 2).as("totalNet"),
        round(avg("netPay"), 2).as("avgNetPay")
      )
      .orderBy(desc("totalNet"))

    deptSummary.show(truncate = false)

    val totalNetPayroll = payroll.agg(round(sum("netPay"), 2).as("total")).first().getAs[Double]("total")
    println(f"Total net payroll across the company: $totalNetPayroll%,.2f")

    // Converting back to a DataFrame to register as a view, e.g. for
    // downstream SQL reporting or hand-off to another team's pipeline.
    payroll.toDF().createOrReplaceTempView("payroll")
    println("\npayroll.toDF() registered as a temp view - SQL can now query it directly:")
    employeesDs.sparkSession.sql(
      "SELECT department, COUNT(*) AS headcount FROM payroll GROUP BY department ORDER BY headcount DESC"
    ).show()
  }

  def section(title: String): Unit = {
    println("\n" + "=" * 70)
    println(title)
    println("=" * 70)
  }
}
