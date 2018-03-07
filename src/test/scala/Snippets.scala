import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._

class SparkSpec extends FlatSpec with Matchers {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("simpleSpark").setMaster("local")

  val sparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import sparkSession.implicits._


  "word count" should "work" in {
    /*
    dbutils.fs.put("/home/spark/1.6/lines",
      """
      Hello hello world
      Hello how are you world
      """, true)

    val ds = sqlContext.read.text("/home/spark/1.6/lines").as[String]
   */

    val ds =
      """
      Hello hello world
      Hello how are you world
      """.split("\n").toSeq.toDS

    val wordCount =
      ds
        .flatMap(_.split(" "))
        .filter(_ != "")
        .groupBy($"value")
        .count()
        .orderBy($"count".desc)


    wordCount.show

    //    +-----+-----+
    //    |value|count|
    //    +-----+-----+
    //    |Hello|    2|
    //    |world|    2|
    //    |  you|    1|
    //    |  how|    1|
    //    |hello|    1|
    //    |  are|    1|
    //    +-----+-----+

    val wordCount2 =
      ds
        .flatMap(_.split(" "))
        .filter(_ != "")
        .groupByKey(_.toLowerCase())
        .count()
        .orderBy($"count(1)".desc) // WTF

    wordCount2.show

    //    +-----+--------+
    //    |value|count(1)|
    //    +-----+--------+
    //    |hello|       3|
    //    |world|       2|
    //    |  you|       1|
    //    |  how|       1|
    //    |  are|       1|
    //    +-----+--------+
  }


  "dataframe count" should "work" in {
    val df =
      """
      Hello hello world
      Hello how are you world
      """.split("\n").toSeq.toDF

    //val df = sparkSession.read.text("hdfs://...")
    val wordsDF = df.select(split(df("value"), " ").alias("words"))
    val wordDF = wordsDF.select(explode(wordsDF("words")).alias("word"))
    val count = wordDF.groupBy(lower($"word")).count


    count.show
  }

  "select where" should "work" in {


    val ds =
      """
      Hello hello world
      Hello how are you world
      """.split("\n").toSeq.toDS

    ds
      .flatMap(_.split(" "))
      .filter(_ != "")
      .map(_.toLowerCase)
      .groupBy($"value")
      .count()
      .orderBy($"count".desc)
      .select($"value") // multiple fields allowed here
      .where("count >= 2")
      .show

    //    +-----+
    //    |value|
    //    +-----+
    //    |hello|
    //    |world|
    //    +-----+


  }

  "user defined function" should "work in dataset with a simple map (no dataframe UDF)" in {
    val upper: String => String = _.toUpperCase

    val ds =
      """
      Hello hello world
      Hello how are you world
      """.split("\n").toSeq.toDS

    ds.flatMap(_.split(" "))
      .map(upper)
      .filter(_ != "")
      .groupByKey(identity)
      .count()
      .orderBy($"count(1)".desc)
      .show

    // +-----+--------+
    //|value|count(1)|
    //+-----+--------+
    //|HELLO|       3|
    //|WORLD|       2|
    //|  HOW|       1|
    //|  YOU|       1|
    //|  ARE|       1|
    //+-----+--------+
  }

  "datasets and dataframes" should "work with Options" in {
    case class Person(name: Option[String], age: Int, gender: String, salary: Int, deptId: Int)

    val people = Seq(
      Person(Some("jane"), 28, "female", 2000, 2),
      Person(Some("bob"), 31, "male", 2000, 1),
      Person(Some("bob"), 35, "male", 2200, 1),
      Person(None, 45, "male", 3000, 1),
      Person(Some("joe"), 40, "male", 3000, 2),
      Person(Some("linda"), 37, "female", 3000, 1)
    ).toDS


    people.printSchema()

    //    root
    //    |-- name: string (nullable = true)
    //    |-- age: integer (nullable = false)
    //    |-- gender: string (nullable = true)
    //    |-- salary: integer (nullable = false)
    //    |-- deptId: integer (nullable = false)

    people.show

    //    +-----+---+------+------+------+
    //    | name|age|gender|salary|deptId|
    //    +-----+---+------+------+------+
    //    | jane| 28|female|  2000|     2|
    //    |  bob| 31|  male|  2000|     1|
    //    |  bob| 35|  male|  2200|     1|
    //    | null| 45|  male|  3000|     1| <-- null
    //    |  joe| 40|  male|  3000|     2|
    //    |linda| 37|female|  3000|     1|
    //    +-----+---+------+------+------+

    people.toDF.as[Person].collect().foreach(println)

    //    Person(Some(jane),28,female,2000,2)
    //    Person(Some(bob),31,male,2000,1)
    //    Person(Some(bob),35,male,2200,1)
    //    Person(None,45,male,3000,1) <-- None
    //    Person(Some(joe),40,male,3000,2)
    //    Person(Some(linda),37,female,3000,1)
  }

  "datasets and dataframes" should "work with Complex types" in {
    case class Department(id: Int, name: String)

    case class ComplexPerson(name: Option[String], age: Int, gender: String, salary: Int, departement: Option[Department])

    val peopleSeq = Seq(
      ComplexPerson(Some("jane"), 28, "female", 2000, Some(Department(2, "it"))),
      ComplexPerson(Some("bob"), 31, "male", 2000, None),
      ComplexPerson(Some("bob"), 35, "male", 2200, Some(Department(1, "rh"))),
      ComplexPerson(None, 45, "male", 3000, Some(Department(1, "rh"))),
      ComplexPerson(Some("joe"), 40, "male", 3000, Some(Department(1, "rh"))),
      ComplexPerson(Some("linda"), 37, "female", 3000, Some(Department(1, "rh")))
    )

    val peopleDS = peopleSeq.toDS

    val peopleDF = peopleDS.toDF

    peopleDF.printSchema()

    //    root
    //    |-- name: string (nullable = true)
    //    |-- age: integer (nullable = false)
    //    |-- gender: string (nullable = true)
    //    |-- salary: integer (nullable = false)
    //    |-- departement: struct (nullable = true)
    //    |    |-- id: integer (nullable = false)
    //    |    |-- name: string (nullable = true)


    peopleDF.show

    //    +-----+---+------+------+-----------+
    //    | name|age|gender|salary|departement|
    //    +-----+---+------+------+-----------+
    //    | jane| 28|female|  2000|     [2,it]|
    //    |  bob| 31|  male|  2000|       null| 
    //    |  bob| 35|  male|  2200|     [1,rh]|
    //    | null| 45|  male|  3000|     [1,rh]|
    //    |  joe| 40|  male|  3000|     [1,rh]|
    //    |linda| 37|female|  3000|     [1,rh]|
    //    +-----+---+------+------+-----------+

    peopleDF.as[ComplexPerson].collect should contain theSameElementsAs peopleSeq //true

  }

  "join" should "work" in {
    case class Person(name: String, age: Int, gender: String, salary: Int, deptId: Int)

    case class Department(id: Int, name: String)

    val departments = Seq(
      Department(1, "rh"),
      Department(2, "it"),
      Department(3, "marketing")
    ).toDS

    val people = Seq(
      Person("jane", 28, "female", 2000, 2),
      Person("bob", 31, "male", 2000, 1),
      Person("bob", 35, "male", 2200, 1),
      Person("john", 45, "male", 3000, 1),
      Person("joe", 40, "male", 3000, 2),
      Person("linda", 37, "female", 3000, 1)
    ).toDS


    // Note : example to create DS with a tuple :
    val dsTuple = Seq((1, "asdf"), (2, "34234"), (3, "45678")).toDS


    people.join(departments, people("deptId") === departments("id")).show
    //+-----+---+------+------+------+---+----+
    //    | name|age|gender|salary|deptId| id|name|
    //    +-----+---+------+------+------+---+----+
    //    | jane| 28|female|  2000|     2|  2|  it|
    //    |  bob| 31|  male|  2000|     1|  1|  rh|
    //    |  bob| 35|  male|  2200|     1|  1|  rh|
    //    | john| 45|  male|  3000|     1|  1|  rh|
    //    |  joe| 40|  male|  3000|     2|  2|  it|
    //    |linda| 37|female|  3000|     1|  1|  rh|
    //    +-----+---+------+------+------+---+----+

    people.joinWith(departments, people("deptId") === departments("id")).show

    //+--------------------+------+
    //    |                  _1|    _2|
    //    +--------------------+------+
    //    |[jane,28,female,2...|[2,it]|
    //    |[bob,31,male,2000,1]|[1,rh]|
    //    |[bob,35,male,2200,1]|[1,rh]|
    //    |[john,45,male,300...|[1,rh]|
    //    |[joe,40,male,3000,2]|[2,it]|
    //    |[linda,37,female,...|[1,rh]|
    //    +--------------------+------+


    departments.joinWith(people, departments("id") === people("deptId"), "left_outer").show

    //    +-------------+--------------------+
    //    |           _1|                  _2|
    //    +-------------+--------------------+
    //    |       [1,rh]|[linda,37,female,...|
    //    |       [1,rh]|[john,45,male,300...|
    //    |       [1,rh]|[bob,35,male,2200,1]|
    //    |       [1,rh]|[bob,31,male,2000,1]|
    //    |       [2,it]|[joe,40,male,3000,2]|
    //    |       [2,it]|[jane,28,female,2...|
    //    |[3,marketing]|                null| <--- WARNING : null
    //      +-------------+--------------------+
    //
    val safeJoin = departments.joinWith(people, departments("id") === people("deptId"), "left_outer").map { case (kv, nums) =>
      (kv, Option(nums))
    }

    safeJoin.collect should contain theSameElementsAs Array(
      (Department(1, "rh"), Some(Person("linda", 37, "female", 3000, 1))),
      (Department(1, "rh"), Some(Person("john", 45, "male", 3000, 1))),
      (Department(1, "rh"), Some(Person("bob", 35, "male", 2200, 1))),
      (Department(1, "rh"), Some(Person("bob", 31, "male", 2000, 1))),
      (Department(2, "it"), Some(Person("joe", 40, "male", 3000, 2))),
      (Department(2, "it"), Some(Person("jane", 28, "female", 2000, 2))),
      (Department(3, "marketing"), None)
    ) // true

  }

  "join" should "work with Frameless TypedDataset" in {
    case class Person(name: String, age: Int, gender: String, salary: Int, deptId: Int)

    case class Department(id: Int, name: String)

    import frameless.TypedDataset

    implicit val sqlContext = sparkSession.sqlContext

    val departments = TypedDataset.create(Seq(
      Department(1, "rh"),
      Department(2, "it"),
      Department(3, "marketing")
    ))

    val people = TypedDataset.create(Seq(
      Person("jane", 28, "female", 2000, 2),
      Person("bob", 31, "male", 2000, 1),
      Person("bob", 35, "male", 2200, 1),
      Person("john", 45, "male", 3000, 1),
      Person("joe", 40, "male", 3000, 2),
      Person("linda", 37, "female", 3000, 1)
    ))

    val joined = people.join(departments, people('deptId), departments('id))

    // val joined = people.join(departments, people('detppid), departments('id)) <-- Won't compile as 'detppid symbol is wrong

    joined.dataset.show

    //    +--------------------+------+
    //    |                  _1|    _2|
    //    +--------------------+------+
    //    |[jane,28,female,2...|[2,it]|
    //    |[bob,31,male,2000,1]|[1,rh]|
    //    |[bob,35,male,2200,1]|[1,rh]|
    //    |[john,45,male,300...|[1,rh]|
    //    |[joe,40,male,3000,2]|[2,it]|
    //    |[linda,37,female,...|[1,rh]|
    //    +--------------------+------+


    val leftJoined: TypedDataset[(Department, Option[Person])] = departments.joinLeft(people, departments('id), people('deptId))

  }

  "aggregate" should "work" in {
    case class Person(name: String, age: Int, gender: String, salary: Int, deptId: Int)

    case class Department(id: Int, name: String)

    val departments = Seq(
      Department(1, "rh"),
      Department(2, "it"),
      Department(3, "marketing")
    ).toDS

    val people = Seq(
      Person("jane", 28, "female", 2000, 2),
      Person("bob", 31, "male", 2000, 1),
      Person("bob", 35, "male", 2200, 1),
      Person("john", 45, "male", 3000, 1),
      Person("joe", 40, "male", 3000, 2),
      Person("linda", 37, "female", 3000, 1)
    ).toDS

    people.filter(_.age > 30)
      .join(departments, people("deptId") === departments("id"))
      .show

    //    +-----+---+------+------+------+---+----+
    //    | name|age|gender|salary|deptId| id|name|
    //    +-----+---+------+------+------+---+----+
    //    |  bob| 31|  male|  2000|     1|  1|  rh|
    //    |  bob| 35|  male|  2200|     1|  1|  rh|
    //    | john| 45|  male|  3000|     1|  1|  rh|
    //    |  joe| 40|  male|  3000|     2|  2|  it|
    //    |linda| 37|female|  3000|     1|  1|  rh|
    //    +-----+---+------+------+------+---+----+

    import org.apache.spark.sql.functions._

    //AVERAGE AND MAX :

    people.filter("age > 30")
      .join(departments, people("deptId") === departments("id"))
      .groupBy(departments("name"), $"gender")
      .agg(avg(people("salary")), max(people("age")))
      .show

    //    +----+------+-----------+--------+
    //    |name|gender|avg(salary)|max(age)|
    //    +----+------+-----------+--------+
    //    |  rh|  male|     2400.0|      45|
    //    |  rh|female|     3000.0|      37|
    //    |  it|  male|     3000.0|      40|
    //    +----+------+-----------+--------+

    //COUNT / SUM :

    departments
      .join(people, departments("id") === people("deptId"))
      .groupBy(departments("name"))
      .agg(count(people("name")), sum(people("salary"))) // or count(people("*"). N.B. : multiple values allowed on count (to count tuples values)
      .show

    //    +----+-----------+-----------+
    //    |name|count(name)|sum(salary)|
    //    +----+-----------+-----------+
    //    |  it|          2|       5000|
    //    |  rh|          4|      10200|
    //    +----+-----------+-----------+

    departments
      .join(people, departments("id") === people("deptId"))
      .groupBy(departments("name"))
      .agg(countDistinct(people("name")))
      .show

    //    +----+--------------------+
    //    |name|count(DISTINCT name)|
    //    +----+--------------------+
    //    |  it|                   2|
    //    |  rh|                   3|
    //    +----+--------------------+

    departments
      .join(people, departments("id") === people("deptId"), "left_outer")
      .groupBy(departments("name"))
      .agg(countDistinct(people("name")))
      .show

    //    +---------+--------------------+
    //    |     name|count(DISTINCT name)|
    //    +---------+--------------------+
    //    |marketing|                   0| <-- here we can coun't values that don't match thanks to left_outer
    //    |       it|                   2|
    //    |       rh|                   3|
    //    +---------+--------------------+

  }

  // JUST to compare with datasets
  "join/count" should "work with rdd" in {
    case class Person(name: String, age: Int, gender: String, salary: Int, deptId: Int)

    case class Department(id: Int, name: String)

    val departments = sparkSession.sparkContext.parallelize(Seq(
      Department(1, "rh"),
      Department(2, "it"),
      Department(3, "marketing")
    ))

    val people = sparkSession.sparkContext.parallelize(Seq(
      Person("jane", 28, "female", 2000, 2),
      Person("bob", 31, "male", 2000, 1),
      Person("bob", 35, "male", 2200, 1),
      Person("john", 45, "male", 3000, 1),
      Person("joe", 40, "male", 3000, 2),
      Person("linda", 37, "female", 3000, 1)
    ))

    val departmentsById = departments.map { department =>
      (department.id, department)
    }

    val peopleByDepartmentId = people.map { person =>
      (person.deptId, person)
    }

    peopleByDepartmentId.join(departmentsById).toDF.show
    //+---+--------------------+
    //    | _1|                  _2|
    //    +---+--------------------+
    //    |  1|[[bob,31,male,200...|
    //    |  1|[[bob,35,male,220...|
    //    |  1|[[john,45,male,30...|
    //    |  1|[[linda,37,female...|
    //    |  2|[[jane,28,female,...|
    //    |  2|[[joe,40,male,300...|
    //    +---+--------------------+

    peopleByDepartmentId.join(departmentsById)
      .map { case (deptId, (person, department)) =>
        (department.name, 1)
      }.reduceByKey((accu, current) => accu + current)
      .toDF("department", "count").show

    //    +----------+-----+
    //    |department|count|
    //    +----------+-----+
    //    |        it|    2|
    //    |        rh|    4|
    //    +----------+-----+


  }

}
