// Databricks notebook source
// MAGIC %md
// MAGIC #Spark SQL and Datasets

// COMMAND ----------

// MAGIC %md
// MAGIC ##Single API for Java and Scala
// MAGIC 
// MAGIC Datasets offer a unified and singular API for strongly typed objects. Among the languages supported by Spark, only Scala and Java are strongly typed; hence, Python and R support only the untyped DataFrame API.
// MAGIC 
// MAGIC Thanks to this singular API, Java developers no longer risk lagging behind. For example, any future interface or behavior changes to Scala’s groupBy(), flatMap(), map(), or filter() API will be the same for Java too, because it’s a singular interface that is
// MAGIC common to both implementations.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Scala Case Classes and JavaBeans for Datasets
// MAGIC Spark has internal data types, such as String Type,BooleanType, that it uses to map seamlessly to the language-specific data types in Scala and Java during Spark operations. . This mapping is done via encoders
// MAGIC 
// MAGIC In order to create Dataset[T], where T is your typed object in Scala, you need a case
// MAGIC class that defines the object. 
// MAGIC 
// MAGIC To create a distributed Dataset[Bloggers], we must first define a Scala case class that defines each individual field that comprises a Scala object. This case class serves as a blueprint or schema for the typed object Bloggers:

// COMMAND ----------

// In Scala
case class Bloggers(id:Int, first:String, last:String, url:String, date:String,
hits: Int, campaigns:Array[String])
val bloggers = "Filestore/Tables/bloggers.json"
val bloggersDS = spark
 .read
 .format("json")
 .option("path", bloggers)
 .load()
 .as[Bloggers]


// COMMAND ----------

// MAGIC %md
// MAGIC Similarly, you can create a JavaBean class of type Bloggers in Java and then use
// MAGIC encoders to create a Dataset<Bloggers>:
// MAGIC   
// MAGIC   

// COMMAND ----------

// In Java
  import org.apache.spark.sql.Encoders;
  import java.io.Serializable;
  public class Bloggers implements Serializable {
    private int id;
    private String first;
    private String last;
    private String url;
    private String date;
    private int hits;
    private Array[String] campaigns;
  // JavaBean getters and setters
    int getID() { return id; }
    void setID(int i) { id = i; }
    String getFirst() { return first; }
    void setFirst(String f) { first = f; }
    String getLast() { return last; }
    void setLast(String l) { last = l; }
    String getURL() { return url; }
    void setURL (String u) { url = u; }
    String getDate() { return date; }
    Void setDate(String d) { date = d; }
    int getHits() { return hits; }
    void setHits(int h) { hits = h; }
    Array[String] getCampaigns() { return campaigns; }
    void setCampaigns(Array[String] c) { campaigns = c; }
  }

// Create Encoder
Encoder<Bloggers> BloggerEncoder = Encoders.bean(Bloggers.class);
String bloggers = "../bloggers.json"
Dataset<Bloggers>bloggersDS = spark
 .read
 .format("json")
 .option("path", bloggers)
 .load()
 .as(BloggerEncoder);

// COMMAND ----------

// MAGIC %md
// MAGIC As you can see, creating Datasets in Scala and Java requires a bit of forethought, as you have to know all the individual column names and types for the rows you are reading.
// MAGIC 
// MAGIC Unlike with DataFrames, where you can optionally let Spark infer the schema, the Dataset API requires that you define your data types ahead of time and that your case class or JavaBean class matches your schema.

// COMMAND ----------

// MAGIC %md
// MAGIC ##Working with Datasets
// MAGIC One simple and dynamic way to create a sample Dataset is using a SparkSession
// MAGIC instance. In this scenario, for illustration purposes, we dynamically create a Scala
// MAGIC object with three fields: uid (unique ID for a user), uname (randomly generated username string), and usage (minutes of server or service usage).

// COMMAND ----------

// MAGIC %md
// MAGIC ###Creating Sample Data

// COMMAND ----------

// In Scala
import scala.util.Random._
// Our case class for the Dataset
case class Usage(uid:Int, uname:String, usage: Int)
val r = new scala.util.Random(42)
// Create 1000 instances of scala Usage class 
// This generates data on the fly
val data = for (i <- 0 to 1000)
 yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
 r.nextInt(1000)))
// Create a Dataset of Usage typed data
val dsUsage = spark.createDataset(data)
dsUsage.show(10)


// COMMAND ----------

// MAGIC %md
// MAGIC In Java the idea is similar, but we have to use explicit Encoders (in Scala, Spark handles this implicitly):

// COMMAND ----------

// In Java
import org.apache.spark.sql.Encoders;
import org.apache.commons.lang3.RandomStringUtils;
import java.io.Serializable;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
// Create a Java class as a Bean
public class Usage implements Serializable {
 int uid; // user id
 String uname; // username
 int usage; // usage
 public Usage(int uid, String uname, int usage) {
 this.uid = uid;
 this.uname = uname;
 this.usage = usage;
 }
 // JavaBean getters and setters 
 public int getUid() { return this.uid; }
 public void setUid(int uid) { this.uid = uid; }
 public String getUname() { return this.uname; }
 public void setUname(String uname) { this.uname = uname; }
 public int getUsage() { return this.usage; }
 public void setUsage(int usage) { this.usage = usage; }
 public Usage() {
 }
 public String toString() {
 return "uid: '" + this.uid + "', uame: '" + this.uname + "', 
 usage: '" + this.usage + "'";
 }
}
// Create an explicit Encoder 
Encoder<Usage> usageEncoder = Encoders.bean(Usage.class);
Random rand = new Random();
rand.setSeed(42);
List<Usage> data = new ArrayList<Usage>()
// Create 1000 instances of Java Usage class 
for (int i = 0; i < 1000; i++) {
 data.add(new Usage(i, "user" +
 RandomStringUtils.randomAlphanumeric(5),
 rand.nextInt(1000));
 
// Create a Dataset of Usage typed data
Dataset<Usage> dsUsage = spark.createDataset(data, usageEncoder);

// COMMAND ----------

// MAGIC %md
// MAGIC ###Transforming Sample Data
// MAGIC Recall that Datasets are strongly typed collections of domain-specific objects. These
// MAGIC objects can be transformed in parallel using functional or relational operations.
// MAGIC Examples of these transformations include map(), reduce(), filter(), select(),
// MAGIC and aggregate().
// MAGIC 
// MAGIC For a simple example, let’s use filter() to return all the users in our dsUsage Dataset whose usage exceeds 900 minutes. One way to do this is to use a functional expression as an argument to the filter() method:

// COMMAND ----------

// In Scala
import org.apache.spark.sql.functions._
dsUsage
 .filter(d => d.usage > 900)
 .orderBy(desc("usage"))
 .show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Another way is to define a function and supply that function as an argument to
// MAGIC filter():

// COMMAND ----------

def filterWithUsage(u: Usage) = u.usage > 900
dsUsage.filter(filterWithUsage(_)).orderBy(desc("usage")).show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC In the first case we used a lambda expression, {d.usage > 900}, as an argument to
// MAGIC the filter() method, whereas in the second case we defined a Scala function, def
// MAGIC filterWithUsage(u: Usage) = u.usage > 900.
// MAGIC 
// MAGIC In both cases, the filter() method
// MAGIC iterates over each row of the Usage object in the distributed Dataset and applies the
// MAGIC expression or executes the function, returning a new Dataset of type Usage for rows
// MAGIC where the value of the expression or function is true.
// MAGIC 
// MAGIC In Java, the argument to filter() is of type FilterFunction<T>. This can be defined
// MAGIC either inline anonymously or with a named function. For example:

// COMMAND ----------

// In Java
// Define a Java filter function
FilterFunction<Usage> f = new FilterFunction<Usage>() {
 public boolean call(Usage u) {
 return (u.usage > 900);
 }
};
// Use filter with our function and order the results in descending order
dsUsage.filter(f).orderBy(col("usage").desc()).show(5);

// COMMAND ----------

// MAGIC %md
// MAGIC Not all lambdas or functional arguments must evaluate to Boolean values; they can
// MAGIC return computed values too. Consider this example using the higher-order function
// MAGIC map(), where our aim is to find out the usage cost for each user whose usage value is
// MAGIC over a certain threshold so we can offer those users a special price per minute.

// COMMAND ----------

// In Scala
// Use an if-then-else lambda expression and compute a value
dsUsage.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50 })
 .show(5, false)


// Define a function to compute the usage
def computeCostUsage(usage: Int): Double = {
 if (usage > 750) usage * 0.15 else usage * 0.50
}
// Use the function as an argument to map()
dsUsage.map(u => {computeCostUsage(u.usage)}).show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC To use map() in Java, you have to define a MapFunction<T>. This can either be an
// MAGIC anonymous class or a defined class that extends MapFunction<T>. For this example,
// MAGIC we use it inline—that is, in the method call itself:

// COMMAND ----------

// In Java
// Define an inline MapFunction
dsUsage.map((MapFunction<Usage, Double>) u -> {
 if (u.usage > 750)
 return u.usage * 0.15;
 else
 return u.usage * 0.50;
}, Encoders.DOUBLE()).show(5); // We need to explicitly specify the Encoder

// COMMAND ----------

// MAGIC %md
// MAGIC Though we have computed values for the cost of usage, we don’t know which users
// MAGIC the computed values are associated with. How do we get this information?
// MAGIC The steps are simple:
// MAGIC 1. Create a Scala case class or JavaBean class, UsageCost, with an additional field or column named cost.
// MAGIC 2. Define a function to compute the cost and use it in the map() method.

// COMMAND ----------

// In Scala
// Create a new case class with an additional field, cost
case class UsageCost(uid: Int, uname:String, usage: Int, cost: Double)
// Compute the usage cost with Usage as a parameter
// Return a new object, UsageCost
def computeUserCostUsage(u: Usage): UsageCost = {
 val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
 UsageCost(u.uid, u.uname, u.usage, v)
}
// Use map() on our original Dataset
dsUsage.map(u => {computeUserCostUsage(u)}).show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC in Java, if we want the cost associated with each user we need to define a
// MAGIC JavaBean class UsageCost and MapFunction<T>.

// COMMAND ----------

// In Java
// Get the Encoder for the JavaBean class
Encoder<UsageCost> usageCostEncoder = Encoders.bean(UsageCost.class);
// Apply map() function to our data
dsUsage.map( (MapFunction<Usage, UsageCost>) u -> {
 double v = 0.0;
 if (u.usage > 750) v = u.usage * 0.15; else v = u.usage * 0.50;
 return new UsageCost(u.uid, u.uname,u.usage, v); },
 usageCostEncoder).show(5);

// COMMAND ----------

// MAGIC %md
// MAGIC ###Converting DataFrames to Datasets
// MAGIC To convert an existing DataFrame df to a Dataset of type SomeCaseClass,
// MAGIC simply use the df.as[SomeCaseClass] notation. We saw an example of this earlier:

// COMMAND ----------

// In Scala
val bloggersDS = spark
 .read
 .format("json")
 .option("path", "Filestore/Tables/bloggers.json")
 .load()
 .as[Bloggers]

// COMMAND ----------

// MAGIC %md
// MAGIC ##Dataset Encoders
// MAGIC Encoders serialize and deserialize Dataset objects from
// MAGIC Spark’s internal format to JVM objects, including primitive data types. Spark has built-in support for automatically generating encoders for primitive types(e.g., string, integer, long).  Compared to Java and Kryo serialization and deserialization, Spark encoders are significantly faster.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Spark’s Internal Format Versus Java Object Format
// MAGIC Java objects have large overheads—header info, hashcode, Unicode info, etc. Even a
// MAGIC simple Java string such as “abcd” takes 48 bytes of storage, instead of the 4 bytes you
// MAGIC might expect. Imagine the overhead to create, for example, a MyClass(Int, String,
// MAGIC String) object
// MAGIC 
// MAGIC Instead of creating JVM-based objects for Datasets or DataFrames, Spark employs encoders to convert the data from in-memory representation to JVM object.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Serialization and Deserialization (SerDe)
// MAGIC A concept not new in distributed computing, where data frequently travels over the network among computer nodes in a cluster, serialization and deserialization is the process by which a typed object is encoded (serialized) into a binary presentation or format by the sender and decoded (deserialized) from binary format into its respective data-typed object by the receiver.
// MAGIC 
// MAGIC The JVM has its own built-in Java serializer and deserializer, but it’s inefficient because the Java objects created by the JVM in the heap memory are bloated. Hence, the process is slow. This is where the Dataset encoders come to the rescue, for a few reasons:
// MAGIC 
// MAGIC - Spark’s internal Tungsten binary format stores objects off the Java heap memory, and it’s compact so those objects occupy less space
// MAGIC - Encoders can quickly serialize by traversing across the memory using simple pointer arithmetic with memory addresses and offsets 
// MAGIC - On the receiving end, encoders can quickly deserialize the binary representation into Spark’s internal representation. Encoders are not hindered by the JVM’s garbage collection pauses

// COMMAND ----------

// MAGIC %md
// MAGIC ##Costs of Using Datasets
// MAGIC In “DataFrames Versus Datasets”, we outlined some of the benefits of using Datasets—but these benefits come at a cost. As noted in the preceding section, when Datasets are passed to higher-order functions such as filter(), map(), or flatMap() that take lambdas and functional arguments, there is a cost associated with deserializing from Spark’s internal Tungsten format into the JVM object.
// MAGIC 
// MAGIC Compared to other serializers used before encoders were introduced in Spark, this cost is minor and tolerable. However, over larger data sets and many queries, this cost accrues and can affect performance.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Strategies to Mitigate Costs
// MAGIC One strategy to mitigate excessive serialization and deserialization is to use
// MAGIC DSL expressions in your queries and avoid excessive use of lambdas as anonymous
// MAGIC functions as arguments to higher-order functions
// MAGIC 
// MAGIC Because lambdas are anonymous and opaque to the Catalyst optimizer until runtime, when you use them it cannot efficiently discern what you’re doing (you’re not telling Spark what to do)
// MAGIC 
// MAGIC The second strategy is to chain your queries together in such a way that serialization
// MAGIC and deserialization is minimized. Chaining queries together is a common practice in
// MAGIC Spark.
// MAGIC 
// MAGIC Let’s illustrate with a simple example. 

// COMMAND ----------

// In Scala
Person(id: Integer, firstName: String, middleName: String, lastName: String,
gender: String, birthDate: String, ssn: String, salary: String)

// COMMAND ----------

// MAGIC %md
// MAGIC Let’s examine a case where we compose a query inefficiently, in such a way that we
// MAGIC unwittingly incur the cost of repeated serialization and deserialization:

// COMMAND ----------

// In Scala
Person(id: Integer, firstName: String, middleName: String, lastName: String,
gender: String, birthDate: String, ssn: String, salary: String)
// Everyone above 40: lambda-1
 .filter(x => x.birthDate.split("-")(0).toInt > earliestYear)
 
 // Everyone earning more than 80K
 .filter($"salary" > 80000)
// Last name starts with J: lambda-2
 .filter(x => x.lastName.startsWith("J"))
 
 // First name starts with D
 .filter($"firstName".startsWith("D"))
 .count()

// COMMAND ----------

// MAGIC %md
// MAGIC By contrast, the following query uses only DSL and no lambdas. As a result, it’s much
// MAGIC more efficient—no serialization/deserialization is required for the entire composed
// MAGIC and chained query

// COMMAND ----------

personDS
 .filter(year($"birthDate") > earliestYear) // Everyone above 40
 .filter($"salary" > 80000) // Everyone earning more than 80K
 .filter($"lastName".startsWith("J")) // Last name starts with J
 .filter($"firstName".startsWith("D")) // First name starts with D
 .count()