**Convert RDD to Dataset using Regex - Input file: Not completely structured data**
- When dealing with **Not so structured data**, the best practice is to
- Read the data into an RDD, apply the Low Level Transformations to impose a structure on top of the RDD.
- Convert them to DataFrame(/Dataset) and apply the High-Level transformations to derive the required OLAP output.
- Sample data:
```
1 2013-07-25	11599,CLOSED
2 2013-07-25	256,PENDING_PAYMENT
```
```
//Define a Regex to match each record in the Dataset
  val ordersRegex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r

  //Create a case class to convert each line to a custom Object
  case class Orders(order_id:Int, order_customer_id:Int, order_status:String)

  //Function definition to convert record of RDD to Row of custom object type
  def lineParser(line: String) = {
    line match { //similar to switch case statement in python
      case ordersRegex(order_id, order_date, order_customer_id ,order_status) => \\ upon match the segments identified by the regex are assigned to variables defined in the case statement 
        Orders(order_id.toInt, order_customer_id.toInt, order_status) \\upon match an object of the Orders case class is created and variables are passed as defined by the class 
    }
```
- rdd[Row: object type].toDs => converts RDD to Dataset
- cache is used so the costly operations performed using the regex are not repeated in the future operations

