**DATAFRAMES**
- DataFrame definition need to be specified with Type (Generic: Row Type)
- With DataFrames, filter operations can be specified ONLY as **expressions (string expressions)**
![image](https://user-images.githubusercontent.com/39640906/117556726-f8e03600-b039-11eb-901b-8a7b227bb17d.png)

- With DataFrames, data type errors are found ONLY at run-time.
![image](https://user-images.githubusercontent.com/39640906/117556782-9c314b00-b03a-11eb-85c4-556f8e0e7abc.png)
orders_id is incorrect column name, but is not recognized by the compiler, so can lead to suprises during run-time.

**DATASETS**
- Dataset definition need to be specified with Custom Object Type
- Custom Object Type can be defined using ```case class <classname> (<attribute_name>: <datatype>, ...)```
![image](https://user-images.githubusercontent.com/39640906/117557146-0b5c6e80-b03e-11eb-98ee-2dd3a84c0d9a.png)
- With Datasets, filter operations can be specified as **expressions (string based or column based)**
![image](https://user-images.githubusercontent.com/39640906/117557186-668e6100-b03e-11eb-91be-4fb9785b1b0c.png)
- DataFrame is converted to Dataset by casting it to custom Object type
- Need to import <sparksession>.implicts._

**DataFrames are preferred than Datasets**
- to convert DataFrames to Datasets there is an overhead involved as it needs to be cast into a custom type
- For thi conversion, need Serialization of Data - converting data to binary format.
- **Serialization in DataFrames** are done using Tungsten binary format which is **FAST**
- **Serialization in Datasets** are done using Java serialization format which is **SLOW**
- Using datasets reduce developer mistakes as they are identified at Compile Time
- But they come at the cost of Casting and expensive Serialization.
