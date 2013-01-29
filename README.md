objectdb
========

Object db server is an HTTP based database server written in NodeJS.

ObjectDB is currently in alpha stage and includes fast written dirty code. Therefore it is **not recommended for production use**. 

Start by typing "nodejs obj.server"

Console Commands
----------------

**Create a database:**   

	dbserver.createdb("testdb")
 
**Create a table in db:**   

	dbserver.testdb.createtable2("testtable", {   
		struct:[   
			{ "name": "__id", "type": "numeric", "size":20 },   
			{ "name": "field1", "type": "string", "size":100 },   
			{ "name": "field2", "type": "string", "size":20 },   
		],   
		pk:field1,     //primary key   
		mode:db        //table mode = db|mem   
	})   
 
**Insert some data to table:**   

	dbserver.testdb.testtable.insert({ field1:"testdata", field2:"trial" })   
   
**Find inserted data by key:**   

	dbserver.testdb.testtable.find("testdata")   

**Try a TSQL Select**   

	select * from testdb.testtable where field1 like 'test%'   

**Update data by PK**   

	dbserver.testdb.testtable.update({ field1:"testdata", field2:"trial - updated" })   

**Reselect again to see if the row is updated**   

	dbserver.testdb.testtable.find("testdata")   

**Delete Row**   

	dbserver.testdb.testtable.delete({ __id:1 })   
  
  
  
Same functions for multi-row operations are also available with specific filters.
  
  
   
 

   
