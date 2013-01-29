objectdb
========

Object db server is an HTTP based database server written in NodeJS.

ObjectDB is currently in alpha stage and includes fast written dirty code. Therefore it is **not recommended for production use**. 

Start by typing "nodejs obj.server"

DB Level Functions
------------------

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

**Rename a table**

	dbserver.testdb.renametable("oldtable","newtable")
	
**Drop a table**

	dbserver.testdb.removetable("oldtable")

Table Level Functions
---------------------

**Insert object to table:**   

	dbserver.testdb.testtable.insert({ field1:"testdata", field2:"trial" })   
	
**Insert array of objects:**

	dbserver.testdb.testtable.insert([
		{ field1:"testdata 2", field2:"test2" },
		{ field1:"testdata 3", field2:"test3" }
	])
   
**Find inserted object by key:**   

	dbserver.testdb.testtable.find("testdata")   

**Running a TSQL Select statement**   

	select * from testdb.testtable where field1 like 'test%'   

**Update object by __id**

	dbserver.testdb.testtable.update({ __id:1, field2:"trial - updated 1" })
	
**Update object by PK (Primary Key)**   

	dbserver.testdb.testtable.updatebyPK({ field1:"testdata", field2:"trial - updated 2" })   

**Update multiple objects by using a filter expression**

	dbserver.testdb.testtable.updateFiltered({ field2:"xxx" }, "itm.field1=='testdata'")
	
**Reselect again to see if the object is updated**   

	dbserver.testdb.testtable.find("testdata")   

**Delete Object**   

	dbserver.testdb.testtable.deleteRow({ __id:1 })   
  
**Delete Multiple Objects by using a filter expression**

	dbserver.testdb.testtable.deleteFiltered("itm.field1=='testdata'")
	
**Get Object at position 2**

	dbserver.testdb.testtable.item(2)
	
**Get Objects between positions 2 and 5**

	dbsrever.testdb.testtable.rows(2,5)
	
**Get Objects by filtering**

	dbserver.testdb.testtable.filter(function(itm) {
		return itm.field1=="testdata"
	})

**Get Object Count**

	dbserver.testdb.testtable.count()
	
**Reindex table**

	dbserver.testdb.testtable.reindex()
	




   .
  
   
 

   
