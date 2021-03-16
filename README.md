# sql-plus-plus
a unitive interface for operating database. Direct or Prepare-statement.
</br>All operation can be done with just one interface--query.

## Mysql
1、Clusetr model. Mysql MGR clusetr, automaticly adapt clusetr member changed, Master/slave connection update.
</br>2、Single model.

## Sqlserver/Oracle
single model.

# Usage
For sqlserver/odbc single mode:
```c++
#include "db.hpp"
#include "sqlserver_connection_pool.hpp"
using namespace sqlcpp;

//for win
std::string driver_name = "Driver={ODBC Driver 17 for SQL Server}"; //"DRIVER={SQL Server}"
//for linux
std::string driver_name = "Driver=ODBC Driver 17 for SQL Server"; //"DRIVER={SQL Server}"
auto db_ptr = std::make_shared<db<model::single, sqlserver::connection_pool>>
		(std::vector<node_info>{ {"10.10.10.8"}}, "user", "pwd", driver_name);

auto conn = db_ptr->get_conn<conn_type::general>();
//create
conn->query<void>(R"(
CREATE TABLE [dbo].[user] (
  [name] varchar(255) COLLATE Chinese_PRC_CI_AS  NOT NULL,
  [sex] int COLLATE Chinese_PRC_CI_AS  NOT NULL
)  
ON [PRIMARY]
GO

ALTER TABLE [dbo].[user] SET (LOCK_ESCALATION = TABLE)
)");
conn->query<void>("insert into [dbo].[user] values(?,?)", "xixi", 1);

//read
auto names = conn->query<std::string>("select name from [dbo].[user]");

struct info {
	std::string name;
	int sex;
	REFLECT(info, name, sex);
};
auto cs1 = conn->query<std::tuple<std::string, int>>("select * from [dbo].[user]");
auto cs2 = conn->query<info>("select * from [dbo].[user]");

auto ns = conn->query<int>("select sex from [dbo].[user] where name = ?", "xixi");
auto sn = conn->query<int>("select sex from [dbo].[user] where name = ? and sex = ?", "xixi", 1);

//update
conn->query<void>("update [dbo].[user] set sex = ? where name = ?", 2, "xixi");

//delete
conn->query<void>("delete from [dbo].[user] where name = ?", "xixi");
```
</br>For mysql single mode just:

```c++
#include "db.hpp"
#include "mysql_connection_pool.hpp"
using namespace sqlcpp;

auto db_ptr = std::make_shared<db<model::single, mysql::connection_pool>>
		(std::vector<node_info>{ {"10.10.10.8"}}, "user", "pwd");
auto conn = db_ptr->get_conn<conn_type::general>();
//then crud is same like above.
```

</br>For mysql cluster mode:

```c++
#include "db.hpp"
#include "mysql_connection_pool.hpp"
using namespace sqlcpp;

auto db_ptr = std::make_shared<db<model::cluster, mysql::connection_pool>>
        (std::vector<node_info>{ {"10.10.10.8"}}, "user", "pwd");
//this is a master node connection, use for writing and reading
auto conn = db_ptr->get_conn<sqlcpp::conn_type::master>();
//this is a slave node connection, just can use for reading
auto conn = db_ptr->get_conn<sqlcpp::conn_type::slave>(); 
//then crud is same like above.
```
Under the example dir, has very detailed examples.

# Maybe do
1、postgresql
</br>2、sqlite

