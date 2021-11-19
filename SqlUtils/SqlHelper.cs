using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Reflection;
using MySql.Data.MySqlClient;

namespace SqlUtils
{
    public class SqlHelper
    {
        private SqlConnection _sqlConnection;
        private MySqlConnection _mySqlConnection;
        private readonly DatabaseTypes _databaseType;


        private DbConnection GetConnection()
        {
            if (_databaseType == DatabaseTypes.Mysql)
                return _mySqlConnection;
            return _sqlConnection;
        }


        private DbCommand GetCommand(string query)
        {
            if (_databaseType == DatabaseTypes.Mysql)
                return new MySqlCommand(query, _mySqlConnection);
            return new SqlCommand(query, _sqlConnection);
        }

        /// <param name="databaseTypes">
        /// valid values are sqlServer and mysql
        /// </param>
        public SqlHelper(DatabaseTypes databaseTypes)
        {
            _databaseType = databaseTypes;
        }

        /// <summary>
        /// connect to database must be called first before any function
        /// </summary>
        public void Connect(string serverName, string dataBaseName, string userName, string password)
        {
            if (_databaseType == DatabaseTypes.Mysql)
                ConnectToMySql(serverName, dataBaseName, userName, password);
            else
                ConnectToSqlServer(serverName, dataBaseName, userName, password);
        }

        private void ConnectToSqlServer(string serverName, string dataBaseName,
            string userName, string password)
        {
            var connectionString =
                $"Data Source={serverName};Initial Catalog={dataBaseName};User ID={userName};Password={password};MultipleActiveResultSets=true";
            _sqlConnection = new SqlConnection(connectionString);
            _sqlConnection.Open();
            Console.WriteLine("connection open");
        }

        private void ConnectToMySql(string serverName, string databaseName,
            string userName, string password)
        {
            var connectionString =
                $"Server={serverName};Database={databaseName};Uid={userName};Pwd={password}";
            _mySqlConnection = new MySqlConnection(connectionString);
            _mySqlConnection.Open();
            Console.WriteLine("connection open");
        }

        /// <summary>
        /// disconnect the database use if before the program exit
        /// </summary>
        public void Disconnect()
        {
            GetConnection().Close();
            Console.WriteLine("connection closed");
        }


        private string PrepareDataForSql(SqlData data)
        {
            switch (data.Type)
            {
                case SqlTypes.StringEn:
                    return $"'{data.Data}'";
                case SqlTypes.StringAr:
                    return $"N'{data.Data}'";
                case SqlTypes.Date:

                    try
                    {
                        DateTime.ParseExact(data.Data, "yyyy/mm/dd", CultureInfo.InvariantCulture);
                        return _databaseType == DatabaseTypes.Sqlserver
                            ? $"cast({data.Data} as datetime)"
                            : $"str_to_date('{data.Data}', '%Y/%m/%d')";
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("correct format should be yyyy/mm/dd");
                        throw ex;
                    }

                case SqlTypes.Time:
                {
                    try
                    {
                        DateTime.ParseExact(data.Data, "HH:mm", CultureInfo.InvariantCulture);
                        return _databaseType == DatabaseTypes.Sqlserver
                            ? $"convert(time, '{data.Data}')"
                            : $"str_to_date('{data.Data}', '%H:%i')";
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("invalid date format");
                        Console.WriteLine("correct format should be HH:mm");
                        throw ex;
                    }
                }
                case SqlTypes.DateTime:
                    try
                    {
                        DateTime.ParseExact(data.Data, "yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture);
                        return _databaseType == DatabaseTypes.Sqlserver
                            ? $"cast({data.Data} as datetime)"
                            : $"str_to_date('{data.Data}', '%Y/%m/%d %H:%i')";
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("invalid date format");
                        Console.WriteLine("correct format should be yyyy/MM/dd HH:mm");
                        throw ex;
                    }


                case SqlTypes.Number:
                    return data.Data;

                default:
                    return data.Data;
            }
        }

        private static bool ContainsArabicLetters(string word) => word.Any(
            letter => letter <= 'ى' && letter >= 'ا'
        );

        private string PrepareDataForSql<T>(PropertyInfo property, T className)
        {
            var value = property.GetValue(className);
            switch (value)
            {
                case string stringValue:
                    return ContainsArabicLetters(stringValue) ? $"N'{stringValue}'" : $"'{stringValue}'";

                case DateTime date:
                    return _databaseType == DatabaseTypes.Sqlserver
                        ? $"cast({date.Year}/{date.Month}/{date.Day} as date)"
                        : $"str_to_date('{date.Year}/{date.Month}/{date.Day}', '%Y/%m/%d')";
                //number
                default:
                    return $"{value}";
            }
        }

        private static string GetEqualCondition(string idColumnName, object id)
        {
            var query = " where ";
            switch (id)
            {
                case string word:
                    query += $"{idColumnName} like '%{word}%'";
                    break;

                //number
                default:
                    query += $"{idColumnName} = {id}";
                    break;
            }

            return query;
        }

        private string GetEqualCondition(string idColumnName, SqlData id)
        {
            var query = " where ";
            if (id.Type == SqlTypes.StringEn || id.Type == SqlTypes.StringAr)
                query += $"{idColumnName} like {PrepareDataForSql(id)}";

            else
                query += $"{idColumnName} = {PrepareDataForSql(id)}";
            Console.WriteLine($"equal condition: {query}");
            return query;
        }

        private string GetUpdateQuery<T>(
            T className,
            string idColumnName,
            object id,
            string tableName)
        {
            var query = GetUpdateQueryWithoutCondition(tableName, className);
            query += GetEqualCondition(idColumnName, id);
            Console.WriteLine($"update query: {query}");
            return query;
        }

        private string GetUpdateQuery(
            string tableName,
            string idColumnName,
            SqlData id,
            Dictionary<string, SqlData> data)
        {
            var query = GetUpdateQueryWithoutCondition(tableName, data);
            query += GetEqualCondition(idColumnName, id);
            Console.WriteLine($"update query: {query}");
            return query;
        }

        private string GetUpdateQueryWithoutCondition<T>(string tableName, T className)
        {
            var query = $"update {tableName} set ";
            var properties = typeof(T).GetProperties();
            foreach (var propertyInfo in properties)
            {
                var value = PrepareDataForSql(propertyInfo, className);
                query += $"{propertyInfo.Name} = {value}, ";
            }

            query = query.Substring(0, query.Length - 2);
            return query;
        }

        private string GetUpdateQueryWithoutCondition(string tableName, Dictionary<string, SqlData> data)
        {
            var query = $"update {tableName} set ";
            foreach (var item in data)
                query += $"{item.Key} = {PrepareDataForSql(item.Value)}, ";


            query = query.Substring(0, query.Length - 2);
            return query;
        }

        private string GetInsertQuery<T>(string tableName, T className)
        {
            var query = $"insert into {tableName} ";
            var columns = "";
            var values = "";
            var properties = typeof(T).GetProperties();
            foreach (var item in properties)
            {
                columns += $"{item.Name}, ";
                values += $"{PrepareDataForSql(item, className)}, ";
            }

            columns = columns.Substring(0, columns.Length - 2);
            values = values.Substring(0, values.Length - 2);
            query += $"({columns}) values ({values})";
            Console.WriteLine($"insert query: {query}");
            return query;
        }

        private string GetInsertQuery(string tableName, Dictionary<string, SqlData> data)
        {
            var query = $"insert into {tableName} ";
            var columns = "";
            var values = "";
            foreach (var item in data)
            {
                columns += $"{item.Key}, ";
                values += $"{PrepareDataForSql(item.Value)}, ";
            }

            columns = columns.Substring(0, columns.Length - 2);
            values = values.Substring(0, values.Length - 2);
            query += $"({columns}) values ({values})";
            Console.WriteLine($"insert query: {query}");
            return query;
        }

        /// <summary>
        /// update table values using their id
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="idColumnName">the name of the primary key column</param>
        /// <param name="id">value of the id and its type in a class</param>
        /// <param name="data">the columns and their corresponding values</param>
        /// <returns>true if the update affected at least one row otherwise false</returns>
        public bool Update(
            string tableName,
            string idColumnName,
            SqlData id,
            Dictionary<string, SqlData> data)
        {
            var updateQuery = GetUpdateQuery(tableName, idColumnName, id, data);
            return GetCommand(updateQuery).ExecuteNonQuery() != 0;
        }

        public bool Update<T>(
            string tableName,
            string idColumnName,
            object id,
            T className
        )
        {
            var updateQuery = GetUpdateQuery(className, idColumnName, id, tableName);
            return GetCommand(updateQuery).ExecuteNonQuery() != 0;
        }

        public bool UpdateWithCondition(string tableName, Dictionary<string, SqlData> data, string condition)
        {
            var query = GetUpdateQueryWithoutCondition(tableName, data) + "where " + condition;
            return GetCommand(query).ExecuteNonQuery() != 0;
        }

        public bool UpdateWithCondition<T>(string tableName, T className, string condition)
        {
            var query = GetUpdateQueryWithoutCondition(tableName, className) + "where " + condition;
            return GetCommand(query).ExecuteNonQuery() != 0;
        }

        /// <summary>
        /// for specific queries
        /// </summary>
        /// <param name="query">the query which should be performed</param>
        /// <returns></returns>
        public bool ExecuteQuery(string query)
        {
            return GetCommand(query).ExecuteNonQuery() != 0;
        }

        /// <summary>
        /// select all data from the table
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="projections">columns which will be selected</param>
        /// <returns>null if theirs an exception or the database reader</returns>
        public DbDataAdapter GetDataAdapter(string tableName, string projections = "*")
        {
            var query = $"select {projections} from {tableName}";

            return GetDbDataAdapter(query);
        }
        
        public DbDataReader GetDataReader(string tableName, string projections = "*")
        {
            var query = $"select {projections} from {tableName}";

            return GetCommand(query).ExecuteReader();
        }

        private DbDataAdapter GetDbDataAdapter(string query)
        {
            if (_databaseType == DatabaseTypes.Mysql)
                return new MySqlDataAdapter(GetCommand(query) as MySqlCommand);
            return new SqlDataAdapter(GetCommand(query) as SqlCommand);
        }

        /// <summary>
        /// selecting with specification
        /// </summary>
        /// <param name="query">the whole select query</param>
        /// <returns></returns>
        public DbDataAdapter SelectByQuery_Adapter(string query)
        {
            return GetDbDataAdapter(query);
        }
        
        public DbDataReader SelectByQuery_Reader(string query)
        {
            return GetCommand(query).ExecuteReader();
        }

        public DbDataAdapter SelectWithCondition_Adapter(string tableName, string condition, string projections = "*")
        {
            var query = $"select {projections} from {tableName} where {condition}";
            return GetDbDataAdapter(query);
        }
        
        public DbDataReader SelectWithCondition_Reader(string tableName, string condition, string projections = "*")
        {
            var query = $"select {projections} from {tableName} where {condition}";
            return GetCommand(query).ExecuteReader();
        }

        /// <summary>
        /// insert a row in a table
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="data">columns and their values</param>
        /// <returns>true if the update affected at least one row otherwise false</returns>
        public bool Insert(string tableName, Dictionary<string, SqlData> data)
        {
            var query = GetInsertQuery(tableName, data);
            return GetCommand(query).ExecuteNonQuery() != 0;
        }

        public bool Insert<T>(string tableName, T className)
        {
            var query = GetInsertQuery(tableName, className);
            return GetCommand(query).ExecuteNonQuery() != 0;
        }

        /// <summary>
        /// delete a row using its id
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="idColumnName">the column name of the primary key</param>
        /// <param name="data"></param>
        /// <returns>true if the update affected at least one row otherwise false</returns>
        public bool Delete(string tableName, string idColumnName, SqlData data)
        {
            var query = $"delete from {tableName} {GetEqualCondition(idColumnName, data)}";
            Console.WriteLine($"delete: {query}");

            return GetCommand(query).ExecuteNonQuery() != 0;
        }

        public bool Delete(string tableName, string idColumnName, object id)
        {
            var query = $"delete from {tableName} {GetEqualCondition(idColumnName, id)}";
            Console.WriteLine($"delete: {query}");

            return GetCommand(query).ExecuteNonQuery() != 0;
        }

        public bool DeleteWithCondition(string tableName, string condition)
        {
            var query = $"delete from {tableName} where {condition}";
            Console.WriteLine($"delete query: {query}");
            return GetCommand(query).ExecuteNonQuery() != 0;
        }
        ~SqlHelper()
        {
            Disconnect();
        }
    }
}