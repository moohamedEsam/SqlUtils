using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Reflection;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;
using SqlUtils.Helpers;

namespace SqlUtils
{
    /// <summary>
    /// the class used in the function
    /// the variable must match the columns with in the given table
    /// </summary>
    public class SqlHelper
    {
        private BaseHelper _helper;
        private readonly DatabaseTypes _databaseType;

        /// <param name="databaseTypes">
        /// type of the used database
        /// valid values are sqlServer and mysql
        /// </param>
        public SqlHelper(DatabaseTypes databaseTypes)
        {
            _databaseType = databaseTypes;
            switch (databaseTypes)
            {
                case DatabaseTypes.Sqlserver:
                    _helper = new SqlServerHelper();
                    break;
                case DatabaseTypes.Mysql:
                    _helper = new MysqlHelper();
                    break;
                case DatabaseTypes.OleDb:
                    _helper = new OleDbHelper();
                    break;
                case DatabaseTypes.Oracle:
                    _helper = new OracleHelper();
                    break;
            }
        }

        public void Connect(string serverName, string databaseName, string username, string password)
        {
            _helper.Connect(serverName, databaseName, username, password);
        }


        /// <summary>
        /// disconnect the database use if before the program exit
        /// </summary>
        public void Disconnect()
        {
            _helper.Disconnect();
        }
        public void InitializeTransaction()
        {
            _helper.InitializeTransaction();
        }
        /// <summary>
        /// legacy function
        /// </summary>
        [Obsolete("legacy function")]
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
                        var unused = DateTime.ParseExact(data.Data, "yyyy/mm/dd", CultureInfo.InvariantCulture);
                        return _databaseType == DatabaseTypes.Sqlserver
                            ? $"cast({data.Data} as datetime)"
                            : $"str_to_date('{data.Data}', '%Y/%m/%d')";
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("correct format should be yyyy/mm/dd");
                        throw;
                    }

                case SqlTypes.Time:
                {
                    try
                    {
                        var unused = DateTime.ParseExact(data.Data, "HH:mm", CultureInfo.InvariantCulture);
                        return _databaseType == DatabaseTypes.Sqlserver
                            ? $"convert(time, '{data.Data}')"
                            : $"str_to_date('{data.Data}', '%H:%i')";
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("invalid date format");
                        Console.WriteLine("correct format should be HH:mm");
                        throw;
                    }
                }
                case SqlTypes.DateTime:
                    try
                    {
                        var unused = DateTime.ParseExact(data.Data, "yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture);
                        return _databaseType == DatabaseTypes.Sqlserver
                            ? $"cast({data.Data} as datetime)"
                            : $"str_to_date('{data.Data}', '%Y/%m/%d %H:%i')";
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("invalid date format");
                        Console.WriteLine("correct format should be yyyy/MM/dd HH:mm");
                        throw;
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

        /// <summary>
        /// function to return values in valid way to save in sql
        /// </summary>
        /// <param name="property">variable member of the class</param>
        /// <param name="className">the class which contains the variable</param>
        /// <typeparam name="T">the type of the class</typeparam>
        /// <returns>the string which will be saved in the database</returns>
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

        /// <summary>
        /// function which return the equal condition weather its string or number
        /// </summary>
        /// <param name="idColumnName">the primary key column name</param>
        /// <param name="id">string or number</param>
        /// <returns>the equal condition</returns>
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

        /// <summary>
        /// legacy function
        /// </summary>
        [Obsolete("legacy function")]
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
            return query;
        }

        [Obsolete("legacy function")]
        private string GetUpdateQuery(
            string tableName,
            string idColumnName,
            SqlData id,
            Dictionary<string, SqlData> data)
        {
            var query = GetUpdateQueryWithoutCondition(tableName, data);
            query += GetEqualCondition(idColumnName, id);
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

        [Obsolete("legacy function")]
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
            return query;
        }

        [Obsolete("legacy function")]
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
        [Obsolete("legacy function")]
        public bool Update(
            string tableName,
            string idColumnName,
            SqlData id,
            Dictionary<string, SqlData> data)
        {
            var updateQuery = GetUpdateQuery(tableName, idColumnName, id, data);
            return _helper.GetCommand(updateQuery).ExecuteNonQuery() != 0;
        }

        /// <summary>
        /// update table values using their id
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="idColumnName">the name of the primary key column</param>
        /// <param name="id">value of the id and its type in a class</param>
        /// <param name="className">the class which contains the id</param>
        /// <typeparam name="T">the class type</typeparam>
        /// <returns>true if the update affected at least one row otherwise false</returns>
        public bool Update<T>(
            string tableName,
            string idColumnName,
            object id,
            T className
        )
        {
            var updateQuery = GetUpdateQuery(className, idColumnName, id, tableName);
            Console.WriteLine($"SqlUtils: update -> {updateQuery}");
            return _helper.GetCommand(updateQuery).ExecuteNonQuery() != 0;
        }

        [Obsolete("legacy function")]
        public bool UpdateWithCondition(string tableName, Dictionary<string, SqlData> data, string condition)
        {
            var query = GetUpdateQueryWithoutCondition(tableName, data) + "where " + condition;
            return _helper.GetCommand(query).ExecuteNonQuery() != 0;
        }

        /// <summary>
        /// update the database with custom condition
        /// </summary>
        /// <param name="tableName">the table which will be updated</param>
        /// <param name="className">the class to update the values</param>
        /// <param name="condition">the condition which will be used in the query</param>
        /// <typeparam name="T">the class type</typeparam>
        /// <returns>the number of the affected rows</returns>
        /// <example>
        /// to update only some of the values use anonymous class
        /// a table has properties id, name, date
        /// to update only the name pass className param as {name=the new value}
        /// </example>
        public bool UpdateWithCondition<T>(string tableName, T className, string condition)
        {
            var query = GetUpdateQueryWithoutCondition(tableName, className) + "where " + condition;
            Console.WriteLine($"SqlUtils: update -> {query}");
            return _helper.GetCommand(query).ExecuteNonQuery() != 0;
        }


        /// <summary>
        /// for specific queries
        /// </summary>
        /// <param name="query">the query which should be performed</param>
        /// <returns></returns>
        public bool ExecuteQuery(string query)
        {
            return _helper.GetCommand(query).ExecuteNonQuery() != 0;
        }

        /// <summary>
        /// select all data from the table
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="projections">columns which will be selected</param>
        /// <param name="orderBy">order by column add desc if you want to revert the sorting</param>
        /// <returns>null if theirs an exception or the database reader</returns>
        public DbDataAdapter GetDataAdapter(string tableName, string projections = "*", string orderBy = "")
        {
            var query = $"select {projections} from {tableName} ";
            if (orderBy != "")
                query += $"order by {orderBy}";
            Console.WriteLine($"SqlUtils: getDataAdapter -> {query}");
            return GetDbDataAdapter(query);
        }

        ///  <summary>
        ///  function to get the data in a data reader
        ///  </summary>
        ///  <param name="tableName">the table which will be returned</param>
        ///  <param name="projections">
        /// the column which will be returned if not passed will return all the columns
        ///  if passed will return only the given columns
        ///  </param>
        ///  <param name="orderBy">order by column add desc if you want to revert the sorting</param>
        ///  <example>to return all the columns use GetDataReader(tableName)</example>
        ///  <example> to return only the id and name of the table use GetDataReader(tableName, "id, name")</example>
        ///  <returns>the data reader</returns>
        public DbDataReader GetDataReader(string tableName, string projections = "*", string orderBy = "")
        {
            var query = $"select {projections} from {tableName} ";
            if (orderBy != "")
                query += $"orderBy {orderBy}";
            Console.WriteLine($"SqlUtils: getData -> {query}");
            return _helper.GetCommand(query).ExecuteReader();
        }

        private DbDataAdapter GetDbDataAdapter(string query) => _helper.GetDbDataAdapter(query);

        /// <summary>
        /// function to get data with the given query
        /// </summary>
        /// <param name="query">the whole select query</param>
        /// <returns>data adapter</returns>
        public DbDataAdapter SelectByQuery_Adapter(string query)
        {
            return GetDbDataAdapter(query);
        }

        /// <summary>
        /// function to get data with the given query
        /// </summary>
        /// <param name="query">the whole select query</param>
        /// <returns>data reader</returns>
        public DbDataReader SelectByQuery_Reader(string query)
        {
            return _helper.GetCommand(query).ExecuteReader();
        }

        /// <summary>
        /// function to get data with custom condition
        /// </summary>
        /// <param name="tableName">the table of the database</param>
        /// <param name="condition">the condition to return only the valid rows</param>
        /// <param name="projections">the column to return if passed return only the given columns
        /// if not passed return all the columns</param>
        /// <param name="orderBy">order by column add desc if you want to revert the sorting</param>
        /// <returns>data adapter</returns>
        public DbDataAdapter SelectWithCondition_Adapter(string tableName, string condition, string projections = "*",
            string orderBy = "")
        {
            var query = $"select {projections} from {tableName} where {condition} ";
            if (orderBy != "")
                query += $"orderBy {orderBy}";
            Console.WriteLine($"SqlUtils: {query}");
            return GetDbDataAdapter(query);
        }

        /// <summary>
        /// function to get data with custom condition
        /// </summary>
        /// <param name="tableName">the table of the database</param>
        /// <param name="condition">the condition to return only the valid rows</param>
        /// <param name="projections">the column to return if passed return only the given columns
        /// if not passed return all the columns</param>
        /// <returns>data reader</returns>
        public DbDataReader SelectWithCondition_Reader(string tableName, string condition, string projections = "*")
        {
            var query = $"select {projections} from {tableName} where {condition}";
            return _helper.GetCommand(query).ExecuteReader();
        }

        /// <summary>
        /// insert a row in a table
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="data">columns and their values</param>
        /// <returns>true if the update affected at least one row otherwise false</returns>
        [Obsolete("legacy function")]
        public bool Insert(string tableName, Dictionary<string, SqlData> data)
        {
            var query = GetInsertQuery(tableName, data);
            var result = _helper.GetCommand(query).ExecuteNonQuery() != 0;
            Console.WriteLine($"SqlUtils: Insert -> {query}");
            return result;
        }

        /// <summary>
        /// insert a row in a table
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="className">the class which contains the data</param>
        /// <typeparam name="T">the class type</typeparam>
        /// <returns>true if the update affected at least one row otherwise false</returns>
        public bool Insert<T>(string tableName, T className)
        {
            var query = GetInsertQuery(tableName, className);
            var result = _helper.GetCommand(query).ExecuteNonQuery() != 0;
            Console.WriteLine($"SqlUtils: Insert -> {query}");
            return result;
        }


        /// <summary>
        /// delete a row using its id
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="idColumnName">the column name of the primary key</param>
        /// <param name="data"></param>
        /// <returns>true if the update affected at least one row otherwise false</returns>
        [Obsolete("legacy function")]
        public bool Delete(string tableName, string idColumnName, SqlData data)
        {
            var query = $"delete from {tableName} {GetEqualCondition(idColumnName, data)}";
            Console.WriteLine($"delete: {query}");
            return _helper.GetCommand(query).ExecuteNonQuery() != 0;
        }

        public bool Delete(string tableName, string idColumnName, object id)
        {
            var query = $"delete from {tableName} {GetEqualCondition(idColumnName, id)}";
            Console.WriteLine($"SqlUtils: Delete -> {query}");
            return _helper.GetCommand(query).ExecuteNonQuery() != 0;
        }

        public bool DeleteAll(string tableName)
        {
            var query = $"delete from {tableName}";
            Console.WriteLine($"SqlUtils: Delete -> {query}");
            return _helper.GetCommand(query).ExecuteNonQuery() != 0;
        }


        /// <summary>
        /// delete with custom condition
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="condition">the condition to delete the rows which make this condition true</param>
        /// <returns>true if the any row is affected else false</returns>
        public bool DeleteWithCondition(string tableName, string condition)
        {
            var query = $"delete from {tableName} where {condition}";
            Console.WriteLine($"SqlUtils: Delete -> {query}");
            return _helper.GetCommand(query).ExecuteNonQuery() != 0;
        }

        /// <summary>
        /// commit the changes done with transactions
        /// </summary>
        public void Commit()
        {
            _helper.Commit();
        }


        /// <summary>
        /// revert the changes done by the transactions
        /// </summary>
        public void RollBack()
        {
            _helper.RollBack();
        }


        ~SqlHelper()
        {
            Disconnect();
        }
    }
}