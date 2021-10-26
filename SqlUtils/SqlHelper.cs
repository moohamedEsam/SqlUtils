using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
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

        public SqlHelper(DatabaseTypes databaseTypes)
        {
            _databaseType = databaseTypes;
        }

        public void Connect(string serverName, string dataBaseName, Credentials credentials)
        {
            try
            {
                if (_databaseType == DatabaseTypes.Mysql)
                    ConnectToMySql(serverName, dataBaseName, credentials);
                else
                    ConnectToSqlServer(serverName, dataBaseName, credentials);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private void ConnectToSqlServer(string serverName, string dataBaseName, Credentials credentials)
        {
            try
            {
                string connectionString =
                    $"Data Source={serverName};Initial Catalog={dataBaseName};User ID={credentials.UserName};Password={credentials.Password}";
                _sqlConnection = new SqlConnection(connectionString);
                _sqlConnection.Open();
                Console.WriteLine("connection open");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
                Console.WriteLine($"error: {ex.Message}");
            }
        }

        private void ConnectToMySql(string serverName, string databaseName, Credentials credentials)
        {
            try
            {
                string connectionString =
                    $"Server={serverName};Database={databaseName};Uid={credentials.UserName};Pwd={credentials.Password}";
                _mySqlConnection = new MySqlConnection(connectionString);
                _mySqlConnection.Open();
                Console.WriteLine("connection open");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        public void Disconnect()
        {
            try
            {
                GetConnection().Close();
                Console.WriteLine("connection closed");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
                Console.WriteLine($"error: {ex.Message}");
            }
        }


        private string PrepareDataForSql(SqlData data)
        {
            switch (data.type)
            {
                case SqlTypes.STRING_EN:
                    return $"'{data.data}'";
                case SqlTypes.STRING_AR:
                    return $"N'{data.data}'";
                case SqlTypes.DATE:

                    try
                    {
                        DateTime.ParseExact(data.data, "yyyy/mm/dd", CultureInfo.InvariantCulture);
                        return _databaseType == DatabaseTypes.Sqlserver
                            ? $"cast({data.data} as datetime)"
                            : $"str_to_date('{data.data}', '%Y/%m/%d')";
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("correct format should be yyyy/mm/dd");
                        Console.WriteLine(ex.Message);
                        throw new FormatException("invalid date");
                    }

                case SqlTypes.TIME:
                {
                    try
                    {
                        DateTime.ParseExact(data.data, "HH:mm", CultureInfo.InvariantCulture);
                        return _databaseType == DatabaseTypes.Sqlserver
                            ? $"convert(time, '{data.data}')"
                            : $"str_to_date('{data.data}', '%H:%i')";
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("invalid date format");
                        Console.WriteLine("correct format should be HH:mm");
                        Console.WriteLine(ex.Message);
                        throw new FormatException("invalid time");
                    }
                }
                case SqlTypes.DATE_TIME:
                    try
                    {
                        DateTime.ParseExact(data.data, "yyyy/MM/dd HH:mm", CultureInfo.InvariantCulture);
                        return _databaseType == DatabaseTypes.Sqlserver
                            ? $"cast({data.data} as datetime)"
                            : $"str_to_date('{data.data}', '%Y/%m/%d %H:%i')";
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("invalid date format");
                        Console.WriteLine("correct format should be yyyy/MM/dd HH:mm");
                        Console.WriteLine(ex.Message);
                        throw new FormatException("invalid datetime");
                    }


                case SqlTypes.NUMBER:
                    return data.data;

                default:
                    return data.data;
            }
        }

        private string GetEqualCondition(string idColumnName, SqlData id)
        {
            var query = " where ";
            if (id.type == SqlTypes.STRING_EN || id.type == SqlTypes.STRING_AR)
                query += $"{idColumnName} like {PrepareDataForSql(id)}";

            else
                query += $"{idColumnName} = {PrepareDataForSql(id)}";
            Console.WriteLine($"equal condition: {query}");
            return query;
        }

        private string GetUpdateQuery(
            string tableName,
            string idColumnName,
            SqlData id,
            Dictionary<string, SqlData> data)
        {
            var query = $"update {tableName} set ";
            foreach (var item in data)
                query += $"{item.Key} = {PrepareDataForSql(item.Value)}, ";


            query = query.Substring(0, query.Length - 2);
            query += GetEqualCondition(idColumnName, id);
            Console.WriteLine($"update query: {query}");
            return query;
        }

        private string GetInsertQuery(string tableName, Dictionary<string, SqlData> data)
        {
            var query = $"insert into {tableName}( ";
            var columns = String.Join(", ", data.Keys);
            List<string> valueData = data.Values.Select(PrepareDataForSql).ToList();
            var values = String.Join(", ", valueData);
            query += $"{columns}) values ({values})";
            Console.WriteLine($"insert query: {query}");
            return query;
        }

        public bool Update(
            string tableName,
            string idColumnName,
            SqlData id,
            Dictionary<string, SqlData> data)
        {
            try
            {
                string updateQuery = GetUpdateQuery(tableName, idColumnName, id, data);
                return GetCommand(updateQuery).ExecuteNonQuery() != 0;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return false;
            }
        }

        public DbDataReader GetData(string tableName)
        {
            string query = $"select * from {tableName}";
            try
            {
                return GetCommand(query).ExecuteReader();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return null;
            }
        }

        public bool insert(string tableName, Dictionary<string, SqlData> data)
        {
            try
            {
                var query = GetInsertQuery(tableName, data);
                return GetCommand(query).ExecuteNonQuery() != 0;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return false;
            }
        }

        public bool Delete(string tableName, string idColumnName, SqlData data)
        {
            string query = $"delete from {tableName} {GetEqualCondition(idColumnName, data)}";
            Console.WriteLine($"delete: {query}");
            try
            {
                return GetCommand(query).ExecuteNonQuery() != 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }

        ~SqlHelper()
        {
            Disconnect();
        }
    }
}