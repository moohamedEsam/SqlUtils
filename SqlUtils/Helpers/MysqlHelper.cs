using System;
using System.Data.Common;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;

namespace SqlUtils.Helpers
{
    public class MysqlHelper : BaseHelper
    {
        public new void Connect(string serverName, string databaseName,
            string username, string password)
        {
            var connectionString =
                $"Server={serverName};Database={databaseName};Uid={username};Pwd={password}";
            Connection = new MySqlConnection(connectionString);
            Connection.Open();
            Console.WriteLine("connection open");
        }

        public new DbCommand GetCommand(string query)
        {
            var command = new MySqlCommand(query, Connection as MySqlConnection);
            AssignTransactionIfExists(command);
            return command;
        }

        public new DbDataAdapter GetDbDataAdapter(string query) => new MySqlDataAdapter(GetCommand(query) as MySqlCommand);
    }
}