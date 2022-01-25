using System;
using System.Data.Common;
using System.Data.OleDb;
using System.Data.SqlClient;

namespace SqlUtils.Helpers
{
    public class OleDbHelper : BaseHelper
    {
        public new void Connect(string serverName, string databaseName,
            string username, string password)
        {
            var connectionString =
                $"Provider={serverName};Data Source={databaseName};Jet OLEDB:Database Password={password};";
            Connection = new OleDbConnection(connectionString);
            Connection.Open();
            Console.WriteLine("connection open");
        }

        public new DbCommand GetCommand(string query)
        {
            var command = new OleDbCommand(query, Connection as OleDbConnection);
            AssignTransactionIfExists(command);
            return command;
        }

        public new DbDataAdapter GetDbDataAdapter(string query) =>
            new OleDbDataAdapter(GetCommand(query) as OleDbCommand);
    }
}