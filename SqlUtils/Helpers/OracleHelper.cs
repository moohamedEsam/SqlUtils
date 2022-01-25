using System;
using System.Data.Common;
using System.Data.SqlClient;
using Oracle.ManagedDataAccess.Client;

namespace SqlUtils.Helpers
{
    public class OracleHelper : BaseHelper
    {
        public new void Connect(string serverName, string databaseName,
            string username, string password)
        {
            var connectionString =
                $"User Id={username};Password={password};Data Source={databaseName}";
            Connection = new OracleConnection(connectionString);
            Connection.Open();
            Console.WriteLine("connection open");
        }

        public new DbCommand GetCommand(string query)
        {
            var command = new OracleCommand(query, Connection as OracleConnection);
            AssignTransactionIfExists(command);
            return command;
        }

        public new DbDataAdapter GetDbDataAdapter(string query) =>
            new OracleDataAdapter(GetCommand(query) as OracleCommand);
    }
}