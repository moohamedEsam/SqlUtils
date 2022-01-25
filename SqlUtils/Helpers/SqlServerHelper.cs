using System;
using System.Data.Common;
using System.Data.SqlClient;

namespace SqlUtils.Helpers
{
    public class SqlServerHelper : BaseHelper
    {
        private new void Connect(string serverName, string databaseName,
            string username, string password)
        {
            var connectionString =
                $"Data Source={serverName};Initial Catalog={databaseName};User ID={username};Password={password};MultipleActiveResultSets=true";
            Connection = new SqlConnection(connectionString);
            Connection.Open();
            Console.WriteLine("connection open");
        }

        public new DbCommand GetCommand(string query)
        {
            var command = new SqlCommand(query, Connection as SqlConnection);
            AssignTransactionIfExists(command);
            return command;
        }

        public new DbDataAdapter GetDbDataAdapter(string query) => new SqlDataAdapter(GetCommand(query) as SqlCommand);
    }
}