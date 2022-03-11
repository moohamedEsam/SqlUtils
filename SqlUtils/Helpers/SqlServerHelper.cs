using System;
using System.Data.Common;
using System.Data.SqlClient;

namespace SqlUtils.Helpers
{
    public class SqlServerHelper : BaseHelper
    {
        private SqlConnection _connection;
        private SqlTransaction _transaction;

        public void AssignTransactionIfExists(DbCommand command)
        {
            if (_transaction != null)
                command.Transaction = _transaction;
        }

        public void ClearTransaction()
        {
            _transaction.Dispose();
            _transaction = null;
        }

        public void Commit()
        {
            _transaction?.Commit();
            ClearTransaction();
        }

        public void Disconnect()
        {
            _connection.Close();
        }

        public DbCommand GetCommand(string query)
        {
            return new SqlCommand(query, _connection);
        }

        public DbDataAdapter GetDbDataAdapter(string query)
        {
            return new SqlDataAdapter(query, _connection);
        }

        public void InitializeTransaction()
        {
            _transaction = _connection.BeginTransaction();
        }

        public void RollBack()
        {
            _transaction.Rollback();
            ClearTransaction();
        }

        public void Connect(string serverName, string databaseName,
            string username, string password)
        {
            var connectionString =
                $"Data Source={serverName};Initial Catalog={databaseName};User ID={username};Password={password};MultipleActiveResultSets=true";
            _connection = new SqlConnection(connectionString);
            _connection.Open();
            Console.WriteLine("connection open");
        }
    }
}