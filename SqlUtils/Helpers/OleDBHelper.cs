using System;
using System.Data.Common;
using System.Data.OleDb;
using System.Data.SqlClient;

namespace SqlUtils.Helpers
{
    public class OleDbHelper : BaseHelper
    {
        private OleDbConnection _connection;
        private OleDbTransaction _transaction;

        public void AssignTransactionIfExists(DbCommand command)
        {
            if(_transaction != null)
                command.Transaction = _transaction;  
        }

        public void ClearTransaction()
        {
            _transaction?.Dispose();
            _transaction = null;
        }

        public void Commit()
        {
            _transaction?.Commit();
            ClearTransaction();
        }

        public void Connect(string serverName, string databaseName,
            string username, string password)
        {
            var connectionString =
                $"Provider={serverName};Data Source={databaseName};Jet OLEDB:Database Password={password};";
            _connection = new OleDbConnection(connectionString);
            _connection.Open();
            Console.WriteLine("connection open");
        }

        public void Disconnect()
        {
            _connection?.Close();
        }

        public DbCommand GetCommand(string query)
        {
            return new OleDbCommand(query, _connection);
        }

        public DbDataAdapter GetDbDataAdapter(string query)
        {
            return new OleDbDataAdapter(query, _connection);
        }

        public void InitializeTransaction()
        {
            _transaction = _connection.BeginTransaction();
        }

        public void RollBack()
        {
            _transaction?.Rollback();
            ClearTransaction();
        }
    }
}