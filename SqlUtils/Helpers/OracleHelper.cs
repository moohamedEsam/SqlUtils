using System;
using System.Data.Common;
using System.Data.SqlClient;
using Oracle.ManagedDataAccess.Client;

namespace SqlUtils.Helpers
{
    public class OracleHelper : BaseHelper
    {
        private OracleConnection _connection;
        private OracleTransaction _transaction;

        public void AssignTransactionIfExists(DbCommand command)
        {
            if(_transaction != null)
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

        public  void Connect(string serverName, string databaseName,
            string username, string password)
        {
            var connectionString =
                $"User Id={username};Password={password};Data Source={databaseName}";
            _connection = new OracleConnection(connectionString);
            _connection.Open();
            Console.WriteLine("connection open");
        }

        public void Disconnect()
        {
            _connection?.Close();
        }

        public DbCommand GetCommand(string query)
        {
            return new OracleCommand(query, _connection);
        }

        public DbDataAdapter GetDbDataAdapter(string query)
        {
            return new OracleDataAdapter(query, _connection);
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