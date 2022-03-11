using System;
using System.Data.Common;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;

namespace SqlUtils.Helpers
{
    public class MysqlHelper : BaseHelper
    {

        private MySqlConnection _connection;
        private MySqlTransaction _transaction;

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
                $"Server={serverName};Database={databaseName};Uid={username};Pwd={password}";
            _connection = new MySqlConnection(connectionString);
            _connection.Open();
            Console.WriteLine("connection open");
        }

        public void Disconnect()
        {
            _connection.Close();
        }

        public DbCommand GetCommand(string query)
        {
            var command = new MySqlCommand(query, _connection);
            AssignTransactionIfExists(command);
            return command;
        }

        public DbDataAdapter GetDbDataAdapter(string query) => new MySqlDataAdapter(GetCommand(query) as MySqlCommand);

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