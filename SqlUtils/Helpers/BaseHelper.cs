using System;
using System.Data.Common;

namespace SqlUtils.Helpers
{
    public class BaseHelper
    {
        public void Connect(string serverName, string databaseName, string username, string password){}
        public DbConnection Connection;
        public DbTransaction Transaction;
        public void AssignTransactionIfExists(DbCommand command)
        {
            if (Transaction != null)
                command.Transaction = Transaction;
        }

        public DbCommand GetCommand(string query) => null;

        public DbDataAdapter GetDbDataAdapter(string query) => null;
        
        public void ClearTransaction()
        {
            Transaction.Dispose();
            Transaction = null;
        }

        public void InitializeTransaction()
        {
            Transaction = Connection.BeginTransaction();
        }
        
        public void Commit()
        {
            Transaction.Commit();
            ClearTransaction();
        }


        /// <summary>
        /// revert the changes done by the transactions
        /// </summary>
        public void RollBack()
        {
            Transaction.Rollback();
            ClearTransaction();
        }

        
        public void Disconnect()
        {
            Connection.Close();
            Console.WriteLine("connection closed");
        }

        ~BaseHelper()
        {
            Disconnect();
        }
    }
}