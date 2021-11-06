namespace SqlUtils
{
    /// <summary>
    /// a wrapper class used to connect to the database 
    /// </summary>
    public class Credentials
    {
        /// <summary>
        /// constructor for creating the class
        /// </summary>
        /// <param name="userName">the username for database</param>
        /// <param name="password"></param>
        public Credentials(string userName, string password)
        {
            UserName = userName;
            Password = password;
        }
        public string UserName { get; set; }

        public string Password { get; set; }
    }
}
