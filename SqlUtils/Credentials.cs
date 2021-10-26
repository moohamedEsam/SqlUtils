using System;
namespace SqlUtils
{
    public class Credentials
    {

        public Credentials(string userName, string password)
        {
            this.UserName = userName;
            this.Password = password;
        }
        public string UserName { get; set; }

        public string Password { get; set; }
    }
}
