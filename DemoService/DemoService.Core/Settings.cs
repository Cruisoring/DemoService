using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json.Linq;

namespace DemoService.Core
{
    /// <summary>
    /// Helper class to keep settings like credentials to access SQL.
    /// 
    /// </summary>
    public static class Settings
    {
        public const string DefaultEnvironmentName = "dev";
        public const string EnvironmentNameKey = "environmentName";
        public const string DBSourceKey = "dataSource";
        public const string DBPasswordKey = "dbPassword";
        public const string DBUserNameKey = "dbUsername";
        public const string DBConnectionStringKey = "dbConnectionString";

        // Concerned Setting values
        public static readonly List<string> ConcernedSettings = new List<string>()
        {
            DBSourceKey, DBUserNameKey, DBPasswordKey, DBConnectionStringKey
        };

        public static readonly string EnvironmentName;
        public static readonly IDictionary<string, string> EnvironmentSettings;


        static Settings()
        {
            EnvironmentName = Environment.GetEnvironmentVariable(EnvironmentNameKey) ?? DefaultEnvironmentName;
            EnvironmentSettings = LoadEnvironmentSettings(EnvironmentName);
        }

        /**
         * This method try to get named settings from C:\users\{current_username}\environment_settings.json.
         */
        private static Dictionary<string, string> getEnvironmentSettingsFromUserHome(string envName)
        {
            //try to get settings from C:\users\{current_username}\environment_settings.json

            var credentialFilepath =
                $@"{Environment.GetEnvironmentVariable("SystemDrive")}\{Environment.GetEnvironmentVariable("HomePath")}\environment_settings.json";
            if (File.Exists(credentialFilepath))
            {
                var baseSettings = JObject.Parse(File.ReadAllText(credentialFilepath));
                var dict = baseSettings.ToObject<Dictionary<string, Dictionary<string, string>>>();

                if (dict.ContainsKey(envName.ToLower())) return dict[envName.ToLower()];
            }

            return new Dictionary<string, string>();
        }

        private static IDictionary<string, string> LoadEnvironmentSettings(string environmentName)
        {
            var settings = getEnvironmentSettingsFromUserHome(environmentName);

            foreach (var key in ConcernedSettings)
            {
                var settingValue = Environment.GetEnvironmentVariable(key);
                if (settingValue != null) settings[key] = settingValue;

                if (!settings.ContainsKey(key)) Console.WriteLine($"Warning: missing environment setting of {key}!");
            }

            return settings;
        }

        public static string Get(string key) => EnvironmentSettings.ContainsKey(key) ? EnvironmentSettings[key] : throw new ArgumentException($"No setting of '{key}'");
    }
}
