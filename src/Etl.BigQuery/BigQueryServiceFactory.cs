using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using Google.Apis.Services;
using log4net;
using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;

namespace Eol.Cig.Etl.BigQuery
{
    public class BigQueryServiceFactory : IBigQueryServiceFactory
    {
        private readonly ILog _logger;
        private readonly string _username;
        private readonly string _keyFilePath;
        private readonly string _keyFilePassword;

        public BigQueryServiceFactory(ILog logger, string username, string keyFilePath, string keyFilePassword)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _username = string.IsNullOrWhiteSpace(username) ? throw new ArgumentNullException(nameof(username)) : username;
            _keyFilePath = string.IsNullOrWhiteSpace(keyFilePath) ? throw new ArgumentNullException(nameof(keyFilePath)) : keyFilePath;
            _keyFilePassword = string.IsNullOrWhiteSpace(keyFilePassword) ? throw new ArgumentNullException(nameof(keyFilePassword)) : keyFilePassword;
            if (!File.Exists(keyFilePath))
            {
                throw new ArgumentException("File does not exist", _keyFilePath);
            }
        }

        public BigqueryService GetAuthorizedService()
        {
            _logger.Debug("Building BigQuery Authorized service.");

            var rawData = File.ReadAllBytes(_keyFilePath);
            var certificate = new X509Certificate2(rawData, _keyFilePassword, X509KeyStorageFlags.Exportable);

            var credential = new ServiceAccountCredential(
                new ServiceAccountCredential.Initializer(_username)
                {
                    Scopes = new[] { BigqueryService.Scope.Bigquery }
                }.FromCertificate(certificate));

            var service = new BigqueryService(new BaseClientService.Initializer
            {
                HttpClientInitializer = credential,
                ApplicationName = "CIG BigQuery Export"
            });

            _logger.Debug("BigQuery Authorized service built.");

            return service;
        }
    }
}
