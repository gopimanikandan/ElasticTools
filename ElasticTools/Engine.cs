using System;
using System.Diagnostics;
using ElasticTools.Code.Extensions;
using ElasticTools.Code.Settings;
using log4net;
using Nest;

namespace ElasticTools
{
	class Engine
	{
		private static readonly ILog Logger = LogManager.GetLogger(typeof(Engine).Name);

		public static void Run()
		{
			var url = new UriBuilder
			{
				Host = AppSettings.Server,
				Port = 9200,
				Scheme = "http"
			}.ToString();

			var connectionSettings = new ConnectionSettings(new Uri(url));
			connectionSettings.SetDefaultIndex(AppSettings.SourceIndex);
			var elasticClient = new ElasticClient(connectionSettings);

			var stopWatch = new Stopwatch();
			stopWatch.Start();

			switch (AppSettings.Option)
			{
				case Usage.ReIndex:
					{
						elasticClient.ReindexDocuments(AppSettings.SourceIndex, AppSettings.DestinationIndex);
						break;
					}
			}

			stopWatch.Stop();

			Logger.InfoFormat("ETL process completed in {0} minutes", Math.Round(stopWatch.Elapsed.TotalMinutes, 2));
		}
	}
}
