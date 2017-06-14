using System.Linq;
using Elasticsearch.Net;
using log4net;
using Nest;
// ReSharper disable AccessToForEachVariableInClosure

namespace ElasticTools.Code.Extensions
{
	public static class ReIndex
	{
		private static readonly ILog Logger = LogManager.GetLogger(typeof(ReIndex).Name);

		public static void ReindexDocuments(this ElasticClient client, string sourceIndex, string destinationIndex)
		{
			Logger.Info("Reindexing documents to new index...");

			var scanResults =
				client.Search<object>(
					s =>
						s.Index(sourceIndex)
							.AllTypes()
							.From(0)
							.Size(500)
							.Query(q => q.MatchAll())
							.SearchType(SearchType.Scan)
							.Scroll("30s"));

			if (scanResults.Total <= 0)
			{
				Logger.Info("Existing index has no documents, nothing to reindex.");
			}
			else
			{
				var scrolls = 0;

				var results = client.Scroll<object>(s => s.Scroll("30s").ScrollId(scanResults.ScrollId));

				while (results.Documents.Any())
				{
					var bulkResponse = client.Bulk(b =>
					{
						// ReSharper disable once AccessToModifiedClosure
						foreach (var hit in results.Hits)
						{
							b.Index<object>(bi => bi.Document(hit.Source).Type(hit.Type).Index(destinationIndex).Id(hit.Id));
						}

						return b;
					});

					if (!bulkResponse.IsValid)
					{
						Logger.ErrorFormat("Exception : {0}, Exception type: {1}, Status : {2} ", bulkResponse.ServerError.Error,
							bulkResponse.ServerError.ExceptionType, bulkResponse.ServerError.Status);
					}

					Logger.Info("Reindexing progress: " + (scrolls + 1) * 500);

					results = client.Scroll<object>(s => s.Scroll("2m").ScrollId(scanResults.ScrollId));
					++scrolls;
				}
			}

			Logger.Info("Reindexing complete!");
		}
	}
}
