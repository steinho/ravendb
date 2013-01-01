//-----------------------------------------------------------------------
// <copyright file="DatabaseBulkOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Database.Data;
using Raven.Database.Json;
using Raven.Json.Linq;

namespace Raven.Database.Impl
{
	public class DatabaseBulkOperations
	{
		private readonly DocumentDatabase database;
		private readonly TransactionInformation transactionInformation;

		public DatabaseBulkOperations(DocumentDatabase database, TransactionInformation transactionInformation)
		{
			this.database = database;
			this.transactionInformation = transactionInformation;
		}

		public void DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale, RavenJArray results)
		{
			PerformBulkOperation(indexName, queryToDelete, allowStale, (docId, tx) =>
			{
				database.Delete(docId, null, tx);
				return new { Document = docId, Deleted = true };
			}, results);
		}

		public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale, RavenJArray results)
		{
			PerformBulkOperation(indexName, queryToUpdate, allowStale, (docId, tx) =>
			{
				var patchResult = database.ApplyPatch(docId, null, patchRequests, tx);
				return new { Document = docId, Result = patchResult };
			}, results);
		}

		public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale, RavenJArray results)
		{
			PerformBulkOperation(indexName, queryToUpdate, allowStale, (docId, tx) =>
			{
				var patchResult = database.ApplyPatch(docId, null, patch, tx);
				return new { Document = docId, Result = patchResult.Item1, Debug = patchResult.Item2 };
			}, results);
		}

		private void PerformBulkOperation(string index, IndexQuery indexQuery, bool allowStale, Func<string, TransactionInformation, object> batchOperation, RavenJArray array)
		{
			var bulkIndexQuery = new IndexQuery
			{
				Query = indexQuery.Query,
				Start = indexQuery.Start,
				Cutoff = indexQuery.Cutoff,
				PageSize = int.MaxValue,
				FieldsToFetch = new[] { Constants.DocumentIdFieldName },
				SortedFields = indexQuery.SortedFields
			};

			bool stale;
			var queryResults = database.QueryDocumentIds(index, bulkIndexQuery, out stale);

			if (stale && allowStale == false)
			{
				throw new InvalidOperationException(
						"Bulk operation cancelled because the index is stale and allowStale is false");
			}

			const int batchSize = 1024;
			using (var enumerator = queryResults.GetEnumerator())
			{
				while (true)
				{
					var batchCount = 0;
					database.TransactionalStorage.Batch(actions =>
					{
						while (batchCount < batchSize && enumerator.MoveNext())
						{
							batchCount++;
							var result = batchOperation(enumerator.Current, transactionInformation);
							array.Add(RavenJObject.FromObject(result));
						}
					});
					if (batchCount < batchSize) break;
				}
			}
		}
	}
}
