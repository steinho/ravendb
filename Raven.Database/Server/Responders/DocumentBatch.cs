//-----------------------------------------------------------------------
// <copyright file="DocumentBatch.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class DocumentBatch : AbstractRequestResponder
	{
		private static long opCounter;

		public override string UrlPattern
		{
			get { return @"^/bulk_docs(/(.+))?"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST", "PATCH", "EVAL", "DELETE", "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var databaseBulkOperations = new DatabaseBulkOperations(Database, GetRequestTransaction(context));
			switch (context.Request.HttpMethod)
			{               
				case "GET":
					var id = context.Request.QueryString["id"];
					var backgroundExecuteStatus = Database.GetBackgroundExecuteStatus(id);
					if(backgroundExecuteStatus == null)
					{
						context.SetStatusToNotFound();
					}
					else
					{
						context.WriteJson(backgroundExecuteStatus);
					}
					break;
				case "POST":
					Batch(context);
					break;
				case "DELETE":
					OnBulkOperation(context, databaseBulkOperations.DeleteByIndex);
					break;
				case "PATCH":
					var patchRequestJson = context.ReadJsonArray();
					var patchRequests = patchRequestJson.Cast<RavenJObject>().Select(PatchRequest.FromJson).ToArray();
					OnBulkOperation(context, (index, query, allowStale, results) =>
						databaseBulkOperations.UpdateByIndex(index, query, patchRequests, allowStale, results));
					break;
				case "EVAL":
					var advPatchRequestJson = context.ReadJsonObject<RavenJObject>();
					var advPatch = ScriptedPatchRequest.FromJson(advPatchRequestJson);
					OnBulkOperation(context, (index, query, allowStale, results) =>
						databaseBulkOperations.UpdateByIndex(index, query, advPatch, allowStale, results));
					break;
			}
		}

		private void OnBulkOperation(IHttpContext context, Action<string, IndexQuery, bool, RavenJArray> batchOperation)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var index = match.Groups[2].Value;
			if (string.IsNullOrEmpty(index))
			{
				context.SetStatusToBadRequest();
				return;
			}
			var allowStale = context.GetAllowStale();
			var indexQuery = context.GetIndexQueryFromHttpContext(maxPageSize: int.MaxValue);


			var id = Interlocked.Increment(ref opCounter).ToString(CultureInfo.InvariantCulture);

			var results = new RavenJArray();
			Database.BackgroundExecute(id, results, () => batchOperation(index, indexQuery, allowStale, results ));

			context.WriteJson(new
			{
				OperationStarted = true,
				OperationId = id,
			});
		}
		
		private void Batch(IHttpContext context)
		{
			var jsonCommandArray = context.ReadJsonArray();

			var transactionInformation = GetRequestTransaction(context);
			var commands = (from RavenJObject jsonCommand in jsonCommandArray
			                select CommandDataFactory.CreateCommand(jsonCommand, transactionInformation))
				.ToArray();

			context.Log(log => log.Debug(()=>
			{
				if (commands.Length > 15) // this is probably an import method, we will input minimal information, to avoid filling up the log
				{
					return "\tExecuted " + string.Join(", ", commands.GroupBy(x => x.Method).Select(x => string.Format("{0:#,#;;0} {1} operations", x.Count(), x.Key)));
				}

				var sb = new StringBuilder();
				foreach (var commandData in commands)
				{
					sb.AppendFormat("\t{0} {1}{2}", commandData.Method, commandData.Key, Environment.NewLine);
				}
				return sb.ToString();
			}));

			var batchResult = Database.Batch(commands);
			context.WriteJson(batchResult);
		}
	}
}
