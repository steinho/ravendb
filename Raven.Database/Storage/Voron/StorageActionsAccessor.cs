﻿using System.Diagnostics;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Util.Streams;
using Raven.Storage.Voron;

namespace Raven.Database.Storage.Voron
{
	using System;
	using System.Collections.Generic;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.MEF;
	using Raven.Database.Impl;
	using Raven.Database.Plugins;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Database.Storage.Voron.StorageActions;
	using Raven.Database.Tasks;

	using global::Voron.Exceptions;
	using global::Voron.Impl;

	public class StorageActionsAccessor : IStorageActionsAccessor
	{
        private readonly DateTime createdAt = SystemTime.UtcNow;
		public event Action OnDispose;

        public StorageActionsAccessor(IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs, IDocumentCacher documentCacher, Reference<WriteBatch> writeBatchReference, SnapshotReader snapshot, TableStorage storage, TransactionalStorage transactionalStorage, IBufferPool bufferPool)
        {
			Documents = new DocumentsStorageActions(generator, documentCodecs, documentCacher, writeBatchReference, snapshot, storage, bufferPool);
            Indexing = new IndexingStorageActions(storage, generator, snapshot, writeBatchReference, this, bufferPool);
            Queue = new QueueStorageActions(storage, generator, snapshot, writeBatchReference, bufferPool);
            Lists = new ListsStorageActions(storage, generator, snapshot, writeBatchReference, bufferPool);
            Tasks = new TasksStorageActions(storage, generator, snapshot, writeBatchReference, bufferPool);
            Staleness = new StalenessStorageActions(storage, snapshot, writeBatchReference, bufferPool);
            MapReduce = new MappedResultsStorageActions(storage, generator, documentCodecs, snapshot, writeBatchReference, bufferPool);
            Attachments = new AttachmentsStorageActions(storage.Attachments, writeBatchReference, snapshot, generator, storage, transactionalStorage, bufferPool);
            General = new GeneralStorageActions(storage, storage.General, writeBatchReference, snapshot, bufferPool);
		}


		public void Dispose()
		{
			var onDispose = OnDispose;
			if (onDispose != null)
				onDispose();
		}

		public IDocumentStorageActions Documents { get; private set; }

		public IQueueStorageActions Queue { get; private set; }

		public IListsStorageActions Lists { get; private set; }

		public ITasksStorageActions Tasks { get; private set; }

		public IStalenessStorageActions Staleness { get; private set; }

		public IAttachmentsStorageActions Attachments { get; private set; }

		public IIndexingStorageActions Indexing { get; private set; }

		public IGeneralStorageActions General { get; private set; }

		public IMappedResultsStorageAction MapReduce { get; private set; }

		public bool IsNested { get; set; }
		public event Action OnStorageCommit;

		public bool IsWriteConflict(Exception exception)
		{
			return exception is ConcurrencyException;
		}

		private readonly List<DatabaseTask> tasks = new List<DatabaseTask>();

		public T GetTask<T>(Func<T, bool> predicate, T newTask) where T : DatabaseTask
        {
            var task = tasks.OfType<T>().FirstOrDefault(predicate);
            
            if (task != null) return task;

            tasks.Add(newTask);
            return newTask;
        }

		private Action<JsonDocument[]> afterCommitAction;
		private List<JsonDocument> docsForCommit;

	    internal void ExecuteOnStorageCommit()
	    {
	        if (OnStorageCommit != null)
	        {
	            OnStorageCommit();
	        }
	    }

		public void AfterStorageCommitBeforeWorkNotifications(JsonDocument doc, Action<JsonDocument[]> afterCommit)
		{
			afterCommitAction = afterCommit;
			if (docsForCommit == null)
			{
				docsForCommit = new List<JsonDocument>();
				OnStorageCommit += () => afterCommitAction(docsForCommit.ToArray());
			}
			docsForCommit.Add(doc);
		}

        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		public void SaveAllTasks()
		{
            foreach (var task in tasks)
            {
                Tasks.AddTask(task, createdAt);
            }
		}
	}
}