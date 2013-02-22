﻿using System.Diagnostics;

namespace Raven.Tryouts
{
    #region

    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Raven.Client.Embedded;
    using Raven.Client.Indexes;

    #endregion

    internal class Program
    {
        #region Methods

        private static int forthCounter = 0;
        private static string GetRandomForthTag()
        {
            forthCounter++;
            if (forthCounter > 21)
            {
                forthCounter = 0;
            }

            return forthCounter.ToString() + "Number";
        }

        private static int secondCounter = 0;
        private static string GetRandomSecondTag()
        {
            secondCounter++;
            if (secondCounter > 20)
            {
                secondCounter = 0;
            }
            if (secondCounter > 3)
            {
                return "Second";
            }
            if (secondCounter > 9)
            {
                return "Third";
            }
            return secondCounter > 16 ? "Forth" : "First";
        }

        private static int thirdCounter = 0;
        private static string GetRandomThirdTag()
        {
            thirdCounter++;
            if (thirdCounter > 20)
            {
                thirdCounter = 0;
            }

            return thirdCounter > 10 ? "gandalf" : "bombor";
        }

        private static List<DummyObject.Tag> GetTags()
        {
            var tags = new List<DummyObject.Tag>
                           {
                               new DummyObject.Tag { Type = "AllTheSame", TagValue = "TheSame" },
                               new DummyObject.Tag
                                   {
                                       Type = "SecondTagWithFourValues",
                                       TagValue = GetRandomSecondTag()
                                   },
                               new DummyObject.Tag
                                   {
                                       Type = "ThirdWithTwoValues",
                                       TagValue = GetRandomThirdTag()
                                   },
                               new DummyObject.Tag
                                   {
                                       Type = "SpeciesGroupId",
                                       TagValue = GetRandomForthTag()
                                   }
                           };
            return tags;
        }

        private static void Main(string[] args)
        {
            using (var documentStore = new EmbeddableDocumentStore
            {
                //RunInMemory = true, 
                UseEmbeddedHttpServer = true
            })
            {
                documentStore.Initialize();
                IndexCreation.CreateIndexes(typeof(DummyIndex).Assembly, documentStore);
                //Console.ReadKey();
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var lastTimeSpan = new TimeSpan();
                var id = 1;
                for (var o = 0; o < 100; o++)
                {
                    using (var bulkInsertOperation = documentStore.BulkInsert())
                    {
                        for (var i = 0; i <20000; i++)
                        {
                            var dummyObject = new DummyObject { Id = id, Tags = GetTags() };
                            bulkInsertOperation.Store(dummyObject);
                            id++;
                        }
                    }

                    // wait for nonstale
                    
                    var doit = true;
                    while (doit)
                    {
                        using (var openSessionI = documentStore.OpenSession())
                        {
                            try
                            {
                                var nonstaleresult =
                                    openSessionI.Query<DummyIndex.Result, DummyIndex>()
                                                .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                                                .Take(1)
                                                .ToList();
                                if (nonstaleresult.Count > 0)
                                {
                                    doit = false;
                                }

                            }
                            catch (Exception)
                            {
                                System.Threading.Thread.Sleep(10000);
                            }
                        }
                    }

                    using (var openSession = documentStore.OpenSession())
                    {
                        var timeSpan = stopwatch.Elapsed;
                        var diff = timeSpan - lastTimeSpan;
                        lastTimeSpan = timeSpan;
                        openSession.Store(new PerfStats() { NonStaleCount = id - 1, Timer = timeSpan, Difference = diff });
                        openSession.SaveChanges();
                    }
                }
                
                stopwatch.Stop();

                int sumOfAllTheSameTag;
                using (var session = documentStore.OpenSession())
                {
                    Console.WriteLine("Wait for nonstale indexes: press any key");
                    Console.ReadKey();

                

                    var sumOfAllTags =
                        session.Query<DummyIndex.Result, DummyIndex>()
                               .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                               .Take(1000)
                               .ToList()
                               .Sum(x => x.Count);
                    Console.WriteLine(
                        "sum of all the tags should equal all document * 4 : " + (id - 1) + " documents, " + sumOfAllTags
                        + " tags....");

                    var sumOfSecondTag =
                        session.Query<DummyIndex.Result, DummyIndex>()
                               .Where(x => x.Type == "SecondTagWithFourValues")
                               .Take(1000)
                               .ToList()
                               .Sum(x => x.Count);
                    Console.WriteLine(
                        "sum of secondtag should equal all document: " + (id - 1) + " documents, " + sumOfSecondTag
                        + " SecondTagWithFourValues....");

                    var sumOfThirdTag =
                        session.Query<DummyIndex.Result, DummyIndex>()
                               .Where(x => x.Type == "ThirdWithTwoValues")
                               .Take(1000)
                               .ToList()
                               .Sum(x => x.Count);
                    Console.WriteLine(
                        "sum of thirdtag should equal all document: " + (id - 1) + " documents, " + sumOfThirdTag
                        + " ThirdWithTwoValues....");

                    var sumOfForthTag =
                        session.Query<DummyIndex.Result, DummyIndex>()
                               .Where(x => x.Type == "SpeciesGroupId")
                               .Take(1000)
                               .ToList()
                               .Sum(x => x.Count);
                    Console.WriteLine(
                        "sum of forthtag should equal all document: " + (id - 1) + " documents, " + sumOfForthTag
                        + " SpeciesGroupId....");

                    sumOfAllTheSameTag = session.Query<DummyIndex.Result, DummyIndex>().Where(x => x.Type == "AllTheSame").Take(1000).ToList().Sum(x => x.Count);
                }
                Console.WriteLine(
                    "sum of allthesametag should equal all document: " + (id - 1) + " documents, " + sumOfAllTheSameTag
                    + " allthesametags....");

                Console.ReadKey();
            }
        }

        #endregion
    }

    public class PerfStats
    {
        public int NonStaleCount { get; set; }
        public TimeSpan Timer { get; set; }
        public TimeSpan Difference { get; set; }
    }

    public class DummyIndex : AbstractIndexCreationTask<DummyObject, DummyIndex.Result>
    {
        #region Constructors and Destructors

        public DummyIndex()
        {
            this.Map =
                dummys =>
                from dummy in dummys
                from tag in dummy.Tags
                select new Result { Type = tag.Type, TagValue = tag.TagValue, Count = 1 };

            this.Reduce = results => from r in results
                                     group r by new { r.Type, r.TagValue }
                                         into g
                                         select
                                             new Result
                                             {
                                                 Type = g.Key.Type,
                                                 TagValue = g.Key.TagValue,
                                                 Count = g.Sum(x => x.Count)
                                             };
        }

        #endregion

        public class Result
        {
            #region Public Properties

            public int Count { get; set; }

            public string TagValue { get; set; }

            public string Type { get; set; }

            #endregion
        }
    }

    public class DummyObject
    {
        #region Public Properties

        public int Id { get; set; }

        public List<Tag> Tags { get; set; }

        #endregion

        public class Tag
        {
            #region Public Properties

            public string TagValue { get; set; }

            public string Type { get; set; }

            #endregion
        }
    }
}