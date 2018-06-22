using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Linq;

namespace ignite_linq_issue_reproducer
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var ignite = Ignition.Start())
            {
                var cacheConf = new CacheConfiguration("test-cache", new QueryEntity(typeof(string), typeof(Record)))
                {
                    CacheMode = CacheMode.Partitioned
                };

                var cache = ignite.GetOrCreateCache<string, Record>(cacheConf);

                UploadData(cache);

                ExecuteLinqQuery(cache, true);

                ExecuteSqlQuery(cache);

                Console.WriteLine("Press any key to exit...");

                Console.ReadKey();
            } 
        }

        private static void ExecuteLinqQuery(ICache<string, Record> cache, bool asCacheQueryable)
        {
            Console.WriteLine("LINQ query results [asCacheQueryable={0}]:", asCacheQueryable);

            IQueryable<ICacheEntry<string, Record>> queryable;

            if (asCacheQueryable)
                // In this case query will throw an exception.
                queryable = cache.AsCacheQueryable();
            else
                queryable = cache.AsQueryable();

            try
            {
                var result = queryable.GroupBy(e => e.Value.ContractId).Select(group => new
                {
                    ContractId = group.Key,
                    Id = group.OrderByDescending(entry => entry.Value.Version).First().Key
                }).ToList();

                foreach (var item in result)
                    Console.WriteLine("ContractId={0}, Id={1}", item.ContractId, item.Id);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
        }

        private static void ExecuteSqlQuery(ICache<string, Record> cache)
        {
            Console.WriteLine("SQL query results:");

            var sql = new SqlFieldsQuery(
               "SELECT cid as cid, " +
               "       min(id) as id " +
               "FROM " +
               "  (SELECT t2.cid, " +
               "          t3.id " +
               "   FROM " +
               "     (SELECT t1.ContractId AS cid, " +
               "             max(t1.Version) AS ver " +
               "      FROM Record AS t1 " +
               "      GROUP BY t1.ContractId) AS t2 " +
               "   JOIN Record AS t3 ON t3.Version = t2.ver " +
               "   AND t3.ContractId = t2.cid) " +
               "GROUP BY cid");

            foreach (var fields in cache.Query(sql))
                Console.WriteLine("ContractId={0}, Id={1}", fields[0], fields[1]);
        }

        private static void UploadData(ICache<string, Record> cache)
        {
            cache.Put("1", new Record(1, "group1", 1));
            cache.Put("2", new Record(2, "group1", 2));

            cache.Put("3", new Record(3, "group2", 2));
            cache.Put("4", new Record(4, "group2", 3));

            cache.Put("5", new Record(5, "group3", 2));
            cache.Put("6", new Record(6, "group3", 3));
            cache.Put("7", new Record(7, "group3", 4));
        }
    }

    public class Record
    {
        public Record(int id, string contractId, int version)
        {
            ContractId = contractId;

            Version = version;

            Id = id;
        }

        [QuerySqlField()]
        public int Id { get; set; }

        [QuerySqlField(IsIndexed = true)]
        public string ContractId { get; set; }

        [QuerySqlField()]
        public int Version { get; set; }
    }
}
